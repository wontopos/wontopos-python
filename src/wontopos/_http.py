from __future__ import annotations

import contextvars
import copy as _copy
import ipaddress
import json
import logging
import math
import os
import random
import re
import socket
import ssl
import threading
import warnings
import weakref
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Optional

import requests
import urllib3
import urllib3.connection as _u3_connection
import urllib3.connectionpool as _u3_pool
from urllib3.util.response import is_fp_closed as _u3_is_fp_closed

from . import (
    APIConnectionError, NotFoundError, RateLimitError, WosError, _JS_SPACE, _js_trim, _logger, _make_error, _time,
)
from ._checks import _ENV_KEYS, _HEADER_CTL_RE, _NO_BODY_STATUS, _check_model, _idem_headers, _json_default, _warn_lock


def _env_key() -> str:
    for name in _ENV_KEYS:
        val = os.environ.get(name)
        if val and _js_trim(val):
            return val
    raise ValueError(f"set {_ENV_KEYS[0]} (or {_ENV_KEYS[1]}) in the environment")


def _new_tls_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    try:
        import certifi

        ctx.load_verify_locations(cafile=certifi.where())
    except (ImportError, OSError, ssl.SSLError):
        pass
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


_TLS_CONTEXTS: "dict" = {}


def _tls_context() -> ssl.SSLContext:
    ctx = _TLS_CONTEXTS.get("sync")
    if ctx is None:
        ctx = _TLS_CONTEXTS["sync"] = _new_tls_context()
        ctx.check_hostname = not urllib3.__version__.startswith("1.")
    return ctx


def _async_tls_context() -> ssl.SSLContext:
    return _TLS_CONTEXTS.get("async") or _TLS_CONTEXTS.setdefault("async", _new_tls_context())


_attempt_local = threading.local()


def _conn_socket(conn: Any) -> Any:
    return getattr(conn, "sock", None) or getattr(conn, "_wos_sock", None)


def _shut_socket(conn: Any) -> None:
    sock = _conn_socket(conn)
    if sock is None:
        return
    try:
        socket.socket.shutdown(sock, socket.SHUT_RDWR)
    except (OSError, TypeError, ValueError):
        pass


_watch_lock = threading.Lock()


def _set_owner(conn: Any, owner: Optional["_Watchdog"]) -> None:
    try:
        conn._wos_watchdog = owner
    except AttributeError:
        pass


class _Watchdog:

    def __init__(self, end: float):
        self._conn: Any = None
        self._done = False
        self.fired = False
        self._timer = _time.timer(max(0.0, end - _time.monotonic()), self._fire)

    def _owned(self) -> Any:
        conn = self._conn
        if conn is not None and getattr(conn, "_wos_watchdog", None) is self:
            return conn
        return None

    def attach(self, conn: Any) -> None:
        with _watch_lock:
            self._conn = conn
            _set_owner(conn, self)
            if self.fired:
                _shut_socket(conn)

    def shrink(self, left: float) -> None:
        with _watch_lock:
            sock = _conn_socket(self._owned())
            if sock is not None:
                try:
                    sock.settimeout(max(0.001, left))
                except (OSError, ValueError):
                    pass

    def sooner(self, end: float) -> None:
        with _watch_lock:
            if self._done or self.fired:
                return
            self._timer.cancel()
            self._timer = _time.timer(max(0.0, end - _time.monotonic()), self._fire)

    def _fire(self) -> None:
        with _watch_lock:
            if self._done:
                return
            self.fired = True
            conn = self._owned()
            if conn is not None:
                _shut_socket(conn)

    def stop(self) -> bool:
        with _watch_lock:
            self._done = True
            conn = self._owned()
            if conn is not None:
                _set_owner(conn, None)
        self._timer.cancel()
        return self.fired


def _attach_to_watchdog(conn: Any) -> None:
    watchdog = getattr(_attempt_local, "watchdog", None)
    if watchdog is not None:
        watchdog.attach(conn)


class _WatchedConnection:

    def request(self, *args: Any, **kwargs: Any) -> Any:
        _attach_to_watchdog(self)
        sock = getattr(self, "sock", None)
        if sock is not None:
            try:
                sock.settimeout(self.timeout)  # type: ignore[attr-defined]
            except (OSError, TypeError, ValueError):
                pass
        return super().request(*args, **kwargs)  # type: ignore[misc]

    def getresponse(self, *args: Any, **kwargs: Any) -> Any:
        _attach_to_watchdog(self)
        self._wos_sock = getattr(self, "sock", None)
        return super().getresponse(*args, **kwargs)  # type: ignore[misc]


class _WatchedHTTPConnection(_WatchedConnection, _u3_connection.HTTPConnection):
    pass


class _WatchedHTTPSConnection(_WatchedConnection, _u3_connection.HTTPSConnection):
    pass


class _ReleasingPool:

    def _put_conn(self, conn: Any) -> None:
        if conn is not None:
            with _watch_lock:
                _set_owner(conn, None)
        super()._put_conn(conn)  # type: ignore[misc]


class _WatchedHTTPPool(_ReleasingPool, _u3_pool.HTTPConnectionPool):
    ConnectionCls = _WatchedHTTPConnection


class _WatchedHTTPSPool(_ReleasingPool, _u3_pool.HTTPSConnectionPool):
    ConnectionCls = _WatchedHTTPSConnection


_WATCHED_POOLS = {"http": _WatchedHTTPPool, "https": _WatchedHTTPSPool}
_WATCHED_SOCKS_POOLS: dict = {}


def _watched_socks_pools() -> dict:
    with _watch_lock:
        if not _WATCHED_SOCKS_POOLS:
            from urllib3.contrib import socks as u3_socks

            class _WatchedSOCKSConnection(_WatchedConnection, u3_socks.SOCKSConnection):
                pass

            class _WatchedSOCKSHTTPSConnection(_WatchedConnection, u3_socks.SOCKSHTTPSConnection):
                pass

            class _WatchedSOCKSHTTPPool(_ReleasingPool, u3_socks.SOCKSHTTPConnectionPool):
                ConnectionCls = _WatchedSOCKSConnection

            class _WatchedSOCKSHTTPSPool(_ReleasingPool, u3_socks.SOCKSHTTPSConnectionPool):
                ConnectionCls = _WatchedSOCKSHTTPSConnection

            _WATCHED_SOCKS_POOLS.update({"http": _WatchedSOCKSHTTPPool, "https": _WatchedSOCKSHTTPSPool})
        return _WATCHED_SOCKS_POOLS


class _TLSAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        kwargs["ssl_context"] = _tls_context()
        super().init_poolmanager(*args, **kwargs)
        self.poolmanager.pool_classes_by_scheme = _WATCHED_POOLS

    def proxy_manager_for(self, proxy: str, **proxy_kwargs: Any) -> Any:
        proxy_kwargs.setdefault("ssl_context", _tls_context())
        manager = super().proxy_manager_for(proxy, **proxy_kwargs)
        socks = proxy.lower().startswith("socks")
        manager.pool_classes_by_scheme = _watched_socks_pools() if socks else _WATCHED_POOLS
        return manager


_sessions: "weakref.WeakSet[requests.Session]" = weakref.WeakSet()


def _drop_pools_after_fork() -> None:
    for sess in list(_sessions):
        for adapter in sess.adapters.values():
            managers = [getattr(adapter, "poolmanager", None)]
            managers += list(getattr(adapter, "proxy_manager", {}).values())
            for pm in managers:
                try:
                    pm.clear()
                except Exception:
                    pass


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_drop_pools_after_fork)


def _check_base_url(base_url: Any) -> str:
    if isinstance(base_url, str):
        base_url = _js_trim(base_url)
    if not isinstance(base_url, str) or not base_url:
        raise ValueError("base_url must be a non-empty string")
    if "\\" in base_url or any(c in _JS_SPACE for c in base_url) or _HEADER_CTL_RE.search(base_url):
        raise ValueError(
            f"base_url {_shown_base(base_url)!r} contains whitespace, a backslash or a "
            "control character. Check for a stray space, newline or paste error."
        )
    base = base_url.rstrip("/")
    _require_url(base, _sync_scheme_host)
    if "?" in base or "#" in base:
        raise ValueError("base_url has a ? query or a # anchor: give the service root without them, e.g. https://api.wontopos.com")
    return base


_AUTHORITY_END = re.compile(r"[/\\?#]")
_QUERY_ON = re.compile(r"([?#]).*", re.S)


def _mask_userinfo(url: str) -> str:
    sep = url.find("://")
    start = sep + 3 if sep != -1 else 0
    scheme, host = _sync_scheme_host(url)
    end = _AUTHORITY_END.search(url, start) if scheme in ("http", "https") and host else None
    at = url.rfind("@", start, len(url) if end is None else end.start())
    return url if at == -1 else url[:start] + "***@" + url[at + 1:]


def _shown_base(url: str) -> str:
    return _QUERY_ON.sub(r"\1", _mask_userinfo(url), 1)


def _url_login(r: Any) -> Any:
    login = requests.utils.get_auth_from_url(r.url)
    return requests.auth.HTTPBasicAuth(*login)(r) if any(login) else r


def _require_url(base: str, read_url: Any) -> None:
    scheme, host = read_url(base)
    if scheme not in ("http", "https") or not host:
        raise ValueError("base_url is not a URL: it needs an https:// (or http://) scheme and a host, e.g. https://api.wontopos.com")


def _sync_scheme_host(base: str) -> tuple:
    from urllib3.util import parse_url

    try:
        p = parse_url(base)
    except Exception:
        return "", ""
    return (p.scheme or "").lower(), (p.host or "").lower().strip("[]")


def _async_scheme_host(base: str) -> tuple:
    import httpx

    try:
        u = httpx.URL(base)
    except Exception:
        return "", ""
    return u.scheme.lower(), u.host.lower()


def _is_local_host(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        ip: Any = ipaddress.ip_address(host)
    except ValueError:
        try:
            ip = ipaddress.IPv4Address(socket.inet_aton(host))
        except (OSError, ValueError):
            return False
    return ip.is_unspecified or (ip.is_loopback if ip.version == 4 else int(ip) == 1)


_warned_plain_http: dict = {}
_WARNED_PLAIN_HTTP_MAX = 1024


def _plain_http_once(key: Any, base: str, read_url: Any = _sync_scheme_host) -> bool:
    scheme, host = read_url(base)
    if scheme != "http" or _is_local_host(host):
        return False
    with _warn_lock:
        if key in _warned_plain_http:
            return False
        _warned_plain_http[key] = True
        while len(_warned_plain_http) > _WARNED_PLAIN_HTTP_MAX:
            del _warned_plain_http[next(iter(_warned_plain_http))]
    return True


def _warn_if_plain_http(base: str, read_url: Any = _sync_scheme_host) -> None:
    if _plain_http_once(("base_url", base), base, read_url):
        warnings.warn("wontopos: base_url uses plain HTTP on a non-local host, so the API key travels unencrypted. Use https://.",
                      stacklevel=3)


def _never_sent(exc: requests.exceptions.ConnectionError) -> bool:
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return True
    try:
        from urllib3.exceptions import ConnectTimeoutError, MaxRetryError, NewConnectionError
    except ImportError:  # pragma: no cover — urllib3 always ships with requests
        return False
    named = getattr(urllib3.exceptions, "NameResolutionError", NewConnectionError)
    reason = exc.args[0] if exc.args else None
    if isinstance(reason, MaxRetryError):
        reason = reason.reason
    return isinstance(reason, (NewConnectionError, ConnectTimeoutError, named))


def _timed_out(exc: BaseException) -> bool:
    if isinstance(exc, requests.exceptions.Timeout):
        return True
    try:
        from urllib3.exceptions import MaxRetryError, ReadTimeoutError, TimeoutError as U3Timeout
    except ImportError:  # pragma: no cover — urllib3 always ships with requests
        return False
    reason = exc.args[0] if exc.args else None
    if isinstance(reason, MaxRetryError):
        reason = reason.reason
    return isinstance(reason, (ReadTimeoutError, U3Timeout))


_MAX_WAIT = 30.0
_DEFAULT_TIMEOUT = 30.0
_STORE_TIMEOUT = 200.0
_STORE_PATHS = frozenset({"/api/v1/memory/store", "/api/v1/memory/store-turn", "/api/v1/memory/bulk-store"})
_DELTA_SECONDS_RE = re.compile(r"[0-9]+")
_HTTP_DATE_RE = re.compile(
    r"[A-Z][a-z]{2}, [0-9]{2} [A-Z][a-z]{2} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT"
    r"|[A-Z][a-z]{5,8}, [0-9]{2}-[A-Z][a-z]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT"
    r"|[A-Z][a-z]{2} [A-Z][a-z]{2} [ 0-9][0-9] [0-9]{2}:[0-9]{2}:[0-9]{2} [0-9]{4}"
)


def _parse_http_date(v: str) -> Optional[datetime]:
    if not _HTTP_DATE_RE.fullmatch(v):
        return None
    try:
        dt = parsedate_to_datetime(v)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if dt is None:
        return None
    dt = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    if "-" in v:
        year = 2000 + dt.year % 100
        if year > datetime.now(timezone.utc).year + 50:
            year -= 100
        dt = dt.replace(year=year)
    return dt


def _parse_retry_after(value: Any) -> Optional[float]:
    if not isinstance(value, str):
        return None
    v = value.strip()
    if _DELTA_SECONDS_RE.fullmatch(v):
        digits = v.lstrip("0") or "0"
        return float(2**31) if len(digits) > 10 else float(min(int(digits), 2**31))
    dt = _parse_http_date(v)
    if dt is None:
        return None
    return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())


_warned_retirements: set = set()
_WARNED_RETIREMENTS_MAX = 64


def _warn_if_retiring(model: Optional[str], headers: Any) -> None:
    value = headers.get("Sunset") if headers is not None else None
    if not isinstance(value, str) or not value.strip():
        return
    dt = _parse_http_date(value.strip())
    if dt is None:
        return
    day = dt.astimezone(timezone.utc).date().isoformat()
    key = (model or "", day)
    with _warn_lock:
        if key in _warned_retirements or len(_warned_retirements) >= _WARNED_RETIREMENTS_MAX:
            return
        _warned_retirements.add(key)
    which = f'model "{model}"' if model else "the model this client uses"
    logging.getLogger("wontopos").warning(
        "%s retires on %s (UTC). From then on its calls answer 410, except delete_store. "
        "list_models() lists the models you can move to.",
        which, day,
    )


def _new_idempotency_key() -> str:
    return f"sdk-{os.urandom(16).hex()}"


def _backoff(attempt: int, retry_after: Optional[str] = None) -> float:
    ra = _parse_retry_after(retry_after)
    if ra is not None:
        return min(_MAX_WAIT, ra)
    return min(8.0, 0.5 * (2 ** min(attempt, 4))) + random.random() * 0.25


def _lock_wait(err: "WosError") -> Optional[float]:
    d = err.details if isinstance(err.details, dict) else {}
    ms = d.get("retry_after_ms")
    if "conflicts_with" in d or isinstance(ms, bool) or not isinstance(ms, (int, float)):
        return None
    if not 0 <= ms <= _MAX_WAIT * 1000:
        return None
    return ms / 1000.0


def _retry_wait(status: int, safe: bool, retry_after: Optional[str], err: Optional["WosError"],
                attempt: int) -> Optional[float]:
    if status == 409:
        wait = _lock_wait(err)
        if wait is None or wait > _MAX_WAIT:
            return None
        return max(wait, _backoff(attempt))
    if status in _RETRY_ALWAYS or (status in _RETRY_IF_IDEMPOTENT and safe):
        ra = _parse_retry_after(retry_after)
        if ra is None:
            return _backoff(attempt)
        return ra if ra <= _MAX_WAIT else None
    return None


_MAX_ERR_MSG = 4096
_MAX_ERR_FIELD = 256


def _strip_ctl(text: str) -> str:
    return _HEADER_CTL_RE.sub("", text)


def _parse_ok(status: int, text: str) -> dict:
    if not text.strip():
        if status in _NO_BODY_STATUS:
            return {}
        raise WosError(status, "empty response body — expected a JSON object")
    try:
        data = json.loads(text)
    except (ValueError, RecursionError) as e:
        raise WosError(status, f"invalid JSON in response: {e}") from None
    if not isinstance(data, dict):
        raise WosError(status, f"expected a JSON object in the response, got {type(data).__name__}")
    return data


def _parse_error(status: int, text: str, unread: Optional[str] = None) -> "WosError":
    msg: Any = None
    request_id: Optional[str] = None
    etype: Optional[str] = None
    details: Optional[dict] = None
    try:
        data = json.loads(text)
    except Exception:
        data = None
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            t = err.get("type")
            said_t = isinstance(t, str) and bool(_strip_ctl(t).strip())
            etype = _clean_text(t, _MAX_ERR_FIELD) if said_t else None
            m = err.get("message")
            msg = m if isinstance(m, str) and _strip_ctl(m).strip() else (t if said_t else None)
            rid = err.get("request_id")
            request_id = _clean_text(rid, _MAX_ERR_FIELD) if isinstance(rid, str) and _strip_ctl(rid).strip() else None
            details = _clean_details({k: v for k, v in err.items() if k not in ("message", "type", "request_id")})
        elif isinstance(err, str):
            msg = err
        elif isinstance(data.get("message"), str) and _strip_ctl(data["message"]).strip():
            msg = data["message"]
    if msg is None:
        msg = text
    msg = _clean_text(msg)
    if not msg.strip():
        msg = f"HTTP {status}"
    if unread:
        msg += f" (the error body could not be read: {_clean_text(unread)[:512]})"
    return _make_error(status, msg, request_id=request_id, type=etype, details=details)


def _clean_text(text: str, limit: int = _MAX_ERR_MSG) -> str:
    cut = len(text) > limit * 2
    text = _strip_ctl(text[: limit * 2])
    if cut or len(text) > limit:
        text = text[:limit] + "…(truncated)"
    return text


_MAX_ERR_DETAILS = 8192
def _clean_details(d: dict) -> Optional[dict]:
    if not d:
        return None

    def clean(v: Any) -> Any:
        root: list = [None]
        stack: list = [(v, root, 0)]
        while stack:
            x, into, at = stack.pop()
            if isinstance(x, str):
                into[at] = _clean_text(x)
            elif isinstance(x, list):
                into[at] = out = [None] * len(x)
                stack.extend((y, out, i) for i, y in enumerate(x))
            elif isinstance(x, dict):
                into[at] = out = {}
                kids = []
                for k, y in x.items():
                    key = _clean_text(k)
                    out[key] = None
                    kids.append((y, out, key))
                stack.extend(reversed(kids))
            else:
                into[at] = x
        return root[0]

    def size(x: Any) -> int:
        return len(json.dumps(x, ensure_ascii=False, separators=(",", ":")))

    try:
        entries = [(k, v, size(k) + size(v) + 2) for k, v in clean(d).items()]
    except (TypeError, ValueError, RecursionError):
        return None
    keep, total = set(), 2
    for k, _, n in sorted(entries, key=lambda e: e[2]):
        if total + n > _MAX_ERR_DETAILS:
            break
        keep.add(k)
        total += n
    return {k: v for k, v, _ in entries if k in keep} or None


def _billed_to(headers: Any) -> Optional[str]:
    v = headers.get("X-WOS-Billed-To") if headers is not None else None
    return "balance" if isinstance(v, str) and v.strip().lower() == "balance" else None


_paid_calls: Any = contextvars.ContextVar("_paid_calls", default=None)


def _count_paid(billed_to: Optional[str]) -> None:
    calls = _paid_calls.get()
    if calls is not None and billed_to == "balance":
        calls.append(billed_to)


def _header_digits(headers: Any, name: str) -> Optional[str]:
    v = headers.get(name) if headers is not None else None
    v = v.strip(" \t") if isinstance(v, str) else ""
    return (v.lstrip("0") or "0") if _DELTA_SECONDS_RE.fullmatch(v) else None


def _week_used(headers: Any) -> Optional[int]:
    digits = _header_digits(headers, "X-WOS-Week-Used")
    return int(digits) if digits is not None and len(digits) <= 3 and int(digits) <= 100 else None


def _read_billing(client: Any, status: int, headers: Any) -> None:
    week = _week_used(headers)
    if week is not None or 200 <= status < 300:
        client._week_used = week
    if 200 <= status < 300:
        client._billed_to = _billed_to(headers)
        _count_paid(client._billed_to)


def _parse_rate_limit(headers: Any) -> Optional[dict]:
    def _int(name: str) -> Optional[int]:
        digits = _header_digits(headers, name)
        return int(digits) if digits is not None and len(digits) <= 16 and int(digits) < 2 ** 53 else None
    limit, remaining, reset = _int("X-RateLimit-Limit"), _int("X-RateLimit-Remaining"), _int("X-RateLimit-Reset")
    if limit is None and remaining is None and reset is None:
        return None
    return {"limit": limit, "remaining": remaining, "reset": reset}
_RETRY_ALWAYS = (429,)
_RETRY_IF_IDEMPOTENT = (408, 502, 503, 504)
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS"})
_READ_POSTS = frozenset({
    "/api/v1/memory/search", "/api/v1/memory/recall", "/api/v1/memory/get", "/api/v1/memory/list", "/api/v1/memory/stats",
    "/api/v1/memory/history", "/api/v1/memory/lineage", "/api/v1/memory/by-speaker", "/api/v1/memory/images",
    "/api/v1/memory/image", "/api/v1/engram/run", "/api/v1/won/revisions", "/api/v1/memory/around", "/api/v1/memory/why",
    "/api/v1/memory/retrievals", "/api/v1/memory/sleep", "/api/v1/memory/sleep/verify", "/api/v1/memory/seen",
    "/api/v1/memory/work/state", "/api/v1/memory/self", "/api/v1/memory/self/open", "/api/v1/memory/self/history",
    "/api/v1/memory/self/notes", "/api/v1/memory/channel/read",
})
_RETRYABLE_BODY_WAIT = 1.0
def _deadline_at(deadline: Optional[float]) -> Optional[float]:
    return None if deadline is None else _time.monotonic() + deadline


class _Clock:

    __slots__ = ("end", "_why", "by_deadline")

    def __init__(self, timeout: float, deadline_at: Optional[float], deadline: Optional[float]):
        now = _time.monotonic()
        if deadline_at is not None and deadline_at <= now:
            raise APIConnectionError(0, f"deadline of {deadline}s exhausted")
        self.end = now + timeout
        self._why = f"request timed out after {timeout}s"
        self.by_deadline = False
        if deadline_at is not None and deadline_at < self.end:
            self.end = deadline_at
            self._why = f"deadline of {deadline}s exhausted"
            self.by_deadline = True

    def left(self) -> float:
        return self.end - _time.monotonic()

    def within(self, seconds: float) -> "_Clock":
        c = _copy.copy(self)
        c.end = min(self.end, _time.monotonic() + seconds)
        return c

    def error(self) -> "APIConnectionError":
        return APIConnectionError(0, self._why)


class _Expired(Exception):
    pass


class _Failure:

    __slots__ = ("reason", "never_sent", "timed_out", "dropped")

    def __init__(self, reason: str, *, never_sent: bool = False, timed_out: bool = False,
                 dropped: bool = False):
        self.reason = reason
        self.never_sent = never_sent
        self.timed_out = timed_out
        self.dropped = dropped


def _exception_chain(e: BaseException) -> list:
    chain: list = []
    cur: Optional[BaseException] = e
    while cur is not None and len(chain) < 16 and not any(cur is c for c in chain):
        chain.append(cur)
        nxt = getattr(cur, "reason", None)
        if not isinstance(nxt, BaseException):
            nxt = next((a for a in getattr(cur, "args", ()) if isinstance(a, BaseException)), None)
        cur = nxt if nxt is not None else (cur.__cause__ or cur.__context__)
    return chain


def _transport_reason(e: BaseException) -> str:
    chain = _exception_chain(e)
    kind = type(e).__name__
    for x in chain:
        if isinstance(x, ssl.SSLCertVerificationError):
            why = _strip_ctl(str(x.verify_message or "verification failed"))
            return f"{kind} (TLS certificate: {why})"
        if isinstance(x, ValueError) and type(x).__name__ == "CertificateError":
            return f"{kind} (TLS certificate: {_strip_ctl(str(x))})"
        if isinstance(x, ssl.SSLError):
            return f"{kind} (TLS: {_strip_ctl(str(x.reason or 'handshake failed'))})"
    for x in chain:
        name = type(x).__name__
        if isinstance(x, ConnectionRefusedError):
            return f"{kind} (connection refused)"
        if isinstance(x, socket.gaierror) or name == "NameResolutionError":
            return f"{kind} (name resolution failed)"
        if isinstance(x, (socket.timeout, TimeoutError)) or "Timeout" in name:
            return f"{kind} (timed out)"
        if name in ("RemoteDisconnected", "IncompleteRead", "ProtocolError",
                    "RemoteProtocolError", "ChunkedEncodingError", "ReadError", "WriteError"):
            return f"{kind} (connection closed before the response completed)"
        if isinstance(x, (ConnectionResetError, BrokenPipeError, ConnectionAbortedError)):
            return f"{kind} (connection reset)"
    return kind


def _net_error(where: str, reason: str) -> "APIConnectionError":
    return APIConnectionError(0, f"network error: {where}: {reason}")


class _Call:

    def __init__(self, method: str, path: str, retries: int, deadline: Optional[float], *,
                 removes: Optional[bool] = None, reads: bool = False, keyed: bool = False):
        self.method = method.upper()
        self.where = f"{method} {path.split('?', 1)[0]}"
        self.attempts = retries + 1
        self.attempt = 0
        self.deadline = deadline
        self.deadline_at = _deadline_at(deadline)
        self.removes = self.method == "DELETE" if removes is None else removes
        self.reads = (reads or self.method in _IDEMPOTENT_METHODS
                      or (self.method == "POST" and path.split("?", 1)[0] in _READ_POSTS))
        self.replay_safe = self.reads or keyed
        self.maybe_applied = False
        self.previous: Optional[WosError] = None
        self.unread: Optional[str] = None

    def clock(self, timeout: float) -> _Clock:
        if (self.previous is not None and self.deadline_at is not None
                and self.deadline_at <= _time.monotonic()):
            raise self._final(self.previous)
        return _Clock(timeout, self.deadline_at, self.deadline)

    def _fits(self, wait: float) -> bool:
        return self.deadline_at is None or wait <= self.deadline_at - _time.monotonic()

    def _more(self) -> bool:
        return self.attempt + 1 < self.attempts

    def _spent(self) -> bool:
        return self.deadline_at is not None and self.deadline_at - _time.monotonic() <= 0.001

    def retries_regardless(self, status: int, headers: Any) -> bool:
        if status == 409 or not self._more():
            return False
        wait = _retry_wait(status, self.replay_safe, headers.get("Retry-After"), None, self.attempt)
        return wait is not None and self._fits(wait)

    def after_failure(self, f: _Failure, clock: _Clock) -> float:
        previous, self.previous = self.previous, None
        if f.timed_out:
            if clock.by_deadline and previous is not None and self.reads:
                raise self._final(previous)
            raise clock.error()
        retryable = f.never_sent or (f.dropped and self.replay_safe)
        wait = _backoff(self.attempt)
        if retryable and (self._more() or self._spent()) and not self._fits(wait):
            if previous is not None:
                raise self._final(previous)
            raise APIConnectionError(0, f"deadline of {self.deadline}s exhausted")
        if not retryable or not self._more():
            raise _net_error(self.where, f.reason)
        self.maybe_applied = self.maybe_applied or not f.never_sent
        _logger.debug("%s: %s — retrying in %.1fs (attempt %d/%d)",
                      self.where, f.reason, wait, self.attempt + 1, self.attempts)
        return wait

    def after_status(self, status: int, headers: Any, payload: bytes) -> float:
        if 300 <= status < 400:
            raise WosError(status, "unexpected redirect — refused (the API key never follows a redirect). "
                                   "Check base_url: exact host, https://.")
        err = _parse_error(status, _model_text(payload), self.unread)
        self.unread = None
        retry_after = headers.get("Retry-After")
        if isinstance(err, RateLimitError):
            asked = _parse_retry_after(retry_after)
            err.retry_after = None if asked is None else float(math.ceil(asked))
        wait = _retry_wait(status, self.replay_safe, retry_after, err, self.attempt)
        if wait is None or not self._more() or not self._fits(wait):
            raise self._final(err)
        if status in _RETRY_IF_IDEMPOTENT:
            self.maybe_applied = True
        self.previous = err
        _logger.debug("%s -> %d — retrying in %.1fs (attempt %d/%d)",
                      self.where, status, wait, self.attempt + 1, self.attempts)
        return wait

    def _final(self, err: WosError) -> WosError:
        if self.removes and self.maybe_applied and isinstance(err, NotFoundError):
            return NotFoundError(
                err.status, err.message + " (an earlier attempt may already have deleted it)",
                request_id=err.request_id, type=err.type, details=err.details,
            )
        return err


def _encode_json(body: Any) -> Optional[bytes]:
    if body is None:
        return None
    try:
        text = json.dumps(body, allow_nan=False, ensure_ascii=False, separators=(",", ":"), default=_json_default)
    except ValueError as e:
        raise ValueError(f"request body is not valid JSON: {e}") from None
    except TypeError as e:
        raise TypeError(f"request body is not JSON-serializable: {e}") from None
    try:
        return text.encode("utf-8")
    except UnicodeEncodeError:
        return json.dumps(body, allow_nan=False, separators=(",", ":"), default=_json_default).encode("ascii")


def _plan_call(client: Any, method: str, path: str, json_body: Optional[dict], model: Optional[str],
               idempotency_key: Optional[str], removes: Optional[bool], reads: bool) -> tuple:
    eff_model = _check_model(model) if model else client._model
    headers = {"X-WOS-Model": eff_model} if eff_model else {}
    idem = _idem_headers(idempotency_key)
    if idem:
        headers.update(idem)
    data = _encode_json(json_body)
    call = _Call(method, path, client._retries, client._deadline, removes=removes, reads=reads,
                 keyed=idem is not None)
    return eff_model, headers or None, data, f"{client._base}{path}", call


def _answered(client: Any, call: _Call, model: Optional[str], got: tuple, start: float) -> bool:
    status, rheaders, _ = got
    if not 300 <= status < 400:
        client._rate_limit = _parse_rate_limit(rheaders) or client._rate_limit
        _warn_if_retiring(model, rheaders)
    _logger.debug("%s -> %d in %.0fms (attempt %d/%d)", call.where, status,
                  (_time.monotonic() - start) * 1000, call.attempt + 1, call.attempts)
    return 200 <= status < 300


def _model_text(raw: bytes) -> str:
    text = bytes(raw).decode("utf-8", "replace")
    return text[1:] if text.startswith("﻿") else text


def _json_answer(got: tuple, made_key: bool) -> dict:
    status, headers, payload = got
    data = _parse_ok(status, _model_text(payload))
    if not made_key and headers.get("Idempotent-Replayed") == "true" and "replayed" not in data:
        data["replayed"] = True
    return data


def _off_the_wire(r: Any) -> bool:
    raw = getattr(r, "raw", None)
    if raw is None or not hasattr(raw, "_fp"):
        return False
    if raw._fp is None:
        return True
    try:
        return bool(_u3_is_fp_closed(raw._fp))
    except Exception:
        return False
