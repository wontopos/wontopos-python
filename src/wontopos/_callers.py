from __future__ import annotations

import json
import math
import re
import threading
import warnings
from typing import Any, Optional

import requests

from . import (
    AskSignal, ModelCall, ModelCallError, _ANTHROPIC_BASE_URL, _ANTHROPIC_CACHE_MIN_TOKENS, _ANTHROPIC_VERSION,
    _JS_SPACE_CLASS, _MODEL_RESPONSE_MAX_BYTES, _ProviderUnreachable, _ROUTER_TEXT, _Undecodable, _as_list,
    _body_inflater, _check_model_name, _inflate_into, _is_whole, _jq, _js_json, _js_str, _js_trim, _logger,
    _provider_base, _raw_chunks, _time, _transport_reason, _within,
)
from ._checks import _as_dict, _mask_key
from ._http import (
    _TLSAdapter, _Watchdog, _async_tls_context, _attempt_local, _model_text, _parse_retry_after, _plain_http_once,
    _sessions, _strip_ctl, _url_login,
)
from ._memory_tools import _json_loads

_MODEL_KEY_SPACE = re.compile(f"[{_JS_SPACE_CLASS}\\x00-\\x1f\\x7f]")
_NOT_ASCII = re.compile(r"[^\x00-\x7f]")


def _check_model_key(raw: Any, what: str) -> str:
    if not isinstance(raw, str) or not _js_trim(raw):
        raise ValueError(f"{what} is required: the model provider's key.")
    key = _js_trim(raw)
    if _MODEL_KEY_SPACE.search(key):
        raise ValueError(f"{what} contains whitespace or a control character: check for a stray newline.")
    if _NOT_ASCII.search(key):
        raise ValueError(f"{what} contains a non-ASCII character: copy the key from a plain-text field.")
    if key.startswith("wos-"):
        raise ValueError(f"{what} starts with wos-, so it is a Wontopos key: a Wontopos key never goes to a model "
                         "provider. Give the provider's own key.")
    return key


def _provider_url(raw: Any, wontopos_base: Optional[str] = None, keyed: bool = True,
                  give: str = "the API origin without them, e.g. https://api.anthropic.com") -> str:
    if isinstance(raw, str) and ("?" in raw or "#" in raw):
        raise ValueError(f"the model provider's base URL has a ? query or a # anchor: give {give}.")
    base = _provider_base(raw, wontopos_base)
    if keyed and _plain_http_once(("provider", base), base):
        warnings.warn("wontopos: the model provider's base URL uses plain HTTP on a non-local host, so the model key "
                      "travels unencrypted. Use https://.", stacklevel=3)
    return base


def _is_service_host(host: str) -> bool:
    h = host.lower().rstrip(".")
    return h == "wontopos.com" or h.endswith(".wontopos.com")


def _url_host(host: str) -> str:
    import unicodedata
    import urllib.parse

    host = urllib.parse.unquote(host)
    try:
        return host.encode("idna").decode("ascii").lower()
    except UnicodeError:
        return unicodedata.normalize("NFKC", host).translate(_HOST_DOTS).lower()


def _url_origin(s: str, *, trailing_dot: bool = False) -> Optional[tuple]:
    import urllib.parse

    try:
        u = urllib.parse.urlsplit(s)
        port = u.port
    except ValueError:
        return None
    scheme = u.scheme.lower()
    host = _url_host(u.hostname or "")
    if not trailing_dot:
        host = host.rstrip(".")
    if scheme not in ("http", "https") or not host:
        return None
    return scheme, host, port if port is not None else (443 if scheme == "https" else 80)


_HOST_DOTS = str.maketrans("\u3002\uff0e\uff61", "...")


def _host_key(host: str) -> str:
    import unicodedata

    return unicodedata.normalize("NFKC", host).translate(_HOST_DOTS).lower().rstrip(".")


_PROVIDER_REASONS = ("credit", "spend_limit", "auth", "rate_limit", "overloaded", "bad_model", "bad_request",
                     "too_large")
_DECIMAL_TEXT = re.compile(r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


def _clean_capped(s: str, n: int) -> str:
    clean = _strip_ctl(s)
    return clean[:n] + "…(truncated)" if len(clean) > n else clean


def _provider_error_parts(text: str) -> dict:
    out: dict = {"message": text, "request_id": None, "type": None, "details": None}
    try:
        data = json.loads(text)
    except (ValueError, RecursionError):
        return out
    if not isinstance(data, dict):
        return out
    err = data.get("error")
    if isinstance(err, dict):
        m, t, rid = err.get("message"), err.get("type"), err.get("request_id")
        rest = {k: v for k, v in err.items() if k not in ("message", "type", "request_id")}
        out["message"] = m if isinstance(m, str) else t if isinstance(t, str) else text
        out["request_id"] = rid if isinstance(rid, str) else None
        out["type"] = t if isinstance(t, str) else None
        out["details"] = rest or None
    elif isinstance(err, str):
        out["message"] = err
    elif isinstance(data.get("message"), str):
        out["message"] = data["message"]
    return out


def _provider_answer(status: int, headers: Any, text: str, reason_of: Any) -> dict:
    if not 200 <= status < 300:
        e = _provider_error_parts(text)
        details = e["details"] if isinstance(e["details"], dict) else {}
        code = details.get("code") if isinstance(details.get("code"), str) else None
        message = _clean_capped(e["message"] or f"HTTP {status}", 1024)
        etype = e["type"] if e["type"] is not None else code
        request_id = e["request_id"] or headers.get("request-id") or headers.get("x-request-id")
        raw_ms = headers.get("retry-after-ms")
        ms = float(raw_ms) if raw_ms is not None and _DECIMAL_TEXT.fullmatch(raw_ms.strip()) else math.nan
        wait = ms / 1000 if math.isfinite(ms) and ms >= 0 else _parse_retry_after(headers.get("retry-after"))
        reason = reason_of(status, f"{etype or ''} {code or ''} {message}") if reason_of else None
        raise ModelCallError(
            status, message,
            type=None if etype is None else _clean_capped(etype, 128),
            retry_after=wait if wait is not None else (1.0 if reason == "rate_limit" else None),
            request_id=None if request_id is None else _clean_capped(request_id, 256),
            reason=reason,
        )
    try:
        data = _json_loads(text)
    except (ValueError, RecursionError):
        raise ModelCallError(status, "the provider's answer is not JSON") from None
    if not isinstance(data, dict):
        raise ModelCallError(status, "the provider's answer is not a JSON object")
    return data


_ACCEPT = {"accept-encoding": "gzip, deflate"}
_PROVIDER_CODING_TEXTS = (
    "the provider's answer has stacked content encodings ({}); refusing to decode it",
    "the provider's answer has an unsupported content encoding {}",
)


def _too_large(status: int) -> Any:
    return lambda: ModelCallError(status, "the provider's answer is too large")


def _provider_head(status: int, headers: Any) -> None:
    if 300 <= status < 400:
        raise ModelCallError(status, "redirect refused: the model key never follows a redirect")
    cl = headers.get("Content-Length", "")
    if cl.isdigit() and int(cl) > _MODEL_RESPONSE_MAX_BYTES:
        raise ModelCallError(status, f"the provider's answer is too large ({cl} bytes)")


def _model_body_bytes(body: Any) -> bytes:
    text = _js_json(body)
    try:
        return text.encode("utf-8")
    except UnicodeEncodeError:  # pragma: no cover - _js_json escapes lone surrogates
        return text.encode("utf-8", "surrogatepass")


class _ProviderCaller:

    def __init__(self, base_url: str):
        self.base_url = base_url
        self._session: Optional[requests.Session] = None
        self._lock = threading.Lock()

    def __reduce__(self) -> Any:
        raise TypeError("a model caller cannot be pickled: it holds the provider's key. Build one per process.")

    def close(self) -> None:
        with self._lock:
            s, self._session = self._session, None
        if s is not None:
            s.close()

    def _sync_session(self) -> requests.Session:
        with self._lock:
            if self._session is None:
                s = requests.Session()
                adapter = _TLSAdapter()
                s.mount("https://", adapter)
                s.mount("http://", adapter)
                _sessions.add(s)
                self._session = s
            return self._session

    def _post(self, url: str, headers: dict, body: Any, signal: Optional[AskSignal],
              reason_of: Any = None) -> dict:
        data = _model_body_bytes(body)
        left = signal.time_left() if signal is not None else 600.0
        if left <= 0:
            raise _ProviderUnreachable("network error: the ask's time ran out")
        if signal is not None and signal.aborted:
            raise _ProviderUnreachable("network error: the call was cancelled")
        watchdog = _Watchdog(_time.monotonic() + left)
        if signal is not None:
            signal._clock.watch(watchdog)
        _attempt_local.watchdog = watchdog
        r = None
        try:
            try:
                r = self._sync_session().post(
                    url, data=data, headers={"content-type": "application/json", **_ACCEPT, **headers},
                    auth=_url_login, timeout=(min(10.0, left), left),
                    allow_redirects=False, stream=True,
                )
                status = r.status_code
                _provider_head(status, r.headers)
                inflater = _body_inflater(status, r.headers, _MODEL_RESPONSE_MAX_BYTES, _too_large(status),
                                          ModelCallError, _PROVIDER_CODING_TEXTS)
                raw = bytearray()
                received = 0
                for chunk in _raw_chunks(r):
                    received += len(chunk)
                    if received > _MODEL_RESPONSE_MAX_BYTES:
                        raise ModelCallError(status, "the provider's answer is too large")
                    _inflate_into(inflater, chunk, raw)
                _inflate_into(inflater, None, raw)
            except requests.exceptions.RequestException as e:
                raise _ProviderUnreachable(f"network error: {_transport_reason(e)}") from None
        finally:
            _attempt_local.watchdog = None
            watchdog.stop()
            if r is not None:
                r.close()
        return _provider_answer(status, r.headers, _model_text(raw), reason_of)

    async def _apost(self, url: str, headers: dict, body: Any, signal: Optional[AskSignal],
                     reason_of: Any = None) -> dict:
        import httpx

        data = _model_body_bytes(body)
        left = signal.time_left() if signal is not None else 600.0
        if left <= 0:
            raise _ProviderUnreachable("network error: the ask's time ran out")
        timeout = httpx.Timeout(left, connect=min(10.0, left))

        async def call() -> tuple:
            async with httpx.AsyncClient(verify=_async_tls_context(), follow_redirects=False,
                                         timeout=timeout) as http:
                async with http.stream("POST", url, content=data,
                                       headers={"content-type": "application/json", **_ACCEPT, **headers}) as r:
                    status = r.status_code
                    _provider_head(status, r.headers)
                    big = _too_large(status)
                    inflater = _body_inflater(status, r.headers, _MODEL_RESPONSE_MAX_BYTES, big, ModelCallError,
                                              _PROVIDER_CODING_TEXTS)
                    raw = bytearray()
                    received = 0
                    try:
                        async for chunk in r.aiter_raw():
                            received += len(chunk)
                            if received > _MODEL_RESPONSE_MAX_BYTES:
                                raise big()
                            if inflater is None:
                                raw.extend(chunk)
                            else:
                                inflater.feed(chunk, raw)
                        if inflater is not None:
                            inflater.finish(raw)
                    except _Undecodable:
                        raise httpx.DecodingError("could not decode the response body") from None
                    return status, r.headers, raw
        try:
            got = await _within(call(), left)
        except httpx.HTTPError as e:
            raise _ProviderUnreachable(f"network error: {_transport_reason(e)}") from None
        if got is None:
            raise _ProviderUnreachable("network error: the call ran past its time")
        status, rheaders, raw = got
        return _provider_answer(status, rheaders, _model_text(raw), reason_of)


class _AnthropicCaller(_ProviderCaller):

    def __init__(self, key: str, base_url: str, model: Optional[str], cache_ttl: str):
        super().__init__(base_url)
        self.model = model
        self._key = key
        self._ttl = cache_ttl
        self._headers = {"x-api-key": key, "anthropic-version": _ANTHROPIC_VERSION}

    def __repr__(self) -> str:
        return f"anthropic_caller({self.base_url}, api_key={_mask_key(self._key)})"

    def _messages_args(self, c: ModelCall, count: bool) -> tuple:
        path = "/v1/messages/count_tokens" if count else "/v1/messages"
        return f"{self.base_url}{path}", self._headers, _anthropic_body(c, count, self._ttl), c.signal

    def call(self, c: ModelCall) -> dict:
        return _anthropic_reply(self._post(*self._messages_args(c, False)))

    def count_tokens(self, c: ModelCall) -> int:
        return _counted(self._post(*self._messages_args(c, True)))

    async def _acall(self, c: ModelCall) -> dict:
        return _anthropic_reply(await self._apost(*self._messages_args(c, False)))

    async def _acount_tokens(self, c: ModelCall) -> int:
        return _counted(await self._apost(*self._messages_args(c, True)))


def _counted(r: dict) -> int:
    n = r.get("input_tokens")
    if not _is_whole(n) or n < 0:
        raise ModelCallError(200, "count_tokens answered without input_tokens")
    return int(n)


def anthropic_caller(api_key: str, *, model: Optional[str] = None, base_url: Optional[str] = None,
                     cache_ttl: Optional[str] = None) -> Any:
    """The default caller: Anthropic's Messages API."""
    key = _check_model_key(api_key, "api_key")
    base = _provider_url(_ANTHROPIC_BASE_URL if base_url is None else base_url)
    name = None if model is None else _check_model_name(model, "anthropic_caller model")
    if cache_ttl is not None and cache_ttl not in ("5m", "1h"):
        raise ValueError('cache_ttl must be "5m" or "1h".')
    return _AnthropicCaller(key, base, name, cache_ttl or "5m")


class _OpenAICaller(_ProviderCaller):

    _FORMATS = ("json_schema", "json_object", "none")

    def __init__(self, base_url: str, model: str, key: Optional[str], level: int, max_field: str, effort: bool):
        super().__init__(base_url)
        self.model = model
        self._key = key
        self._level = level
        self._max_field = max_field
        self._effort = effort
        self._headers = {} if key is None else {"authorization": f"Bearer {key}"}

    def __repr__(self) -> str:
        key = "none" if self._key is None else _mask_key(self._key)
        return f"openai_caller({self.base_url}, model={self.model}, api_key={key})"

    def _format(self, c: ModelCall) -> str:
        return self._FORMATS[self._level] if c.schema is not None else "none"

    def _refused(self, c: ModelCall, fmt: str, e: ModelCallError) -> bool:
        refused = (e.status in _FORMAT_REFUSED and (e.reason is None or e.reason == "bad_request")
                   and _FORMAT_WORDS.search(e.message or "") is not None)
        if c.schema is None or fmt == "none" or not refused or (c.signal is not None and c.signal.aborted):
            return False
        with self._lock:
            self._level = max(self._level, self._FORMATS.index(fmt) + 1)
        _logger.debug("openai_caller %s: response_format %s refused (%d); asking with %s", self.base_url, fmt,
                      e.status, self._FORMATS[self._level])
        return True

    def _args(self, c: ModelCall, fmt: str) -> tuple:
        body = _openai_body(c, fmt, self._max_field, self._effort)
        return f"{self.base_url}/chat/completions", self._headers, body, c.signal, _openai_reason

    def call(self, c: ModelCall) -> dict:
        while True:
            fmt = self._format(c)
            try:
                return _openai_reply(self._post(*self._args(c, fmt)))
            except ModelCallError as e:
                if not self._refused(c, fmt, e):
                    raise

    async def _acall(self, c: ModelCall) -> dict:
        while True:
            fmt = self._format(c)
            try:
                return _openai_reply(await self._apost(*self._args(c, fmt)))
            except ModelCallError as e:
                if not self._refused(c, fmt, e):
                    raise


_FORMAT_REFUSED = (400, 415, 422)
_FORMAT_WORDS = re.compile(r"response_format|json_schema|json_object", re.I)


def openai_caller(*, base_url: Optional[str] = None, model: Optional[str] = None, api_key: Optional[str] = None,
                  response_format: Optional[str] = None, max_tokens_field: Optional[str] = None,
                  reasoning_effort: Optional[bool] = None) -> Any:
    """A caller for any server that speaks OpenAI's chat completions."""
    base = _provider_url(base_url, None, not (api_key is None or api_key == ""),
                         "the base URL without them, e.g. https://api.openai.com/v1")
    name = _check_model_name(model, "openai_caller model")
    key = None if api_key is None or api_key == "" else _check_model_key(api_key, "api_key")
    formats = _OpenAICaller._FORMATS
    if response_format is not None and response_format not in formats:
        raise ValueError('response_format must be "json_schema", "json_object" or "none".')
    level = 0 if response_format is None else formats.index(response_format)
    origin = _url_origin(base, trailing_dot=True)
    field = max_tokens_field
    if field is None:
        field = "max_completion_tokens" if origin is not None and origin[1] == "api.openai.com" else "max_tokens"
    if field not in ("max_tokens", "max_completion_tokens"):
        raise ValueError('max_tokens_field must be "max_tokens" or "max_completion_tokens".')
    if reasoning_effort is not None and not isinstance(reasoning_effort, bool):
        raise ValueError(f"reasoning_effort must be true or false, got {_jq(reasoning_effort)}.")
    return _OpenAICaller(base, name, key, level, field, reasoning_effort is True)


_CREDIT_WORDS = re.compile(
    r"insufficient[_ ]quota|insufficient[_ ]balance|arrearage|exceeded your current quota|credit|billing", re.I)
_CONTEXT_WORDS = re.compile(r"context[_ ]length|maximum context|too many tokens|prompt is too long", re.I)


def _openai_reason(status: int, said: str) -> Optional[str]:
    reasons = (("auth", status in (401, 403)), ("credit", status == 402 or _CREDIT_WORDS.search(said)),
               ("bad_model", status == 404), ("too_large", status == 413 or _CONTEXT_WORDS.search(said)),
               ("rate_limit", status == 429), ("overloaded", status == 408 or status >= 500),
               ("bad_request", status in (400, 422, 415)))
    return next((reason for reason, hit in reasons if hit), None)


def _openai_body(c: ModelCall, fmt: str, max_field: str, effort: bool) -> dict:
    system = "\n\n".join(t for t in c.system if len(t) > 0)
    if c.schema is not None and fmt != "json_schema":
        system += ("\n\n" if system else "") + f"{_ROUTER_TEXT['jsonReply']}\n{_js_json(c.schema)}"
    messages = ([{"role": "system", "content": system}] if system else []) + [{"role": "user", "content": c.user}]
    b: dict = {"model": c.model, "messages": messages, max_field: c.max_tokens}
    if c.schema is not None and fmt == "json_schema":
        b["response_format"] = {"type": "json_schema", "json_schema": {"name": "decision", "strict": True,
                                                                       "schema": c.schema}}
    elif c.schema is not None and fmt == "json_object":
        b["response_format"] = {"type": "json_object"}
    if effort and c.effort is not None:
        b["reasoning_effort"] = "high" if c.effort in ("xhigh", "max") else c.effort
    return b


_THINK_BLOCK = re.compile(f"\\A[{_JS_SPACE_CLASS}]*<think>[\\s\\S]*?</think>[{_JS_SPACE_CLASS}]*")


def _openai_reply(r: dict) -> dict:
    choices = _as_list(r.get("choices"))
    choice = _as_dict(choices[0]) if choices else {}
    msg = _as_dict(choice.get("message"))
    content = msg.get("content")
    if isinstance(content, str):
        text = content
    else:
        text = "".join(p["text"] if isinstance(p, dict) and isinstance(p.get("text"), str) else ""
                       for p in _as_list(content))
    text = _THINK_BLOCK.sub("", text, count=1)
    fr = choice.get("finish_reason")
    refusal = msg.get("refusal")
    refused = isinstance(refusal, str) and _js_trim(refusal)
    stop = "refusal" if refused or fr == "content_filter" else "max_tokens" if fr == "length" else "end"
    return {"text": text, "stop": stop, "usage": r.get("usage") if isinstance(r.get("usage"), dict) else None}


def _anthropic_body(c: ModelCall, count: bool, ttl: str) -> dict:
    parts = [t for t in c.system if len(t) > 0]
    marked = not count and c.system_tokens is not None and c.system_tokens >= _ANTHROPIC_CACHE_MIN_TOKENS
    system = []
    for i, text in enumerate(parts):
        block: dict = {"type": "text", "text": text}
        if marked and i == len(parts) - 1:
            block["cache_control"] = {"type": "ephemeral", "ttl": "1h"} if ttl == "1h" else {"type": "ephemeral"}
        system.append(block)
    b: dict = {"model": c.model, "system": system, "messages": [{"role": "user", "content": c.user}]}
    if not count:
        b["max_tokens"] = c.max_tokens
    oc: dict = {}
    if c.effort is not None and not count:
        oc["effort"] = c.effort
    if c.schema is not None:
        oc["format"] = {"type": "json_schema", "schema": c.schema}
    if oc:
        b["output_config"] = oc
    return b


def _anthropic_reply(r: dict) -> dict:
    text = "".join(b["text"] for b in _as_list(r.get("content"))
                   if isinstance(b, dict) and b.get("type") == "text" and isinstance(b.get("text"), str))
    s = r.get("stop_reason")
    stop = ("end" if s in ("end_turn", "stop_sequence") or s is None
            else "max_tokens" if s in ("max_tokens", "model_context_window_exceeded") else _js_str(s))
    return {"text": text, "stop": stop, "usage": r.get("usage") if isinstance(r.get("usage"), dict) else None}
