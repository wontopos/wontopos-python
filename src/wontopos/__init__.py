"""Wontopos — long-term memory for AI, in three lines.

    pip install wontopos

    from wontopos import Client
    mem = Client(api_key="wos-...", user_id="alice")       # set the store once
    mem.create_store()                                     # create it (uses the client's user_id)
    mem.add("I love hiking in Yosemite")                   # no user_id needed — uses "alice"
    print(mem.search("what does she like?"))

Async is the same surface with ``await`` (needs the extra: ``pip install "wontopos[async]"``):

    from wontopos import AsyncClient
    async with AsyncClient(api_key="wos-...", user_id="alice") as mem:
        await mem.add("I love hiking in Yosemite")
        hits = await mem.search("what does she like?")

Set ``user_id`` once on the client and every call uses it — handy when you work
with one store. Override any single call by passing ``user_id=`` to it. With no
``user_id`` anywhere, calls use the account's built-in ``default`` store.

Stores are explicit: the ``user_id`` you read/write under must exist first
(``create_store``), or the call returns 404. Every account starts with a
``default`` store, so the zero-setup path just works.

Recall quality does not depend on which language a memory was written in, so a
memory stored in one language is found by a question asked in another (Korean,
Japanese, Chinese, English, ...). Storing and searching call no LLM; you pay
retrieval, not generation.

The API key picks *which memory* (your account); ``model`` picks *which engine* reads
it. Models on the shared pool read the same memory, so you can store with one and
recall with another. Set a default on the client, override per call:

    mem = Client(api_key="wos-...", model="tablet-1")
    mem.recall("...", user_id="alice", model="tablet-1")   # this call only

Reliability: every call retries transient failures with exponential backoff +
jitter, honoring ``Retry-After`` up to 30s — 429 and a 409 for a write already in
flight on every call; 408/502/503/504 and connection errors only when a retry cannot
apply a write twice (idempotent calls, or a failure at connect time). ``timeout=``
bounds each attempt and ``deadline=`` the whole call, in wall-clock time. Tune with
``Client(retries=...)`` (0 disables), or per call site via the ``with_timeout()`` /
``with_retries()`` / ``with_deadline()`` clones.

Debugging: set ``WONTOPOS_LOG=debug`` (or configure the standard ``"wontopos"``
logger) to log method/path/status/timing/retries — never memory content,
request bodies, or the API key.

Security posture (not configurable off): TLS 1.2 is the floor and certificate
verification can never be disabled; redirects are refused so the API key can
never follow one to another host; responses over 64MB are refused; the key is
masked in ``repr``. Prefer ``Client.from_env()`` over keys in source code.
"""
from __future__ import annotations

import asyncio
import copy as _copy
import json
import logging
import math
import numbers
import os
import platform
import random
import re
import socket
import ssl
import sys
import threading
import time
import warnings
import weakref
import zlib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.client import IncompleteRead as _IncompleteRead
from typing import Any, Optional

import requests
import urllib3.connection as _u3_connection
import urllib3.connectionpool as _u3_pool
from urllib3.util.response import is_fp_closed as _u3_is_fp_closed

__version__ = "2.2.45"

# Without this, `from wontopos import *` also bound os, sys, json, re, time,
# random, logging, platform, ssl and requests in the caller's namespace, and they
# cluttered dir(wontopos) / autocomplete. Only the public surface is exported.
__all__ = [
    "Client", "AsyncClient", "WME",
    "WosError", "APIConnectionError", "AuthenticationError", "BadRequestError",
    "ConflictError", "NotFoundError", "PaymentRequiredError",
    "PermissionDeniedError", "RateLimitError", "ServerError", "GoneError",
    "DEFAULT_BASE_URL", "DEFAULT_MODEL", "__version__",
]

DEFAULT_BASE_URL = "https://api.wontopos.com"
# The engine every call uses unless the caller names another. Pin another with
# ``Client(key, model="tablet-1")`` or per call with ``model=``. What each model
# serves is listed under ``capabilities`` in ``list_models()``.
DEFAULT_MODEL = "tablet-2"

# Names the runtime in the User-Agent so a report can be reproduced on the version
# that produced it ("python 3.11 on Windows"). Platform only, nothing identifying.
_USER_AGENT = (
    f"wontopos-python/{__version__} "
    f"(python/{platform.python_version()}; {sys.platform}-{platform.machine()})"
)

# Wire-level debug logging: set WONTOPOS_LOG=debug (standard `logging` logger
# named "wontopos"). Logs method/path/status/timing/retries — NEVER memory
# content, request bodies, or the API key.
_logger = logging.getLogger("wontopos")
if os.environ.get("WONTOPOS_LOG", "").lower() == "debug":  # opt-in, like OPENAI_LOG
    _logger.setLevel(logging.DEBUG)
    if not _logger.handlers:
        _handler = logging.StreamHandler()
        _handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
        _logger.addHandler(_handler)
# 429 is refused before the request is processed, so nothing was stored and any
# method may retry. 502/503 are ambiguous for a write — they can arrive after the
# write was applied, and a retried POST would store it twice. Those retry only
# for idempotent methods.
_RETRY_ALWAYS = (429,)
# 504 carries the same ambiguity as 502/503. 408 is listed here rather than in
# _RETRY_ALWAYS: RFC 7231 says the server never got a complete request, but an
# intermediary can answer 408 on its own, and then it does not know what happened
# further along either.
_RETRY_IF_IDEMPOTENT = (408, 502, 503, 504)
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS"})
# POST routes that only read. When the deadline cuts short a retry of one, the answer
# before it still describes the call, as for an idempotent method.
_READ_POSTS = frozenset({
    "/api/v1/memory/search",
    "/api/v1/memory/recall",
    "/api/v1/memory/get",
    "/api/v1/memory/list",
    "/api/v1/memory/stats",
    "/api/v1/memory/history",
    "/api/v1/memory/lineage",
    "/api/v1/memory/by-speaker",
    "/api/v1/memory/images",
    "/api/v1/memory/image",
    "/api/v1/engram/run",
    "/api/v1/won/revisions",
})
# How long the error body of an answer that will be retried gets to arrive. The retry
# does not need it; it only fills in the error reported if the retry cannot be made.
_RETRYABLE_BODY_WAIT = 1.0
_LOOPBACK_HOSTS = ("localhost", "127.0.0.1", "::1", "0.0.0.0")
# Refuse to buffer absurd responses (real ones are a few KB) — protects the
# process if a custom base_url points somewhere broken or hostile.
_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
# Backstop for the paging helpers. A cursor that repeats is caught directly; one that
# is fresh on every page never ends, so the walk also has a ceiling: 20,000 pages, two
# million memories at 100 per page. The ceiling, or a repeat after a page with rows,
# raises rather than ending the walk, because a truncated list looks exactly like a
# complete one.
_MAX_PAGES = 20_000


def _truncated(why: str) -> RuntimeError:
    return RuntimeError(f"{why}. This is a truncated answer, not the whole store.")


def _cursor_repeats(cursor: Any, seen: set, rows: list) -> bool:
    """True when the walk should end quietly: the cursor came back after an empty page.
    A cursor that comes back after a page with rows would loop over them again, so it
    raises the truncated-answer error."""
    try:
        hash(cursor)
    except TypeError:  # an unhashable cursor from a broken server
        cursor = repr(cursor)
    if cursor not in seen:
        seen.add(cursor)
        return False
    if rows:
        raise _truncated("the service returned a page cursor it had already returned")
    return True


# What the API accepts as an ``Idempotency-Key``. Checked client-side so a bad key
# fails before the request instead of coming back as a 400 mid-retry.
# `fullmatch`, not `match`: in Python `$` also matches just before a trailing
# newline.
_IDEMPOTENCY_KEY_RE = re.compile(r"[A-Za-z0-9._:\-]{1,128}")



# ── The boundaries that go wrong quietly, said out loud ─────────────────────

#: The store ids the API accepts.
_STORE_ID_FORMAT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")


def _normalize_store_id(sid: str) -> str:
    """The normalized form the API derives from a store id: lowercased, anything outside
    ``[a-z0-9_]`` as ``_``."""
    return re.sub(r"[^a-z0-9_]", "_", sid.lower())


# Ids already warned about, so each one is reported once. A dict is used as an
# ORDERED set (insertion order) purely so the oldest entry can be evicted.
#
# Bounded: with one store per end user it sees one id per user, and past a few
# hundred distinct ids the warning has been read.
_WARNED_STORE_IDS_MAX = 1024
#: One lock for both process-global warn caches — see _warn_if_store_id_collapses.
_warn_lock = threading.Lock()
_warned_store_ids: "dict" = {}


def _assert_usable_store_id(user_id: Any) -> None:
    """A store id is usable when it is a non-blank string. Anything else is a bug at the
    call site, not a value to fall back from.

    Not just blank: any value that is not a usable store id. An integer primary key is
    the common one. ``user_id=0`` is falsy, so without this check it would reach the
    default store and write a customer's memories there.
    """
    if not isinstance(user_id, str) or not user_id.strip():
        raise ValueError(
            f"user_id must be a non-blank string; got {user_id!r}. Omit it to use the client's default "
            "store, or pass a real store id — anything else would silently write into the default store."
        )


def _warn_if_store_id_collapses(sid: str) -> None:
    """A store id is 1-64 ASCII letters, digits, ``.``, ``_`` and ``-``, starting with a
    letter or digit. Creating any other id (an email address, a name in another script) is
    refused (400), so key stores on an id of your own. Store ids compare without regard to
    case: ``Alice`` and ``alice`` name one store. Ids that differ only in ``.``, ``_`` or
    ``-`` cannot both exist: once ``alice-smith`` exists, creating ``alice.smith`` is
    refused (409) and using it answers 404. With one store per end user, derive the ids so
    two users never differ only in those three characters."""
    if not sid:
        return
    valid = _STORE_ID_FORMAT.match(sid) is not None
    normalized = _normalize_store_id(sid)
    if valid and normalized == sid:
        return
    # Under a lock. The set is process-global and every request touches it, so a
    # threaded app can run membership, insert, iterate and evict at the same time.
    # Unlocked, eviction raises KeyError and `next(iter(...))` raises
    # `RuntimeError: dictionary changed size during iteration`.
    # A warning helper must never be the thing that raises inside a customer's request.
    with _warn_lock:
        if sid in _warned_store_ids:
            return
        _warned_store_ids[sid] = None
        # Bounded: drop the oldest rather than stop recording, so a collision that
        # first appears late still gets its one warning.
        while len(_warned_store_ids) > _WARNED_STORE_IDS_MAX:
            _warned_store_ids.pop(next(iter(_warned_store_ids)))
    if not valid:
        logging.getLogger("wontopos").warning(
            "store id %r is not a valid store id: use 1-64 ASCII letters, digits, '.', '_' and "
            "'-', starting with a letter or digit. Creating it is refused (400).",
            sid,
        )
        return
    logging.getLogger("wontopos").warning(
        "store id %r normalizes to %r. Ids that differ only by case name this same store; one "
        "that differs only by punctuation cannot be created beside it (409) and is not found "
        "when used (404). If these ids come from your end users, normalize them yourself first "
        "so two people never compete for one name.",
        sid, normalized,
    )


#: The filter keys the API actually honours. Anything else is **dropped silently**, so a
#: typo widens the search and nobody hears about it. A warning rather than a refusal:
#: the API can grow a key before this package ships an update, and refusing would lock
#: callers out of a feature that already works.
_KNOWN_FILTER_KEYS = frozenset(
    {"categories", "event_from", "event_to", "time_from", "time_to", "min_importance"}
)
_warned_filter_keys: set = set()
_WARNED_FILTER_KEYS_MAX = 1024  # same cap, same reason, as the store-id warn set


def _warn_on_unknown_filters(filters: Any) -> None:
    if not isinstance(filters, dict):
        return
    for k in filters:
        if k in _KNOWN_FILTER_KEYS:
            continue
        # Same lock, same reason as the store-id set above: unlocked eviction races.
        with _warn_lock:
            if k in _warned_filter_keys:
                continue
            _warned_filter_keys.add(k)
            # Bounded like the store-id set: an app forwarding user-supplied filter keys
            # would otherwise grow it by one entry per typo.
            while len(_warned_filter_keys) > _WARNED_FILTER_KEYS_MAX:
                _warned_filter_keys.pop()
        logging.getLogger("wontopos").warning(
            "unknown search filter %r — the API drops keys it does not know, so this filter has "
            "NO effect and the search is wider than you think. Known keys: %s",
            k, ", ".join(sorted(_KNOWN_FILTER_KEYS)),
        )



SEARCH_LIMIT_MIN = 5
SEARCH_LIMIT_MAX = 20

#: ``recall``'s surrounding-context count. 0 to 20 inclusive; 0 attaches none.
CONTEXT_LIMIT_MIN = 0
CONTEXT_LIMIT_MAX = 20


def _is_int(v: Any) -> bool:
    """An integer of any kind (numpy's included), but not a bool."""
    return isinstance(v, numbers.Integral) and not isinstance(v, bool)


def _json_default(o: Any) -> Any:
    """Lets an integer of another type (numpy's) that passed the checks reach the wire."""
    if _is_int(o):
        return int(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _check_count(limit: int, name: str = "limit") -> None:
    """The 5-to-20 count shared by ``search`` and ``recall``, refused out of range
    rather than quietly adjusted: asking for 20 and silently getting 10 reads as "that
    is all there is".

    ``ValueError``, like every other argument check in this file. NOT ``WosError``:
    nothing was sent, and ``WosError`` with status 0 is ``APIConnectionError`` — "the
    request never got a response" — which a caller may retry.
    """
    if not _is_int(limit):
        raise ValueError(f"{name} must be an int, got {type(limit).__name__}")
    if limit < SEARCH_LIMIT_MIN or limit > SEARCH_LIMIT_MAX:
        raise ValueError(
            f"{name} must be an integer between {SEARCH_LIMIT_MIN} and {SEARCH_LIMIT_MAX}, got {limit}. "
            "Out of range is refused rather than adjusted, so a short answer always "
            "means the store was short."
        )


def _check_context_limit(n: int) -> None:
    """``recall``'s ``context_limit``, 0 to 20, refused out of range like the count above.

    0 is a real answer ("attach none"), not a missing value, so it must pass.
    """
    if not _is_int(n):
        raise ValueError(f"context_limit must be an int, got {type(n).__name__}")
    if n < CONTEXT_LIMIT_MIN or n > CONTEXT_LIMIT_MAX:
        raise ValueError(
            f"context_limit must be an integer between {CONTEXT_LIMIT_MIN} and {CONTEXT_LIMIT_MAX}, got {n}."
        )


# Page sizes the service accepts for image, speaker and revision pages, and for
# list_memories. Refused locally with the range, so the caller never sees a bare 400.
_PAGE_MIN, _PAGE_MAX = 5, 20
_LIST_MIN, _LIST_MAX, _LIST_DEFAULT = 1, 500, 100
_MAX_IMAGES = 5


def _check_int(value: Any, name: str, lo: int, hi: int) -> None:
    """An integer in ``lo..hi``; anything else (a bool, a float, NaN, a string, out of
    range) is refused before sending."""
    if not _is_int(value) or not lo <= value <= hi:
        raise ValueError(f"{name} must be an integer between {lo} and {hi}, got {value!r}.")


def _check_page(value: Any, name: str = "limit") -> None:
    _check_int(value, name, _PAGE_MIN, _PAGE_MAX)


def _check_include(include: Any) -> None:
    """``revisions``' side: refused before sending, since the service's 400 names no field."""
    if include is not None and include not in ("revised", "unrevised"):
        raise ValueError(f'include must be "revised" or "unrevised", got {include!r}.')


def _list_size(value: Any, name: str) -> int:
    """A ``list_memories`` page size: 1 to 500, with ``None`` meaning the default (100)."""
    if value is None:
        return _LIST_DEFAULT
    _check_int(value, name, _LIST_MIN, _LIST_MAX)
    return value


def _recall_body(store_id: str, query: str, form: Optional[str], tz: Optional[int],
                 limit: Optional[int], context_limit: Optional[int]) -> dict:
    """``recall``'s body, built once for the sync and async clients, so its checks live
    in one place."""
    body = _form_body({"user_id": store_id, "query": query}, form, tz)
    if limit is not None:
        _check_count(limit)
        body["limit"] = limit
    if context_limit is not None:
        _check_context_limit(context_limit)
        body["context_limit"] = context_limit
    return body


_KNOWN_SEARCH_KEYS = frozenset(
    {"cache_control", "speaker", "filters", "verify", "max_images", "form", "tz", "extra"}
)
# Keys that name a store or a request setting. Given to ``add`` as metadata they would
# be stored as a field while the write went to the default store.
_NOT_METADATA = frozenset({"userid", "storeid", "idempotencykey"})


def _names_a_store(key: Any) -> bool:
    return isinstance(key, str) and re.sub(r"[\s_.\-]", "", key.lower()) in _NOT_METADATA


#: The metadata keys the service keeps. It drops every other key.
_KEPT_METADATA_KEYS = frozenset({"speaker", "event_date", "category", "conversation_id"})
_warned_metadata_keys: set = set()


def _warn_on_unknown_metadata(md: dict) -> None:
    for k in md:
        if k in _KEPT_METADATA_KEYS:
            continue
        # Same lock and cap as the other warn sets.
        with _warn_lock:
            if k in _warned_metadata_keys:
                continue
            _warned_metadata_keys.add(k)
            while len(_warned_metadata_keys) > _WARNED_FILTER_KEYS_MAX:
                _warned_metadata_keys.pop()
        logging.getLogger("wontopos").warning(
            "metadata key %r is not kept: the service stores only %s and drops other keys, "
            "so this value is lost.",
            k, ", ".join(sorted(_KEPT_METADATA_KEYS)),
        )


def _metadata(metadata: Optional[dict], extra: dict) -> dict:
    # Checked before a None is set aside: userId=None is still a store named in the wrong
    # place, and dropping it first would send the write to the default store.
    bad = sorted(k for k in {**(metadata or {}), **extra} if _names_a_store(k))
    # A keyword of None is "not given": it neither overrides metadata= nor is sent.
    extra = {k: v for k, v in extra.items() if v is not None}
    md = {**metadata, **extra} if metadata else extra
    if bad:
        raise ValueError(
            f"{bad[0]!r} is not a metadata field: pass the store as user_id= and an "
            "idempotency key as idempotency_key=. Nothing was sent."
        )
    _warn_on_unknown_metadata(md)
    return md
# Named arguments of the call. Reaching the body through ``opts`` would let forwarded
# input choose someone else's store.
_RESERVED_SEARCH_KEYS = frozenset({"user_id", "query", "max_results"})


def _edit_within(a: str, b: str, max_edits: int) -> bool:
    """Within ``max_edits`` single-character edits. Short keys, one bad key at a time."""
    if abs(len(a) - len(b)) > max_edits:
        return False
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[len(b)] <= max_edits


def _check_search_opts(opts: dict) -> None:
    """Refuse a search option this client does not know.

    The service drops keys it does not recognise and answers normally, so a misspelled
    option cannot be told from one that worked: ``verify`` buys extra retrieval passes
    and ``verfy`` buys nothing while the reply still looks complete. Filters only warn,
    because a wrong filter still returns memories; a wrong option turns a paid feature
    off in silence. ``extra`` carries anything this version has not learned yet.
    """
    for k in opts:
        if k in _RESERVED_SEARCH_KEYS:
            raise ValueError(
                f"{k!r} is set by the call, not by options — pass it as an argument."
            )
        if k in _KNOWN_SEARCH_KEYS:
            continue
        near = next(
            (n for n in sorted(_KNOWN_SEARCH_KEYS) if n != "extra" and _edit_within(k, n, 2)), None
        )
        raise ValueError(
            f"unknown search option {k!r}"
            + (f" — did you mean {near!r}?" if near else "")
            + ". The service drops keys it does not know and answers anyway, so this "
            "would have looked like it worked. Pass it under 'extra' if the service "
            "accepts it and this client does not know it yet."
        )


def _search_body(store_id: str, query: str, limit: Optional[int], opts: dict,
                 verify: Optional[int] = None, max_images: Optional[int] = None) -> dict:
    """The /memory/search request body, built one way for every search method of both
    clients, so a check here holds for all of them.

    The store, the query and the count win over anything in ``extra``, and ``verify``
    and ``max_images`` over a stray copy in ``opts``.
    """
    _check_search_opts(opts)
    _warn_on_unknown_filters(opts.get("filters"))
    if limit is None:  # as for recall and page sizes
        limit = 10
    _check_count(limit)
    extra = opts.get("extra") or {}
    # None means "not given"; sent as null, speaker and cache_control are a 400.
    known = {k: v for k, v in opts.items() if k != "extra" and v is not None}
    body = {**extra, **known, "user_id": store_id, "query": query, "max_results": limit}
    if verify is not None:
        body["verify"] = verify
    if max_images is not None:
        body["max_images"] = max_images
    # Whichever way it came, ``extra`` included.
    if body.get("max_images") is not None:
        _check_int(body["max_images"], "max_images", 0, _MAX_IMAGES)
    return body

def _speakers_list(r: Any) -> dict:
    """``list_speakers``' reply with ``speakers`` always a list, as TypeScript returns it."""
    out = dict(r) if isinstance(r, dict) else {}
    out["speakers"] = _as_records(r.get("speakers") if isinstance(r, dict) else None)
    return out


def _reset_warning_state() -> None:
    """For tests — each warning is emitted once per process, globally."""
    _warned_store_ids.clear()
    _warned_filter_keys.clear()
    _warned_metadata_keys.clear()


def _normalize_image(image: Any) -> dict:
    """Put an image into the shape the API expects, checking only what cannot change.

    Deliberately NOT checked here: the size limits, which the service enforces — a 10MB
    request body, a 700px minimum edge, and the long edge downscaled to 1568px.

    What IS checked is the part that silently breaks: a ``data:image/jpeg;base64,`` prefix.
    Browsers and file pickers hand you the whole data URL, the API wants only what follows
    the comma, and passing the prefix fails much later with "not a readable image".
    """
    if not isinstance(image, dict):
        raise TypeError("image must be a dict — {'data': '<base64>'}")
    data = image.get("data")
    if not isinstance(data, str) or not data.strip():
        raise ValueError("image['data'] is required — base64 of the image (a data: URL is fine)")
    # Trim before looking for the prefix. Checked against the raw string, one leading
    # space (" data:image/png;base64,…") hides the prefix, and the literal
    # "data:image/png;base64," travels as part of the base64. The service answers 400
    # "image could not be read" and the caller has no way to tell why.
    data = data.strip()
    if data.startswith("data:"):
        comma = data.find(",")
        if comma != -1:
            data = data[comma + 1:]
    # Strip whitespace in the middle as well. `base64` and `openssl base64` wrap at 76
    # columns, and trimming only the ends leaves those line breaks in the payload. The
    # base64 alphabet contains no whitespace, so removing all of it is safe.
    data = "".join(data.split())
    if not data:
        raise ValueError("image['data'] is empty after stripping its data: prefix")
    out: dict = {"data": data}
    for k in ("reference", "taken_at"):
        if image.get(k) is not None:
            out[k] = image[k]
    return out


def _idem_headers(key: Optional[str]) -> Optional[dict]:
    """``{"Idempotency-Key": key}``, or None when no key was given."""
    if key is None:
        return None
    if not isinstance(key, str) or not _IDEMPOTENCY_KEY_RE.fullmatch(key):
        raise ValueError(
            f"invalid idempotency_key: {key!r} — 1-128 chars of [A-Za-z0-9._:-]"
        )
    return {"Idempotency-Key": key}
# Statuses that legitimately carry NO body (RFC 9110). Everything else must answer
# with a JSON object — see ``_parse_ok``.
_NO_BODY_STATUS = frozenset({204, 205, 304})
_MODEL_RE = re.compile(r"[A-Za-z0-9._-]+")
_ENV_KEYS = ("WONTOPOS_API_KEY", "WOS_API_KEY")


def _mask_key(key: str) -> str:
    """``wos-abc...wxyz`` — enough to tell keys apart, never enough to use."""
    return f"{key[:4]}...{key[-4:]}" if len(key) > 12 else "***"


def _clean_key(api_key: str) -> str:
    """Trim the key and reject inner whitespace — a stray newline from a file or
    env var otherwise turns into a mystery 401 (or a mangled header)."""
    if not api_key or not api_key.strip():
        raise ValueError("api_key is required")
    key = api_key.strip()
    if any(c.isspace() for c in key):
        raise ValueError("api_key contains whitespace - check for a stray newline or paste error")
    # ``isspace()`` is False for NUL, 0x01 and DEL, and the key becomes a header.
    if _HEADER_CTL_RE.search(key):
        raise ValueError("api_key contains a control character - check for a paste error")
    # Keys are ASCII by construction. A key pasted from a rich-text doc, Slack or a PDF
    # often has its hyphen turned into an en dash, which would otherwise fail deep in
    # http.client with an error that never names api_key. ASCII rather than latin-1,
    # so 'é' is caught here instead of reaching a 401.
    if not key.isascii():
        bad = next(c for c in key if not c.isascii())
        raise ValueError(
            f"api_key contains a non-ASCII character ({bad!r}) - rich text turns '-' into an en dash; "
            "copy the key from a plain-text field"
        )
    return key


def _check_model(model: Optional[str]) -> Optional[str]:
    """Model names travel in a header — allow only header-safe characters so a
    bad value fails here with a clear message, not inside the HTTP stack."""
    if model and not _MODEL_RE.fullmatch(model):
        raise ValueError(f"invalid model name: {model!r} (letters, digits, '.', '_', '-' only)")
    return model


def _as_list(x: Any) -> list:
    """A list field that the API promises. `x or []` only replaces a FALSY value —
    a broken/hostile server sending a truthy wrong type (``"memories": "oops"``)
    would slip through as a str/int/dict and blow up the caller's `for m in …`.
    Coerce anything that isn't actually a list to []."""
    return x if isinstance(x, list) else []


def _as_dict(x: Any) -> dict:
    """Same guard for a dict field (e.g. get()'s single memory): a non-dict is []'d to {}."""
    return x if isinstance(x, dict) else {}


def _memory_from_get(r: Any) -> dict:
    """``get`` answers ``{"memory": {...}}`` on some models and the row itself on others."""
    r = _as_dict(r)
    if isinstance(r.get("memory"), dict):
        return r["memory"]
    return r if isinstance(r.get("id"), str) else {}


def _copy_client(obj: Any, owner_flag: str, shared: tuple, memo: Optional[dict]) -> Any:
    """``copy.copy`` / ``copy.deepcopy`` of a client: the transport is shared, never
    owned, so closing the copy leaves the original working."""
    new = object.__new__(type(obj))
    if memo is not None:
        memo[id(obj)] = new
    for k, v in obj.__dict__.items():
        new.__dict__[k] = v if memo is None or k in shared else _copy.deepcopy(v, memo)
    new.__dict__[owner_flag] = False
    return new


def _as_records(x: Any) -> list:
    """A list of API records (memories, turns, models...) — every element is an
    object by contract. `_as_list` only fixes the CONTAINER: a hostile/broken
    server sending `{"memories": [null, 1, "x", {...}]}` still handed the caller
    those non-objects, and the first `m["content"]` raised a raw TypeError from
    inside user code. Drop elements that aren't objects and keep the valid
    records, so one bad element never poisons the batch (and never destroys it —
    the whole array staying usable is the point)."""
    return [m for m in _as_list(x) if isinstance(m, dict)]


# get()/delete() take the STORE first, unlike every payload-first method on this
# client (add, search, recall, engram...). With a default store set on the client,
# `mem.get(memory_id)` is the call people actually write, and it lands the id in
# `user_id`. Recover that case: a lone argument shaped like a memory id (a UUID, which
# is what the service mints) can only have been meant as the memory id. Only when
# memory_id was not passed at all: a memory_id passed as "" is a bug at the call site,
# and reading the store id as the memory id would act on the default store.
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


class _NoMemoryId(str):
    """The default ``memory_id``: an empty string that tells "not passed" apart from ""."""


_NO_ID: Any = _NoMemoryId("")


def _split_id_args(user_id: Optional[str], memory_id: str) -> tuple[Optional[str], str]:
    if memory_id is _NO_ID and isinstance(user_id, str) and _UUID_RE.match(user_id):
        return None, user_id
    return user_id, memory_id


def _merge_results(resp: Any) -> list:
    """Every memory a search returned — ``memories``, then ``self_memories``, then
    ``images`` — as one list, de-duplicated by id. Each keeps its ``speaker`` and, for
    a photo, its ``image_ref``, so a caller can still tell them apart."""
    if not isinstance(resp, dict):
        return []
    out = _as_records(resp.get("memories"))
    seen = {m["id"] for m in out if isinstance(m.get("id"), str)}
    for m in _as_records(resp.get("self_memories")) + _as_records(resp.get("images")):
        mid = m.get("id")
        if isinstance(mid, str):
            if mid in seen:
                continue
            seen.add(mid)
        out.append(m)
    return out


def _form_body(body: dict, form: Optional[str], tz: Optional[int]) -> dict:
    """Attach a delivery-form override (``"memoir"``/``"archive"``) and timezone to a
    request body, the same way search passes them. A model with the ``forms`` capability
    renders every returned memory's time in that form. Server validates the form (400 on
    unknown)."""
    if form is not None:
        body["form"] = form
    if tz is not None:
        body["tz"] = tz
    return body


# Control characters (CR/LF/NUL/…) in a value bound for an HTTP header are a
# request-splitting/header-injection vector. The HTTP stack blocks CR/LF today,
# but the SDK rejects them itself so a bad value fails with a clear message and
# we never depend on the library catching it.
_HEADER_CTL_RE = re.compile(r"[\x00-\x1f\x7f]")
# A value interpolated into a URL PATH must be a plain id — anything else ('/',
# '?', '#', '..', whitespace) re-targets the request to a different path.
_PATH_ID_RE = re.compile(r"[A-Za-z0-9._~-]+")


def _reject_ctl(name: str, value: str) -> str:
    if _HEADER_CTL_RE.search(value):
        raise ValueError(f"{name} contains a control character (a stray newline?) — refusing to put it in a header")
    return value


def _require_memory_id(memory_id: object, came_from: str) -> str:
    """A single-memory call must name a memory: a blank or non-string id is refused
    here with a sentence about the argument, before anything is sent.
    """
    if not isinstance(memory_id, str) or not memory_id.strip():
        raise ValueError(f"memory_id is required (non-blank) — {came_from}.")
    return memory_id.strip()



def _env_key() -> str:
    for name in _ENV_KEYS:
        val = os.environ.get(name)
        if val and val.strip():
            return val
    raise ValueError(f"set {_ENV_KEYS[0]} (or {_ENV_KEYS[1]}) in the environment")


def _new_tls_context() -> ssl.SSLContext:
    """The OS trust store plus certifi's roots, a TLS 1.2 floor and hostname
    verification, with no knob to turn any of it off."""
    ctx = ssl.create_default_context()
    try:
        import certifi

        ctx.load_verify_locations(cafile=certifi.where())
    except (ImportError, OSError, ssl.SSLError):
        pass
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


# One context per transport, built once and shared by every client of that kind. An HTTP
# stack may load more roots into the context it is handed, so the two never share one.
_TLS_CONTEXTS: "dict" = {}


def _tls_context() -> ssl.SSLContext:
    """The sync transport's TLS context."""
    ctx = _TLS_CONTEXTS.get("sync")
    if ctx is None:
        ctx = _TLS_CONTEXTS["sync"] = _new_tls_context()
    return ctx


def _async_tls_context() -> ssl.SSLContext:
    """The async transport's TLS context."""
    ctx = _TLS_CONTEXTS.get("async")
    if ctx is None:
        ctx = _TLS_CONTEXTS["async"] = _new_tls_context()
    return ctx


# ── A wall-clock limit on each sync attempt ─────────────────────────────────
#
# requests and urllib3 time out one socket read at a time, so a response that trickles
# in never trips them. Each attempt runs under a timer that shuts its socket when the
# attempt's time is up; the connection classes below hand the socket to that timer.

_attempt_local = threading.local()


def _conn_socket(conn: Any) -> Any:
    """The connection's socket, or the one its response took: http.client clears ``sock``
    when the response will close the connection, and the body is still read from it."""
    return getattr(conn, "sock", None) or getattr(conn, "_wos_sock", None)


def _shut_socket(conn: Any) -> None:
    sock = _conn_socket(conn)
    if sock is None:
        return
    try:
        # The plain-socket method, so a TLS socket is shut at the descriptor and a read
        # blocked on it returns at once.
        socket.socket.shutdown(sock, socket.SHUT_RDWR)
    except (OSError, TypeError, ValueError):
        pass


# Which watchdog owns a connection. urllib3 returns a connection to the pool from inside
# the read that finishes the body, before the attempt stops its watchdog, and another
# thread can take it from there. Ownership moves under this one lock, so a timer never
# touches a socket that the pool has handed on.
_watch_lock = threading.Lock()


def _set_owner(conn: Any, owner: Optional["_Watchdog"]) -> None:
    try:
        conn._wos_watchdog = owner
    except AttributeError:
        pass


class _Watchdog:
    """Shuts the attempt's connection when the attempt's time is up."""

    def __init__(self, end: float):
        self._conn: Any = None
        self._done = False
        self.fired = False
        self._timer = threading.Timer(max(0.0, end - time.monotonic()), self._fire)
        self._timer.daemon = True
        self._timer.start()

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
        """Lower the socket's read timeout to the time left."""
        with _watch_lock:
            sock = _conn_socket(self._owned())
            if sock is not None:
                try:
                    sock.settimeout(max(0.001, left))
                except (OSError, ValueError):
                    pass

    def sooner(self, end: float) -> None:
        """Shut the connection at ``end`` instead, when that comes first."""
        with _watch_lock:
            if self._done or self.fired:
                return
            self._timer.cancel()
            self._timer = threading.Timer(max(0.0, end - time.monotonic()), self._fire)
            self._timer.daemon = True
            self._timer.start()

    def _fire(self) -> None:
        with _watch_lock:
            if self._done:
                return
            self.fired = True
            conn = self._owned()
            if conn is not None:
                _shut_socket(conn)

    def stop(self) -> bool:
        """End the watch. True when the time ran out first."""
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
    """Hands the connection to the running attempt's watchdog."""

    def request(self, *args: Any, **kwargs: Any) -> Any:
        _attach_to_watchdog(self)
        # A reused socket still carries the last attempt's lowered timeout. urllib3 2
        # resets it here; urllib3 1.x does not.
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
    """A connection going back to the pool leaves its watchdog first."""

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


class _TLSAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        kwargs["ssl_context"] = _tls_context()
        super().init_poolmanager(*args, **kwargs)
        self.poolmanager.pool_classes_by_scheme = _WATCHED_POOLS

    def proxy_manager_for(self, proxy: str, **proxy_kwargs: Any) -> Any:
        manager = super().proxy_manager_for(proxy, **proxy_kwargs)
        if not proxy.lower().startswith("socks"):
            manager.pool_classes_by_scheme = _WATCHED_POOLS
        return manager


# A forked child inherits the parent's open connections. Two processes reading one
# socket get each other's responses, so the child drops every pooled connection.
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
    """Surrounding whitespace is trimmed. A base URL with whitespace inside it or a
    backslash is refused: parsers disagree on what it names, so the host the key goes to
    could differ from the one checked. So is one that does not parse as http(s) with a
    host."""
    if isinstance(base_url, str):
        base_url = base_url.strip()
    if not isinstance(base_url, str) or not base_url:
        raise ValueError("base_url must be a non-empty string")
    if "\\" in base_url or any(c.isspace() for c in base_url) or _HEADER_CTL_RE.search(base_url):
        raise ValueError(
            f"base_url {_mask_userinfo(base_url)!r} contains whitespace, a backslash or a "
            "control character. Check for a stray space, newline or paste error."
        )
    base = base_url.rstrip("/")
    _require_url(base, _sync_scheme_host)
    return base


def _mask_userinfo(url: str) -> str:
    """``url`` with any ``user:password@`` before the host replaced by ``***@``."""
    sep = url.find("://")
    start = sep + 3 if sep != -1 else 0
    slash = url.find("/", start)
    at = url.rfind("@", start, len(url) if slash == -1 else slash)
    return url if at == -1 else url[:start] + "***@" + url[at + 1:]


def _require_url(base: str, read_url: Any) -> None:
    """Refuse a base URL the transport cannot read as http(s) with a host. The URL itself
    stays out of the message: it can carry credentials."""
    scheme, host = read_url(base)
    if scheme not in ("http", "https") or not host:
        raise ValueError(
            "base_url is not a URL: it needs an https:// (or http://) scheme and a host, "
            "e.g. https://api.wontopos.com"
        )


def _sync_scheme_host(base: str) -> tuple:
    """Scheme and host as the sync transport (urllib3) reads them."""
    from urllib3.util import parse_url

    try:
        p = parse_url(base)
    except Exception:
        return "", ""
    return (p.scheme or "").lower(), (p.host or "").lower().strip("[]")


def _async_scheme_host(base: str) -> tuple:
    """Scheme and host as the async transport (httpx) reads them."""
    import httpx

    try:
        u = httpx.URL(base)
    except Exception:
        return "", ""
    return u.scheme.lower(), u.host.lower()


def _warn_if_plain_http(base: str, read_url: Any = _sync_scheme_host) -> None:
    # An API key on plain HTTP travels readable by anyone on the path. Loopback
    # is fine (local dev, or a proxy on the same box); anything else gets a
    # warning, not an error, so private-network gateways keep working. The URL is
    # read by the transport's own parser, so the warning and the connection agree
    # on the host.
    scheme, host = read_url(base)
    if scheme == "http" and host not in _LOOPBACK_HOSTS:
        warnings.warn(
            "wontopos: base_url uses plain HTTP on a non-local host, so the API key "
            "travels unencrypted. Use https://.",
            stacklevel=3,
        )


def _never_sent(exc: requests.exceptions.ConnectionError) -> bool:
    """True when the failure happened while ESTABLISHING the connection (DNS,
    refused, connect timeout) — the request never reached the server, so a retry
    can't double-process a write. A mid-stream drop ("Connection aborted") is
    ambiguous: the server may already have processed the request."""
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return True
    try:
        from urllib3.exceptions import ConnectTimeoutError, MaxRetryError, NewConnectionError
    except ImportError:  # pragma: no cover — urllib3 always ships with requests
        return False
    connect_errs: tuple = (NewConnectionError, ConnectTimeoutError)
    try:
        from urllib3.exceptions import NameResolutionError
        connect_errs = (NewConnectionError, ConnectTimeoutError, NameResolutionError)
    except ImportError:  # urllib3 < 2 reports DNS failures as NewConnectionError
        pass
    reason = exc.args[0] if exc.args else None
    if isinstance(reason, MaxRetryError):
        reason = reason.reason
    return isinstance(reason, connect_errs)


def _timed_out(exc: BaseException) -> bool:
    """True when the body stopped arriving because time ran out, not because the
    connection broke.

    The two are not the same for a retry. A drop means nothing more is coming, so
    replaying an idempotent read costs one extra request. A timeout means the server
    may still be working on it — replaying adds load to a request that could yet be
    answered, and the reply the caller finally gets is the second one.

    ``stream=True`` sends a read timeout through ``iter_content``, where requests
    reports it as ConnectionError carrying urllib3's ReadTimeoutError in ``args[0]``
    rather than as its own ReadTimeout — so checking the requests class alone misses it.
    """
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


# The longest a retry waits. A Retry-After beyond it ends the call with that error.
_MAX_WAIT = 30.0
_DELTA_SECONDS_RE = re.compile(r"[0-9]+")
# The three HTTP-date forms of RFC 9110: IMF-fixdate, RFC 850 and asctime.
_HTTP_DATE_RE = re.compile(
    r"[A-Z][a-z]{2}, [0-9]{2} [A-Z][a-z]{2} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT"
    r"|[A-Z][a-z]{5,8}, [0-9]{2}-[A-Z][a-z]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT"
    r"|[A-Z][a-z]{2} [A-Z][a-z]{2} [ 0-9][0-9] [0-9]{2}:[0-9]{2}:[0-9]{2} [0-9]{4}"
)


def _parse_retry_after(value: Any) -> Optional[float]:
    """The wait a ``Retry-After`` header asks for, in seconds. None when it is missing or
    is neither delta-seconds nor an HTTP-date."""
    if not isinstance(value, str):
        return None
    v = value.strip()
    if _DELTA_SECONDS_RE.fullmatch(v):
        # Past 10 digits the value is over the cap anyway, and int() refuses very long ones.
        digits = v.lstrip("0") or "0"
        return float(2**31) if len(digits) > 10 else float(min(int(digits), 2**31))
    if not _HTTP_DATE_RE.fullmatch(v):
        return None
    try:
        dt = parsedate_to_datetime(v)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())


def _backoff(attempt: int, retry_after: Optional[str] = None) -> float:
    """Seconds to sleep before retry ``attempt`` (0-based): the ``Retry-After`` the server
    sent, capped at 30s, or exponential backoff with jitter when it sent none or one that
    does not parse."""
    ra = _parse_retry_after(retry_after)
    if ra is not None:
        return min(_MAX_WAIT, ra)
    return min(8.0, 0.5 * (2**attempt)) + random.random() * 0.25


def _lock_wait(err: "WosError") -> Optional[float]:
    """Seconds a 409 asks to wait when another write to the store was in flight. None for
    the permanent kind, a store id that collides with an existing one."""
    d = err.details if isinstance(err.details, dict) else {}
    ms = d.get("retry_after_ms")
    if "conflicts_with" in d or isinstance(ms, bool) or not isinstance(ms, (int, float)):
        return None
    if not math.isfinite(ms) or ms < 0:
        return None
    return ms / 1000.0


def _retry_wait(status: int, method: str, retry_after: Optional[str], err: Optional["WosError"],
                attempt: int) -> Optional[float]:
    """Seconds to wait before retrying this response, or None when it is final.

    A 429, and a 409 for a write already in flight, are answered before anything is
    stored, so every method retries them. 408/502/503/504 retry only an idempotent method:
    a write may already have been applied. A wait over 30s is final.
    """
    if status == 409:
        wait = _lock_wait(err)
        if wait is None or wait > _MAX_WAIT:
            return None
        return max(wait, _backoff(attempt))
    if status in _RETRY_ALWAYS or (
        status in _RETRY_IF_IDEMPOTENT and method.upper() in _IDEMPOTENT_METHODS
    ):
        ra = _parse_retry_after(retry_after)
        if ra is None:
            return _backoff(attempt)
        return ra if ra <= _MAX_WAIT else None
    return None


_MAX_ERR_MSG = 4096  # a hostile server's error body shouldn't become a giant exception/log line


def _strip_ctl(text: str) -> str:
    """Server text without C0 control characters or DEL, so it cannot forge log lines or
    send terminal escapes through an error message."""
    return _HEADER_CTL_RE.sub("", text)


def _parse_ok(status: int, text: str) -> dict:
    """Parse a 2xx body and enforce it's a JSON OBJECT. Every WOS endpoint
    returns an object, and every method reads it with ``.get(...)`` — so a body
    that parses to null / a number / a string / an array (a broken or hostile
    server) must become a clean ``WosError``, never a raw ``AttributeError`` when
    a method calls ``.get`` on it.

    An empty body is accepted only when the STATUS says there is no body (204/205/304),
    where it reads as ``{}``. An empty body on any other 2xx is an error: it means a
    response went missing on the way, and passing it off as success hides that."""
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
    # Server may return either:
    #   Anthropic-style envelope: {"type":"error","error":{"type":...,"message":...,"request_id":...}}
    #   Spec simple:             {"error":"reason string"}
    # Fall back to the raw text. Parse first, then cap the extracted strings: capping
    # the raw body can cut the JSON and lose `request_id`.
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
            etype = _strip_ctl(t) if isinstance(t, str) else None
            m = err.get("message")
            # A message that is not a string (an object, a number) says less than the type.
            msg = m if isinstance(m, str) and m else etype
            rid = err.get("request_id")
            request_id = _strip_ctl(rid) if isinstance(rid, str) else None
            details = _clean_details(
                {k: v for k, v in err.items() if k not in ("message", "type", "request_id")}
            )
        elif isinstance(err, str):
            msg = err
        elif isinstance(data.get("message"), str):
            msg = data["message"]
    if not isinstance(msg, str) or not msg:
        msg = text
    msg = _clean_text(msg)
    if not msg.strip():
        msg = f"HTTP {status}"
    if unread:
        msg += f" (the error body could not be read: {_clean_text(unread)[:512]})"
    return _make_error(status, msg, request_id=request_id, type=etype, details=details)


def _clean_text(text: str) -> str:
    """Server text without control characters, capped."""
    cut = len(text) > _MAX_ERR_MSG * 2
    text = _strip_ctl(text[: _MAX_ERR_MSG * 2])
    if cut or len(text) > _MAX_ERR_MSG:
        text = text[:_MAX_ERR_MSG] + "…(truncated)"
    return text


_MAX_ERR_DETAILS = 8192
def _clean_details(d: dict) -> Optional[dict]:
    """The rest of a server's error object as ``WosError.details``, with control characters
    removed from every key and string and each string capped. When its JSON is still over
    8192 characters, the biggest fields are dropped until it fits, so a short field such as
    ``conflicts_with`` survives a long one beside it."""
    if not d:
        return None

    def clean(v: Any) -> Any:
        if isinstance(v, str):
            return _clean_text(v)
        if isinstance(v, list):
            return [clean(x) for x in v]
        if isinstance(v, dict):
            return {_clean_text(k): clean(x) for k, x in v.items()}
        return v

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


def _parse_rate_limit(headers: Any) -> Optional[dict]:
    """Pull the standard ``X-RateLimit-*`` headers the API sends on every response
    into ``{limit, remaining, reset}`` (ints; ``reset`` is a unix-ish epoch second).
    Returns ``None`` when the headers aren't present (e.g. a network error)."""
    def _int(name: str) -> Optional[int]:
        v = headers.get(name)
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None
    limit, remaining, reset = _int("X-RateLimit-Limit"), _int("X-RateLimit-Remaining"), _int("X-RateLimit-Reset")
    if limit is None and remaining is None and reset is None:
        return None
    return {"limit": limit, "remaining": remaining, "reset": reset}


class WosError(RuntimeError):
    """Raised when the Wontopos API returns a non-2xx response (or the network fails).

    ``status`` is the HTTP status (0 for network failures), ``message`` the parsed
    server message, ``request_id`` the server's id for the request when it sent
    one — include it when contacting support. ``type`` is the error type the service
    named (``"invalid_request_error"``, ``"rate_limit_error"``, ...), and ``details`` the
    other fields it sent with the error; each is ``None`` when absent. Control characters
    are removed from server text, each string in ``details`` is capped, and when its JSON
    is over 8192 characters the biggest fields are left out.

    Errors pickle, so they cross process boundaries (``ProcessPoolExecutor``,
    ``multiprocessing``) intact.
    """

    def __init__(self, status: int, message: str, request_id: Optional[str] = None, *,
                 type: Optional[str] = None, details: Optional[dict] = None):
        self.status = status
        self.message = message
        self.request_id = request_id
        self.type = type
        self.details = details
        suffix = f" (request_id: {request_id})" if request_id else ""
        super().__init__(f"[{status}] {message}{suffix}")

    def __reduce__(self) -> Any:
        return (self.__class__, (self.status, self.message, self.request_id), dict(self.__dict__))


# Typed error subclasses so callers can branch on the failure instead of reading
# ``.status`` by hand — ``except RateLimitError`` / ``except AuthenticationError``.
# Every one is a WosError, so ``except WosError`` still catches them all.
class APIConnectionError(WosError):
    """The request never got a response (DNS/TLS/timeout/connection). ``status`` is 0.

    The message never carries the URL or its query string."""


class BadRequestError(WosError):
    """400, 413 or 422 — the request was malformed, too large, or not accepted as sent."""


class AuthenticationError(WosError):
    """401 — the API key is missing, wrong, or revoked."""


class PaymentRequiredError(WosError):
    """402 — no card on file or the balance is depleted. Top up to continue."""


class PermissionDeniedError(WosError):
    """403 — the key/model isn't allowed to do this."""


class NotFoundError(WosError):
    """404 — the store or resource doesn't exist.

    A DELETE retried after an answer that may have followed the delete (408/502/503/504,
    or a dropped connection) ends its message with "(an earlier attempt may already have
    deleted it)"."""


class ConflictError(WosError):
    """409: another write to this store was in flight (retried automatically; nothing was
    stored), or the store id collides with an existing one (not retried).

    ``conflicts_with`` names the existing store in the second case, and is ``None``
    otherwise."""

    @property
    def conflicts_with(self) -> Optional[str]:
        v = self.details.get("conflicts_with") if isinstance(self.details, dict) else None
        return v if isinstance(v, str) else None


class GoneError(WosError):
    """410 — the model this call named is retired. Retrying cannot succeed: name a live
    model instead (``list_models()`` lists them). ``delete_store`` still works under a
    retired model."""


class RateLimitError(WosError):
    """429 — too many requests. The client already retries these, waiting as long as
    ``Retry-After`` asks, up to 30s.

    ``retry_after`` is that wait in seconds, or ``None`` when the service sent none. When
    it is over 30s the error is raised at once rather than waited out."""

    retry_after: Optional[float] = None


class ServerError(WosError):
    """5xx — the service failed.

    ``502`` / ``503`` / ``504`` are transient: this client already retries them where a
    retry cannot apply a write twice. ``501`` is NOT: the model you selected does not
    serve that endpoint, so retrying can never succeed, and ``details`` carries ``model``
    and ``endpoint``. Pick a model that lists the capability in ``list_models()`` instead.
    """


_STATUS_ERRORS = {
    400: BadRequestError,
    401: AuthenticationError,
    402: PaymentRequiredError,
    403: PermissionDeniedError,
    404: NotFoundError,
    409: ConflictError,
    410: GoneError,
    413: BadRequestError,
    422: BadRequestError,
    429: RateLimitError,
}


_GONE_HINT = "This model is retired; list_models() lists the ones you can use."


def _make_error(status: int, message: str, request_id: Optional[str] = None, *,
                type: Optional[str] = None, details: Optional[dict] = None) -> WosError:
    if status == 410:
        message = f"{message} {_GONE_HINT}"
    if status == 0:
        cls: Any = APIConnectionError
    elif status in _STATUS_ERRORS:
        cls = _STATUS_ERRORS[status]
    elif 500 <= status < 600:
        cls = ServerError
    else:
        cls = WosError
    return cls(status, message, request_id=request_id, type=type, details=details)


# ── Time budgets and the retry rules, shared by both clients ────────────────

# A timer waits at most 2**31 - 1 milliseconds on some platforms; longer values clamp.
_MAX_SECONDS = 2147483


def _check_seconds(value: Any, default: Optional[float]) -> Optional[float]:
    """A timeout or deadline in seconds. Anything that is not a positive number means
    the default; a value past what a timer can wait, infinity included, is clamped."""
    if isinstance(value, bool) or not isinstance(value, numbers.Real) or math.isnan(value) or value <= 0:
        return default
    return value if value <= _MAX_SECONDS else _MAX_SECONDS


def _deadline_at(deadline: Optional[float]) -> Optional[float]:
    """When this call's budget runs out, or ``None`` when it has none.

    Computed once per call, never per attempt — a budget recomputed each attempt is
    not a budget, it is the per-attempt timeout wearing a different name.
    """
    return None if deadline is None else time.monotonic() + deadline


class _Clock:
    """One attempt's wall-clock end: its ``timeout``, or the call's deadline when that
    comes first. Raises when the deadline is already spent, so no socket is opened that
    there is no time to use."""

    __slots__ = ("end", "_why", "by_deadline")

    def __init__(self, timeout: float, deadline_at: Optional[float], deadline: Optional[float]):
        now = time.monotonic()
        if deadline_at is not None and deadline_at <= now:
            raise APIConnectionError(0, f"deadline of {deadline}s exhausted")
        self.end = now + timeout
        self._why = f"request timed out after {timeout}s"
        # True when the deadline, not the timeout, ends this attempt.
        self.by_deadline = False
        if deadline_at is not None and deadline_at < self.end:
            self.end = deadline_at
            self._why = f"deadline of {deadline}s exhausted"
            self.by_deadline = True

    def left(self) -> float:
        return self.end - time.monotonic()

    def within(self, seconds: float) -> "_Clock":
        """This clock, ending no later than ``seconds`` from now."""
        c = _copy.copy(self)
        c.end = min(self.end, time.monotonic() + seconds)
        return c

    def error(self) -> "APIConnectionError":
        return APIConnectionError(0, self._why)


class _Expired(Exception):
    """Internal: the attempt's time ran out between two reads of the body."""


class _Failure:
    """A transport failure, reduced to what the retry decision and the message need.

    The exception itself is not kept. It carries the request, the API key among its
    headers, and an error chained to it would carry them too.
    """

    __slots__ = ("reason", "never_sent", "timed_out", "dropped")

    def __init__(self, reason: str, *, never_sent: bool = False, timed_out: bool = False,
                 dropped: bool = False):
        self.reason = reason
        self.never_sent = never_sent  # failed while connecting: the request never left
        self.timed_out = timed_out    # the attempt's time ran out
        self.dropped = dropped        # the connection broke after the request left


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
    """The exception type and the OS or TLS cause behind it. Never the URL: the transport's
    own text carries the host and the query string."""
    chain = _exception_chain(e)
    kind = type(e).__name__
    for x in chain:
        if isinstance(x, ssl.SSLCertVerificationError):
            why = _strip_ctl(str(x.verify_message or "verification failed"))
            return f"{kind} (TLS certificate: {why})"
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
    """The retry rules for one call, shared by the sync and async clients. ``after_*``
    returns how long to wait before the next attempt, or raises the error that ends the
    call."""

    def __init__(self, method: str, path: str, retries: int, deadline: Optional[float], *,
                 removes: Optional[bool] = None, reads: bool = False):
        self.method = method.upper()
        # The query string stays out of logs and errors: it can carry a store id.
        self.where = f"{method} {path.split('?', 1)[0]}"
        self.attempts = retries + 1
        self.attempt = 0
        self.deadline = deadline
        self.deadline_at = _deadline_at(deadline)
        # Whether the call deletes something (a DELETE, unless it only previews).
        self.removes = self.method == "DELETE" if removes is None else removes
        # Whether the answer before an attempt cut short still stands: an idempotent
        # method, or a POST that only reads.
        self.reads = (reads or self.method in _IDEMPOTENT_METHODS
                      or (self.method == "POST" and path.split("?", 1)[0] in _READ_POSTS))
        # Set once a retry follows an answer that may have come after the request was
        # applied: 408/502/503/504, or a connection that broke after sending.
        self.maybe_applied = False
        # The answer that led to the attempt under way, reported when the deadline runs
        # out before that attempt gets its own.
        self.previous: Optional[WosError] = None
        # Why the error answer being handled came without its body, when it did.
        self.unread: Optional[str] = None

    def clock(self, timeout: float) -> _Clock:
        if (self.previous is not None and self.deadline_at is not None
                and self.deadline_at <= time.monotonic()):
            raise self._final(self.previous)
        return _Clock(timeout, self.deadline_at, self.deadline)

    def _fits(self, wait: float) -> bool:
        return self.deadline_at is None or wait <= self.deadline_at - time.monotonic()

    def _more(self) -> bool:
        return self.attempt + 1 < self.attempts

    def _spent(self) -> bool:
        # A timer set for the deadline can fire a hair before it.
        return self.deadline_at is not None and self.deadline_at - time.monotonic() <= 0.001

    def retries_regardless(self, status: int, headers: Any) -> bool:
        """Whether another attempt follows ``status`` whatever its body says. A 409 is
        decided by its body, so never."""
        if status == 409 or not self._more():
            return False
        wait = _retry_wait(status, self.method, headers.get("Retry-After"), None, self.attempt)
        return wait is not None and self._fits(wait)

    def after_failure(self, f: _Failure, clock: _Clock) -> float:
        previous, self.previous = self.previous, None
        if f.timed_out:
            # The deadline cut this attempt short: report the answer before it, unless a
            # write was in flight and may have been applied.
            if clock.by_deadline and previous is not None and self.reads:
                raise self._final(previous)
            raise clock.error()
        # Retry only when it cannot apply a write twice: an idempotent method, or a
        # failure while connecting, before the request left.
        retryable = f.never_sent or (f.dropped and self.method in _IDEMPOTENT_METHODS)
        wait = _backoff(self.attempt)
        if retryable and (self._more() or self._spent()) and not self._fits(wait):
            # The deadline, not the retry count, ends the call, and this attempt got no
            # answer of its own: the one before it stands.
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
            # The API never redirects, and following one would send the key along.
            raise WosError(
                status,
                "unexpected redirect — refused (the API key never follows a redirect). "
                "Check base_url: exact host, https://.",
            )
        err = _parse_error(status, payload.decode("utf-8", "replace"), self.unread)
        self.unread = None
        retry_after = headers.get("Retry-After")
        if isinstance(err, RateLimitError):
            err.retry_after = _parse_retry_after(retry_after)
        wait = _retry_wait(status, self.method, retry_after, err, self.attempt)
        # A wait that does not fit the deadline ends the call with this response's error.
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
    """The request body as JSON bytes, built the same way for both clients. A value JSON
    cannot carry is refused before sending: ``ValueError`` for NaN or Infinity,
    ``TypeError`` for an object that is not JSON."""
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
        # A lone surrogate cannot be UTF-8; escaped, it is still valid JSON.
        return json.dumps(body, allow_nan=False, separators=(",", ":"), default=_json_default).encode("ascii")


def _off_the_wire(r: Any) -> bool:
    """True once the whole response has been read from the socket."""
    raw = getattr(r, "raw", None)
    if raw is None or not hasattr(raw, "_fp"):
        return False
    if raw._fp is None:
        return True
    try:
        return bool(_u3_is_fp_closed(raw._fp))
    except Exception:
        return False


def _read_body(r: Any, clock: Optional[_Clock] = None, watchdog: Optional[_Watchdog] = None) -> bytes:
    """A sync response body, refused past the 64MB cap and, with a clock, past the
    attempt's end.

    Streams and caps incrementally: ``iter_content`` decompresses as it streams, so a
    small gzip body whose compressed size sails under the cap stops the moment its
    decoded size crosses it.
    """
    cl = r.headers.get("Content-Length", "")
    if cl.isdigit() and int(cl) > _MAX_RESPONSE_BYTES:
        raise WosError(r.status_code, f"response too large ({cl} bytes) — refusing to buffer it")
    raw = bytearray()
    chunks = r.iter_content(chunk_size=65536)
    while True:
        # Once the last byte is off the socket the rest is in memory: a body that
        # arrived in time is kept, and the socket may already serve another request.
        if clock is not None and not _off_the_wire(r):
            left = clock.left()
            if left <= 0:
                raise _Expired()
            if watchdog is not None:
                watchdog.shrink(left)
        chunk = next(chunks, None)
        if chunk is None:
            break
        if not chunk:
            continue
        # Refuse the chunk that would cross the line rather than absorbing it first:
        # with decoded (decompressed) chunks, "check after extend" means the memory
        # is already committed.
        if len(raw) + len(chunk) > _MAX_RESPONSE_BYTES:
            raise WosError(r.status_code, "response too large — refusing to buffer it")
        raw.extend(chunk)
    # A body that ends short of its Content-Length was cut off. urllib3 2 raises for this
    # itself; urllib3 1.x returns the short body.
    if cl.isdigit() and not r.headers.get("Content-Encoding") and len(raw) < int(cl):
        raise _IncompleteRead(b"", int(cl) - len(raw))
    return bytes(raw)


class _Undecodable(Exception):
    """Internal: a response body that does not decode as its Content-Encoding says."""


class _Inflater:
    """One gzip or deflate layer, decoded with each step bounded by what is left of the
    cap, so the output can never outgrow it."""

    def __init__(self, status: int, coding: str):
        self._status = status
        self._gzip = coding != "deflate"
        self._d: Any = zlib.decompressobj(31) if self._gzip else None

    def feed(self, data: bytes, out: bytearray) -> None:
        if self._d is None:
            # "deflate" is zlib-wrapped by the spec and raw in some servers; the header says which.
            zlib_wrapped = len(data) >= 2 and (data[0] & 0x0F) == 8 and ((data[0] << 8) | data[1]) % 31 == 0
            self._d = zlib.decompressobj(15 if zlib_wrapped else -15)
        d = self._d
        while data and not d.eof:
            room = _MAX_RESPONSE_BYTES - len(out)
            try:
                piece = d.decompress(data, room + 1)
            except zlib.error:
                raise _Undecodable() from None
            if len(piece) > room:
                raise WosError(self._status, "response too large — refusing to buffer it")
            out.extend(piece)
            data = d.unconsumed_tail

    def finish(self, out: bytearray) -> None:
        if self._d is None:
            return
        try:
            tail = self._d.flush()
        except zlib.error:
            raise _Undecodable() from None
        if len(out) + len(tail) > _MAX_RESPONSE_BYTES:
            raise WosError(self._status, "response too large — refusing to buffer it")
        out.extend(tail)


async def _aread_body(r: Any) -> bytes:
    """An async (httpx) response body, refused past the 64MB cap.

    Reads the raw bytes and decodes at most one gzip or deflate layer itself, each step
    bounded by what is left of the cap. A stacked or unknown encoding is refused.
    """
    status = r.status_code
    codings = [c.strip().lower() for c in r.headers.get("Content-Encoding", "").split(",")]
    codings = [c for c in codings if c and c != "identity"]
    if len(codings) > 1:
        raise WosError(status, f"response has stacked content encodings ({', '.join(codings)!r}); refusing to decode it")
    coding = codings[0] if codings else ""
    if coding not in ("", "gzip", "x-gzip", "deflate"):
        raise WosError(status, f"unsupported response content encoding {coding!r}")
    cl = r.headers.get("Content-Length", "")
    if cl.isdigit() and int(cl) > _MAX_RESPONSE_BYTES:
        raise WosError(status, f"response too large ({cl} bytes) — refusing to buffer it")
    inflater = _Inflater(status, coding) if coding else None
    out = bytearray()
    received = 0
    async for chunk in r.aiter_raw(65536):
        if not chunk:
            continue
        received += len(chunk)
        if received > _MAX_RESPONSE_BYTES:
            raise WosError(status, "response too large — refusing to buffer it")
        if inflater is None:
            out.extend(chunk)
        else:
            inflater.feed(chunk, out)
    if inflater is not None:
        inflater.finish(out)
    return bytes(out)


class Client:
    """Wontopos memory client.

    Store and recall memories, isolated per ``user_id``.

        mem = Client(api_key="wos-...")
        mem.add("she prefers tea over coffee", user_id="alice")
        hits = mem.search("what does alice drink?", user_id="alice")

    The API key picks *which memory* (your account). ``model`` picks *which engine*
    reads it. Models on the shared pool read the same memory, so you can store
    with one and recall with another. Set a default on the client, override per call:

        mem = Client(api_key="wos-...")                        # tablet-2
        mem.recall("...", user_id="alice")                     # tablet-2
        mem.recall("...", user_id="alice", model="tablet-1")   # this call only

    A client pickles, so it can be handed to a process pool that starts its workers with
    spawn. The pickle carries the API key: keep it wherever you would keep the key.
    ``copy.copy()`` and ``copy.deepcopy()`` work and share the connection pool, like
    ``with_user()``.

    Args:
        api_key:  your Wontopos API key (sent as ``X-API-Key``).
        base_url: API base URL (defaults to the hosted service). Whitespace at either
                  end is trimmed; whitespace inside it, or a backslash, is refused.
        timeout:  wall-clock limit for one attempt, in seconds, however slowly the
                  response arrives. Each retry gets its own.
        model:    default model for every call (sent as ``X-WOS-Model``).
                  See ``list_models()`` for what's available.
        user_id:  default store for every call. Pass ``user_id=`` on a single
                  call to override it. Defaults to the account's ``default`` store.
        retries:  how many times to retry transient failures before raising —
                  429 and a 409 for a write already in flight always; 408/502/503/504
                  and connection errors only when a retry cannot apply a write twice.
                  0 disables retries.
        deadline: a total budget for one call, in seconds, across every attempt and
                  the waits between them. ``timeout`` bounds one attempt; at the
                  defaults (30s, two retries) a call can take over a minute. When the
                  next wait does not fit, the last response's error is raised.
                  ``None`` means no overall budget.
    """

    DEFAULT_USER = "default"

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
        model: str = DEFAULT_MODEL,
        user_id: str = DEFAULT_USER,
        retries: Optional[int] = None,
        *,
        max_retries: Optional[int] = None,
        deadline: Optional[float] = None,
        _session: Optional["requests.Session"] = None,
    ):
        # ``max_retries`` is an alias for ``retries``; an explicit ``retries`` wins.
        # The default is None rather than 2 so that "the caller said 2" and "the
        # caller said nothing" are different states.
        if retries is None:
            retries = 2 if max_retries is None else max_retries
        self._api_key = _clean_key(api_key)
        self._base = _check_base_url(base_url)
        self._timeout = _check_seconds(timeout, 30.0)
        self._deadline = _check_seconds(deadline, None)
        self._model = _check_model(model)
        # The store every call uses unless one passes user_id=. "default" is the
        # account's built-in store, so the zero-config path needs no create call.
        # The same guard _uid applies per call, so the constructor and with_user() —
        # the documented per-tenant pattern — cannot land in the default store either.
        _assert_usable_store_id(user_id)
        self._user_id = user_id
        self._retries = max(0, int(retries))
        self._rate_limit: Optional[dict] = None
        # A clone (with_model/with_user/with_timeout/with_retries) SHARES the
        # parent's session so the connection pool + TLS context are reused —
        # otherwise `mem.with_model(...).recall(...)` in a loop opens a fresh TLS
        # connection every call. The model is NOT baked into the shared session's
        # headers (clones differ on it); it's set per-request from self._model.
        # Only the owner closes the session (see close()).
        self._owns_session = _session is None
        if _session is not None:
            self._session = _session
        else:
            self._session = requests.Session()
            adapter = _TLSAdapter()
            self._session.mount("https://", adapter)
            self._session.mount("http://", adapter)
            self._session.headers.update({
                "X-API-Key": self._api_key,
                "Content-Type": "application/json",
                "User-Agent": _USER_AGENT,
            })
        _sessions.add(self._session)
        _warn_if_plain_http(self._base)

    @classmethod
    def from_env(cls, **kwargs: Any) -> "Client":
        """A client whose key comes from ``WONTOPOS_API_KEY`` (or ``WOS_API_KEY``) —
        keeps keys out of source code. Other constructor args pass through:

            mem = Client.from_env(user_id="alice")
        """
        return cls(_env_key(), **kwargs)

    def __repr__(self) -> str:  # never the key or a URL password: repr ends up in logs
        return (
            f"Client(base_url={_mask_userinfo(self._base)!r}, model={self._model!r}, "
            f"user_id={self._user_id!r}, api_key='{_mask_key(self._api_key)}')"
        )

    def _clone(self, **overrides: Any) -> "Client":
        """Everything else kept.

        One list, four callers. Four hand-written copies of "everything else" is how
        an option gets dropped by one clone and not the others: the failure reads as
        "with_model() ignores my timeout" and nothing points at the constructor call
        that forgot it. The transport is carried too — a clone that rebuilt it would
        drop a caller's session pooling.
        """
        kw: dict = dict(
            api_key=self._api_key, base_url=self._base, timeout=self._timeout,
            model=self._model, user_id=self._user_id, retries=self._retries,
            deadline=self._deadline, _session=self._session,
        )
        kw.update(overrides)
        return Client(**kw)

    def with_deadline(self, deadline: float) -> "Client":
        """A client with a total budget per call, in seconds, across every retry.

            mem.with_deadline(5.0).search(q, user_id="alice")   # a handler with 5s

        ``timeout`` bounds ONE attempt. At the defaults — 30s, two retries — a single
        call can take over a minute.
        """
        return self._clone(deadline=deadline)

    def with_user(self, user_id: str) -> "Client":
        """A client bound to ``user_id`` as its default store (everything else kept).
        Shares this client's connection pool.

            mem.with_user("alice").add("she prefers tea")   # stores under "alice"
        """
        return self._clone(user_id=user_id)

    def with_model(self, model: str) -> "Client":
        """A client that uses ``model`` for every call (everything else kept).
        Shares this client's connection pool, so a per-call override
        (``mem.with_model("scroll-1.2").recall(...)``) reuses the open connection.
        """
        return self._clone(model=model)

    def with_timeout(self, timeout: float) -> "Client":
        """A client with a different per-attempt timeout in seconds (everything else kept).
        Shares this client's connection pool.

            mem.with_timeout(120).add_bulk(big_blob)   # this slow call only
        """
        return self._clone(timeout=timeout)

    def with_retries(self, retries: int) -> "Client":
        """A client with a different retry budget (everything else kept). 0 disables retries.
        Shares this client's connection pool."""
        return self._clone(retries=retries)

    def close(self) -> None:
        """Release the HTTP connection pool. ``with Client(...) as mem:`` does this for you.
        A clone (``with_model`` etc.) shares the parent's pool and does NOT close it —
        only the client that created the session does."""
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @property
    def rate_limit(self) -> Optional[dict]:
        """Quota from the MOST RECENT call: ``{"limit", "remaining", "reset"}`` (or
        ``None`` before the first call). Read it to self-throttle — the client already
        retries 429s for you, but this lets you slow down before hitting the wall.

            mem.search("...")
            rl = mem.rate_limit
            if rl and rl["remaining"] is not None and rl["remaining"] < 5:
                time.sleep(1)
        """
        return self._rate_limit

    def _uid(self, user_id: Optional[str]) -> str:
        """Resolve a call's store: the explicit user_id, else the client default."""
        # An OMITTED id means "use the client default" — the documented shortcut. An id
        # that was PASSED but is blank is a different thing: the caller computed a tenant
        # id and got nothing, and falling back writes that customer's memories into
        # whatever store this client defaults to, silently.
        if user_id is not None:
            _assert_usable_store_id(user_id)
        sid = user_id if user_id else self._user_id
        _warn_if_store_id_collapses(sid)
        return sid

    # ----- write -----

    def add(self, content: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
            idempotency_key: Optional[str] = None, metadata: Optional[dict] = None,
            image: Optional[dict] = None,
            **extra: Any) -> dict:
        """Store one memory. Extra keyword args become metadata.

            mem.add("moved to Berlin", user_id="alice", event_date="2026-03-01")      # when it happened
            mem.add("I promised the report by Friday", user_id="alice", speaker="me")   # the assistant's own words
            mem.add("Bob said the deadline moved", user_id="alice", speaker="Bob")      # a person (up to 50 per store)

        ``user_id`` is optional — omit it to use the client's default store.

        The service keeps four metadata keys: ``speaker``, ``event_date`` (RFC3339 or a
        plain date, ``YYYY-MM-DD``; a value that is not a date is refused with 400 naming
        the field), ``category`` and ``conversation_id``. It drops any other key, and this
        client logs a warning the first time it sees one.

        Do not splat input you did not write (``**request_json``): ``user_id`` and
        ``model`` bind to the named parameters, so the dict would choose them. Forward
        it as ``metadata=``. A key that names a store (``userId``, ``store_id``, ...) or
        the idempotency key is refused rather than stored as metadata.

        ``idempotency_key`` makes repeating THIS EXACT write safe once the first attempt
        has finished: the API replays the first response instead of storing again, and
        answers 422 if the same key arrives with a different body. A repeat that overlaps
        a first attempt still running can run twice. The window is best-effort and can be
        shorter than 10 minutes; it is not a durable de-duplication record. Use it when the
        retry is yours — a job that died and was re-run, a queue that redelivers. The SDK
        retries a write only on a 429 or a 409 for a write already in flight, both answered
        before anything is stored. It never retries a write on 408 / 502 / 503 / 504 or a
        dropped body, where the first attempt may already have been applied — without a
        key the client cannot know whether it was.

        Derive the key from the thing being stored (``f"import:{row.id}"``), never a
        constant: reusing one key for two different writes replays the first and the second
        is silently lost. It is a keyword here so it is sent as a header, never stored as
        a metadata field.

        ``metadata=`` also works, merged with the keyword form::

            mem.add("...", "alice", metadata={"speaker": "me"})   # same as speaker="me"

        A loose keyword wins on a conflict, because it is the more specific thing the
        caller just typed. A keyword of ``None`` counts as not given: it is not sent and
        does not override ``metadata=``, which is sent as given.

        ``image=`` attaches an image, on a model that lists the ``images`` capability in
        ``list_models()``::

            mem.add("at the beach", image={"data": b64})

        The caption is required: empty content is refused (400), image or not.
        ``{"data": <base64>}`` is the only required part of the image; a
        ``data:image/...;base64,`` prefix is accepted and stripped. Optional:
        ``taken_at`` (RFC3339 or a plain date, usually from EXIF; it fills ``event_date``
        when that is empty, so the memory sorts by when the image was TAKEN rather than
        when it was uploaded) and ``reference`` (where your own copy lives, stored as a
        string and never fetched). When ``reference`` is sent the service keeps no image
        bytes: ``get_image`` answers 404, and you fetch the picture from your reference.

        Both edges must be 700px or more; a smaller image is refused (400).

        What we keep is NOT your original. Over 1568px on the long edge the picture is
        downscaled to 1568 on the way in, and downscaling means re-encoding: lossless
        formats are written as WebP, so a PNG comes back from ``get_image`` as
        ``image/webp``; JPEG stays JPEG. Under 1568px the bytes are untouched.

        Returns ``{"id", "status"}``. When something was saved, ``status`` starts with
        ``"stored"`` and can carry more text, so match on the prefix. It is
        ``"duplicate"`` when nothing was saved. A duplicate can arrive with an empty ``id``
        and no ``duplicate_of``; then search with the same text to find the memory it
        matched.
        """
        sid = self._uid(user_id)
        md = _metadata(metadata, extra)
        body: dict = {"user_id": sid, "content": content, "metadata": md}
        if image is not None:
            body["image"] = _normalize_image(image)
        return self._post(
            "/api/v1/memory/store",
            body,
            model=model,
            idempotency_key=idempotency_key,
        )

    store = add  # alias (back-compat / preference)


    def add_turn(
        self, user_msg: str = "", assistant_msg: str = "", user_id: Optional[str] = None, *,
        model: Optional[str] = None, idempotency_key: Optional[str] = None,
    ) -> dict:
        """Store a conversation turn (user + assistant) into short-term + long-term memory.

        Payload first, ``user_id`` second — same shape as ``store``/``search``/``recall``.
        ``user_id`` is optional; omit it to use the client's default store
        (``add_turn("hi", "yo")`` or ``add_turn(user_msg="hi", assistant_msg="yo")``).
        """
        return self._post(
            "/api/v1/memory/store-turn",
            {"user_id": self._uid(user_id), "user_msg": user_msg, "assistant_msg": assistant_msg},
            model=model,
            idempotency_key=idempotency_key,
        )

    def add_bulk(
        self, content: str, user_id: Optional[str] = None, category: Optional[str] = "general",
        timestamp: Optional[str] = None, *, model: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Bulk-ingest a large blob of text in one call.

        Use this to backfill long histories in one call. ``timestamp`` must be RFC3339
        (``"2026-05-02T09:00:00Z"``); a plain date or any other string is ignored and the
        memory is filed at upload time. ``user_id`` is optional — omit it to use the
        client's default store.
        """
        body: dict[str, Any] = {"user_id": self._uid(user_id), "content": content,
                                "category": "general" if category is None else category}
        if timestamp:
            body["timestamp"] = timestamp
        return self._post("/api/v1/memory/bulk-store", body, model=model,
                          idempotency_key=idempotency_key)

    def update(
        self, old_memory_id: str = "", new_content: str = "", user_id: Optional[str] = None, *,
        model: Optional[str] = None, idempotency_key: Optional[str] = None,
    ) -> dict:
        """Supersede an old memory with new content (e.g. a fact that changed).

        Payload first, ``user_id`` second — same shape as ``store``/``search``.
        ``user_id`` is optional; omit it to use the client's default store
        (``update(old_memory_id=..., new_content=...)``).
        """
        return self._post(
            "/api/v1/memory/supersede",
            {"user_id": self._uid(user_id), "old_memory_id": old_memory_id, "new_content": new_content},
            model=model,
            idempotency_key=idempotency_key,
        )

    # ----- read -----

    def search(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> list[dict]:
        """Search a store's memories. Returns them most relevant first.

        The list ARRIVES in that order — do not re-sort it. ``similarity`` on each
        memory is a raw closeness score, not the ranking key: what produces the order
        is internal and is not returned, so sorting by ``similarity`` overrides the
        ranking and makes results worse. There is no ``score`` field.

        ``user_id`` is optional — omit it to use the client's default store.

        Do not splat input you did not write (``**request_json``): ``user_id``, ``limit``
        and ``model`` bind to the named parameters, so the dict would choose them.
        Forward it as ``extra=``, where the store, query and count always win.

        ``limit`` is 5-20, and out of range is refused rather than clamped: asking for
        50 and silently receiving 20 reads as "that is all there is". The default is 10.

        ``limit`` bounds ``memories``, not the returned list. The assistant's own words
        (``self_memories``, on a model with that capability) and image memories (one
        unless ``max_images`` says otherwise) come back in it as well, so it can hold more
        than ``limit``. They are billed either way. Size a prompt window on the list you
        get back, not on ``limit``. :meth:`search_full` hands the fields back apart.

        ``filters`` chooses what is searched, not what is kept afterwards — a narrow
        filter still returns your full ``limit`` when that many matches sit inside it::

            mem.search("what did we decide", filters={
                "categories": ["work"],
                "event_from": "2026-01-01",   # WHEN IT HAPPENED (metadata.event_date),
                "event_to": "2026-06-30",     # not when it was written
            })

        Accepted keys: ``categories``, ``event_from``, ``event_to``, ``time_from``,
        ``time_to``, ``min_importance``. The four dates take RFC3339 or a plain date
        (``YYYY-MM-DD``); a plain end date (``time_to``, ``event_to``) covers that whole
        day in UTC, and a value that is not a date is refused (400) naming the field.
        Filtering behaves identically in every language. Unlisted keys are dropped by the
        API rather than rejected — a typo silently widens the search, so spell them
        exactly.

        Filters apply to ``memories``. The assistant's own words (``self_memories``) are
        not filtered, and this method returns them in the same list; use
        :meth:`search_full` to keep them apart.

        Other options, passed the same way as ``filters``:

        ``cache_control`` turns on recall caching — a repeated or extended query inside
        the TTL bills at **0.1×**::

            mem.search("what did we decide", cache_control={"ttl": "5m"})

        It is not free to switch on. The FIRST call writes the cache and bills the query
        tokens at **2×** for a ``5m`` TTL and **3×** for ``1h``; only hits inside the TTL
        get the 0.1× rate. So it pays for a query you repeat or extend and costs more for
        one you issue once — enabling it on every search raises the bill.

        Accepts ``{"ttl": "5m"}`` or ``{"ttl": "1h"}``; any other value is a 400. Any
        write to the store invalidates its cache at once, so a hit can never serve a
        result that predates a new memory. Accepted on the shared-pool models; a model
        that does not support it refuses the option rather than accepting it and
        silently doing nothing.

        ``speaker`` recalls one person's words only — ``"me"`` for the assistant's own,
        or a name registered with :meth:`add_speaker`.

        ``verify`` (0–3) asks again after the first answer, up to that many times, and
        each extra pass reaches memories the earlier ones did not. No LLM runs at any
        value. It stops early when
        a pass finds nothing new, and ``verify_used`` in the response says how many ran.
        Each pass is another engine call that can add up to ``limit`` more memories, so
        it costs more; you are billed for what is delivered. Default 0. It helps most on
        questions needing several distinct memories from far apart in the history and
        does little on a single-fact lookup.

        ``max_images`` (0–5) is how many image memories the answer may carry; omit it
        and the service uses 1, ``0`` asks for none. Out of range is refused here rather
        than clamped — silently cutting 6 to 5 would leave you believing you got six.

        Both need a model that lists the capability (``re_ask``, ``images``) in
        ``list_models()``, and are REFUSED (403) on one without it, instead of being
        accepted and quietly doing nothing.

        A misspelled option (``verfy=3``) is refused with ``ValueError`` naming the key
        you probably meant, before anything is sent: the service would drop it and answer
        normally, re-asking nothing. Pass a genuinely new option under ``extra``.
        """
        body = _search_body(self._uid(user_id), query, limit, opts, verify, max_images)
        r = self._post("/api/v1/memory/search", body, model=model)
        return _merge_results(r)

    def search_full(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> dict:
        """Search, and keep every field the answer came with.

        Same request as :meth:`search` — reach for this one when the options make the
        merged list an incomplete answer::

            r = mem.search_full("the day we moved", "alice", max_images=3, verify=2)
            r["images"]        # the photos, apart from the text memories
            r["verify_used"]   # re-ask passes that actually ran (you are billed per pass)

        ``memories``, ``self_memories`` and ``images`` are always lists. Anything else
        the service sent comes through untouched.
        """
        body = _search_body(self._uid(user_id), query, limit, opts, verify, max_images)
        r = self._post("/api/v1/memory/search", body, model=model)
        r = dict(r) if isinstance(r, dict) else {}
        for field in ("memories", "self_memories", "images"):
            r[field] = _as_records(r.get(field))
        return r

    def search_self(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> dict:
        """Search a model with the ``self_memories`` capability: both fields from ONE call.

        Returns ``{"memories": [...], "self_memories": [...]}``.
        ``memories`` is what others said and general memories; ``self_memories`` is the
        assistant's OWN words (stored with ``speaker="me"``), kept apart so whoever
        reads them never confuses who said what — never mixed into ``memories``. On a
        model without that capability ``self_memories`` is ``[]``. Image memories are not
        included; :meth:`search` and :meth:`search_full` carry them. Filters apply to
        ``memories`` only.
        ``user_id`` is optional — omit it to use the client's default store.

            r = mem.with_model("scroll-1.2").search_self("what did I promise Alice?")
            for m in r["memories"]: ...        # partner / general memories
            for m in r["self_memories"]: ...   # the assistant's own words
        """
        body = _search_body(self._uid(user_id), query, limit, opts, verify, max_images)
        r = self._post("/api/v1/memory/search", body, model=model)
        # `or []` on each field: a broken proxy sending null for either yields [], not None.
        return {
            "memories": _as_records(r.get("memories")),
            "self_memories": _as_records(r.get("self_memories")),
        }

    def recall(
        self, query: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
        form: Optional[str] = None, tz: Optional[int] = None,
        limit: Optional[int] = None, context_limit: Optional[int] = None,
    ) -> dict:
        """One-call context for an LLM: short-term turns + long-term matches + surrounding context.

        Returns ``{"short_term": ..., "long_term": ..., "context": ...}``. No extra
        round-trips. ``user_id`` is optional — omit it to use the client's default store.
        ``form`` (``"memoir"``/``"archive"``, on a model with the ``forms`` capability)
        renders each long-term memory's time in that form; ``tz`` is your UTC-offset
        hours for that rendering.

        ``limit`` (5–20, default 10) is how many long-term memories come back, and
        ``context_limit`` (0–20, default 10) how much surrounding context rides
        along with the best match; 0 attaches none. Out of range is refused rather than
        clamped — asking for 20 and silently getting 10 reads as "that is all there is".

        Both need a limit-aware model. An older one recalls a fixed ten whatever you
        send, so the API refuses the call (403) instead of answering with a number you did
        not ask for.
        """
        body = _recall_body(self._uid(user_id), query, form, tz, limit, context_limit)
        return self._post("/api/v1/memory/recall", body, model=model)

    def engram(
        self, name: str, query: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
        form: Optional[str] = None, tz: Optional[int] = None,
    ) -> dict:
        """Run a built-in engram (``"deep_recall"``, ``"timeline"``, ``"gather"``,
        ``"equilibrium"``, ``"tone_stabilizer"``; the service is the authority — an
        unknown name comes back with the list it accepts).

        Composes retrieval into a richer result than one search. Returns
        ``{"engram", "hops", "count", "memories", "usage"}``. ``user_id`` is optional —
        omit it to use the client's default store. ``form``/``tz`` render memory times
        (memoir/archive) on a model with the ``forms`` capability, same as search/recall.
        """
        body = _form_body({"name": name, "user_id": self._uid(user_id), "query": query}, form, tz)
        return self._post("/api/v1/engram/run", body, model=model)

    def history(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """Recent conversation turns (short-term memory). Omit ``user_id`` for the default store."""
        return _as_records(self._post("/api/v1/memory/history", {"user_id": self._uid(user_id)}, model=model).get("turns"))

    def stats(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> dict:
        """Memory counts for a store: ``{total_memories, short_term_turns}``. Omit ``user_id`` for the default."""
        return self._post("/api/v1/memory/stats", {"user_id": self._uid(user_id)}, model=model)

    def get(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *, model: Optional[str] = None) -> dict:
        """Fetch ONE memory by id — the text you stored, and its metadata.

        The id is the one ``add``/``store`` or ``list_memories`` returned. Same
        visibility as ``list_memories``: an id from another store, an id that
        ``list_memories`` does not return, or an invalidated memory raises
        ``NotFoundError``. Omit
        ``user_id`` (keyword ``memory_id=``) for the default store.

            m = mem.get(memory_id="9b2d8c1e-...")
            print(m["content"], m["is_superseded"])
        """
        user_id, memory_id = _split_id_args(user_id, memory_id)
        # A blank id is refused and a valid one is stripped, as in the sibling calls.
        if not isinstance(memory_id, str) or not memory_id.strip():
            raise ValueError(
                "memory_id is required — the id that add/store or list_memories returned. "
                "Note the argument order: get(user_id, memory_id). With a store set on the "
                'client, call get(memory_id="...").'
            )
        memory_id = memory_id.strip()
        r = self._post(
            "/api/v1/memory/get", {"user_id": self._uid(user_id), "memory_id": memory_id}, model=model
        )
        return _memory_from_get(r)

    def list_memories(
        self, user_id: Optional[str] = None, *, limit: Optional[int] = 100, cursor: Optional[str] = None,
        model: Optional[str] = None
    ) -> dict:
        """List a store's stored memories — the original text you saved plus its
        metadata. Paginated: pass the returned ``next_cursor`` back as ``cursor`` for the
        next page, and stop when it is ``None``. It can be non-null on the last page, in
        which case the next call returns an empty page. Pass back only a cursor the
        service returned, with the model that returned it. ``limit`` is 1 to 500 (default 100, also for ``None``); anything
        else is refused.
        Use it to browse or export a store. Omit ``user_id`` for the client's default
        store.

            page = mem.list_memories(limit=100)
            for m in page["memories"]:
                print(m["id"], m["content"])

            # export everything:
            out, cursor = [], None
            while True:
                page = mem.list_memories(cursor=cursor)
                out += page["memories"]
                cursor = page["next_cursor"]
                if not cursor:
                    break

        Returns ``{"memories": [...], "count": int, "next_cursor": str | None}``.
        """
        limit = _list_size(limit, "limit")
        body: dict = {"user_id": self._uid(user_id), "limit": limit}
        if cursor:
            body["cursor"] = cursor
        return self._post("/api/v1/memory/list", body, model=model)

    def iter_memories(self, user_id: Optional[str] = None, *, page_size: Optional[int] = 100,
                      model: Optional[str] = None):
        """Yield every stored memory in a store, paging under the hood — no cursor
        bookkeeping. The text you stored and its metadata only.

            for m in mem.iter_memories():
                print(m["id"], m["content"])
        """
        page_size = _list_size(page_size, "page_size")
        cursor: Optional[str] = None
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = self.list_memories(user_id, limit=page_size, cursor=cursor, model=model)
            rows = _as_records(page.get("memories"))
            for m in rows:
                yield m
            nxt = page.get("next_cursor")
            if not nxt or _cursor_repeats(nxt, seen, rows):
                return
            cursor = nxt
        raise _truncated(f"stopped after {_MAX_PAGES} pages — the store did not end")

    def export_memories(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """Return ALL of a store's memories as a list (the text you stored, and its
        metadata). Convenience over ``iter_memories`` for dumping a whole store."""
        return list(self.iter_memories(user_id, model=model))

    def export_images(self, user_id: Optional[str] = None, *, page_size: Optional[int] = None,
                      model: Optional[str] = None) -> list[dict]:
        """Return ALL of a store's image memories as a list. The image-side pair of
        ``export_memories``.

        ``page_size`` (5 to 20) sets how many arrive per request, not how many you get back."""
        return list(self.iter_images(user_id, page_size=page_size, model=model))

    # ----- images (models with the ``images`` capability) -----

    def get_image(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                  model: Optional[str] = None) -> tuple[bytes, str]:
        """Fetch the bytes of an image memory. Returns ``(bytes, content_type)``.

            data, mime = mem.get_image(memory_id=mid)
            ext = mime.split("/")[1]          # "webp" for a downscaled PNG
            open(f"image.{ext}", "wb").write(data)

        This is the picture the SERVICE holds, which is not necessarily your upload —
        in size or in format. An image whose long edge was over 1568px was downscaled to
        1568 on the way in and re-encoded (lossless formats as WebP, so a PNG comes back
        as ``image/webp``; JPEG stays JPEG), and that smaller picture is what is
        stored and comes back here. Nothing on our side ever uses more than 1568, so the
        extra pixels would be bytes nobody reads. Keep your own copy if you need the
        full-resolution file.

        The type is sniffed from the BYTES, not from whatever the upload was named, so
        take the file extension from ``content_type`` rather than from what you sent.
        Raises ``NotFoundError`` when the memory has no image, or when the image was stored
        with a ``reference``: then the service keeps no bytes, and you fetch the picture
        from your own reference. It says "no" rather than handing back something empty,
        so "a memory with no image" never looks like "an image we lost".
        """
        user_id, memory_id = _split_id_args(user_id, memory_id)
        memory_id = _require_memory_id(memory_id, "the id that add/store or list_images returned")
        return self._request_bytes(
            "/api/v1/memory/image",
            {"user_id": self._uid(user_id), "memory_id": _reject_ctl("memory_id", memory_id)},
            model=model,
        )

    def forget_image(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                     preview: bool = False, model: Optional[str] = None) -> dict:
        """Remove the PHOTO from a memory, keeping its text.

        ``preview=True`` reports what would happen and changes nothing.
        """
        user_id, memory_id = _split_id_args(user_id, memory_id)
        memory_id = _require_memory_id(memory_id, "the id of the memory whose image you want removed")
        body: dict = {"user_id": self._uid(user_id), "memory_id": _reject_ctl("memory_id", memory_id)}
        if preview:
            body["preview"] = True
        return self._request("DELETE", "/api/v1/memory/image", json_body=body, model=model,
                             removes=not preview)

    def list_images(self, user_id: Optional[str] = None, *, limit: Optional[int] = None,
                    before: Optional[str] = None, skip_ids: Optional[list] = None,
                    model: Optional[str] = None) -> dict:
        """One page of image memories, newest first, plus the store's TOTAL image count.

        ``count`` is the total, not the size of the page — so "142 images" needs one call,
        not a walk. ``limit`` is 5 to 20; anything else is refused. Paging is by cursor:
        hand ``next_before`` and ``next_skip_ids`` back as ``before`` / ``skip_ids``. Both
        are needed because several images can share a timestamp, and a timestamp alone
        would repeat or skip them.
        """
        if limit is not None:
            _check_page(limit)
        body: dict = {"user_id": self._uid(user_id)}
        if limit is not None:
            body["limit"] = limit
        if before is not None:
            body["before"] = before
        if skip_ids is not None:
            body["skip_ids"] = skip_ids
        return self._post("/api/v1/memory/images", body, model=model)

    def iter_images(self, user_id: Optional[str] = None, *, page_size: Optional[int] = None,
                    model: Optional[str] = None):
        """Iterate every image memory, paging under the hood. ``page_size`` is 5 to 20."""
        if page_size is not None:
            _check_page(page_size, "page_size")
        before: Optional[str] = None
        skip: Optional[list] = None
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = self.list_images(user_id, limit=page_size, before=before,
                                    skip_ids=skip, model=model)
            rows = _as_records(page.get("images"))
            for m in rows:
                yield m
            if not page.get("has_more") or not page.get("next_before"):
                return
            before = page.get("next_before")
            nxt = page.get("next_skip_ids")
            skip = nxt if isinstance(nxt, list) else None
            if _cursor_repeats((before, tuple(skip) if skip else ()), seen, rows):
                return
        raise _truncated(f"stopped after {_MAX_PAGES} pages — the store did not end")

    def usage(self, days: int = 7) -> dict:
        """What this key has spent, and what is left — the numbers behind "keep going?".

        Free: no charge and no balance gate, because an account at zero still has to be
        able to find out why. Rate-limited instead.

        Scoped to THIS key: its own lifetime spend, plus its workspace and stores over
        the window. Never another key's. ``balance_cents`` is account-wide, since that
        is what gates the next call whichever key makes it.

        ``stores`` is highest spend first and at most 50 rows — a longer list is cut, so the
        rows need not sum to ``workspace``. A row named ``other`` is an overflow bucket,
        not a store: passing it as a store id finds nothing.

            u = mem.usage(7)
            if u["balance_cents"] < 100:
                stop()
        """
        if not _is_int(days) or not 1 <= days <= 365:
            raise ValueError(f"days must be an integer between 1 and 365, got {days!r}.")
        return self._request("GET", f"/api/v1/won/usage?days={days}")

    # ----- how much has this memory been edited -----

    def revisions(self, user_id: Optional[str] = None, *, include: Optional[str] = None,
                  limit: Optional[int] = None, before: Optional[str] = None,
                  skip_ids: Optional[list] = None, model: Optional[str] = None) -> dict:
        """How much of this store has been altered since it was written.

        Aimed at the MODEL rather than at you: an assistant leaning on its own memory
        should be able to ask how far that memory has been edited underneath it. Returns
        ``revised`` / ``unrevised`` / ``total`` plus a plain-language ``counts`` and
        ``excludes``.

        By default this is COUNTS ONLY, and the answer is the same size for a store of
        a hundred memories and a store of a hundred million. That is the point: a model
        asks this mid-conversation, and an answer that grew with the store would be
        unusable for exactly the customers who most need to ask.

        Pass ``include="revised"`` or ``include="unrevised"`` to ALSO get one page of the
        memories behind that number — ``limit`` 5 to 20 per call, one side per call. There
        is no way to ask for both lists in a single response. Page by cursor, handing
        ``next_before`` / ``next_skip_ids`` back as ``before`` / ``skip_ids``::

            n = mem.revisions()                       # numbers only
            print(n["revised"], "of", n["total"])

            before = skip = None
            while True:
                p = mem.revisions(include="revised", before=before, skip_ids=skip)
                for m in p["memories"]:
                    print(m["content"])
                if not p.get("has_more"):
                    break
                before, skip = p["next_before"], p["next_skip_ids"]

        Pages are ordered by when each memory was STORED, not by when it was edited;
        the response says so in ``ordered_by``.

        Served from ``/api/v1/won/*``, not ``/api/v1/memory/*``. Won is the surface for
        calls a model makes ABOUT its memory rather than calls an application makes WITH
        it, and the address says so.

        Counts memories a transform touched (supersede, update, retract, image removed).
        Deletions are NOT counted — a deleted memory leaves nothing to count. ``total``
        counts the memories you stored.
        """
        _check_include(include)
        if limit is not None:
            _check_page(limit)
        body: dict = {"user_id": self._uid(user_id)}
        if include is not None:
            body["include"] = include
        if limit is not None:
            body["limit"] = limit
        if before is not None:
            body["before"] = before
        if skip_ids is not None:
            body["skip_ids"] = skip_ids
        return self._post("/api/v1/won/revisions", body, model=model)

    def lineage(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                model: Optional[str] = None) -> dict:
        """The full chain of edits behind one memory, oldest first.

        ``is_current`` marks the version in force. ``revisions()`` says how much a store
        moved; this says what happened to one fact.
        """
        user_id, memory_id = _split_id_args(user_id, memory_id)
        memory_id = _require_memory_id(memory_id, "the id that add/store or list_memories returned")
        return self._post(
            "/api/v1/memory/lineage",
            {"user_id": self._uid(user_id), "memory_id": _reject_ctl("memory_id", memory_id)},
            model=model,
        )

    def by_speaker(self, speaker: str, user_id: Optional[str] = None, *,
                   limit: Optional[int] = None, before: Optional[str] = None,
                   skip_ids: Optional[list] = None, model: Optional[str] = None) -> dict:
        """What one person said, newest first.

        ``speaker`` is the tag written at store time — ``"me"`` for the assistant's own
        words, otherwise a person's name. Same cursor paging as ``list_images``, and the
        same ``limit`` of 5 to 20.

        ``records_to_delete`` is the count to show before anyone confirms a delete of this
        speaker's memories; ``points_to_delete`` is the same number under its old name.
        """
        if not isinstance(speaker, str) or not speaker.strip():
            raise ValueError('speaker is required — "me" for the assistant, or a person\'s name')
        if limit is not None:
            _check_page(limit)
        body: dict = {"user_id": self._uid(user_id), "speaker": _reject_ctl("speaker", speaker.strip())}
        if limit is not None:
            body["limit"] = limit
        if before is not None:
            body["before"] = before
        if skip_ids is not None:
            body["skip_ids"] = skip_ids
        return self._post("/api/v1/memory/by-speaker", body, model=model)

    def ping(self) -> bool:
        """Check connectivity AND that the API key works. Returns ``True`` on success,
        else raises — ``AuthenticationError`` (bad key), ``PaymentRequiredError`` (valid
        key but no card / depleted balance), or ``APIConnectionError`` (unreachable).
        Makes one ordinary (metered) request. Handy as a one-line setup check."""
        self._request("GET", "/api/v1/memory/collections")
        return True

    # ----- models -----

    def list_models(self) -> list[dict]:
        """Available models: ``[{"id", "name", "available", "memory", "capabilities"}, ...]``.

        ``memory`` is ``"shared"`` (models that read the same store) or ``"isolated"``
        (a model with its own dedicated memory). ``capabilities`` says what an available
        model can do: ``{"images", "engrams", "forms", "re_ask", "self_memories",
        "speaker_names"}``, each true or false. Needs no API key.

        ``retires_at`` (RFC3339) is present on a live model that is scheduled to retire.
        From that instant the model leaves this list and calls naming it are refused.
        """
        return _as_records(self._request("GET", "/api/v1/models").get("models"))

    def list_engrams(self) -> dict:
        """The engrams (and delivery forms) the selected model can actually run.

        Ask here rather than hard-coding names from the docs: the service is the
        authority, and the answer depends on the model (delivery forms need the ``forms``
        capability), so use ``with_model()`` for another model::

            cat = mem.list_engrams()
            [e["name"] for e in cat["engrams"]]

        Returns ``{"engrams": [...], "forms": [...], "note": str | None}``; ``note``
        explains an empty ``engrams`` on a model without engram support.
        """
        r = self._request("GET", "/api/v1/engram")
        # Start from the reply so fields this version does not name still reach the
        # caller; only the promised shapes are normalised.
        out = dict(r) if isinstance(r, dict) else {}
        out["engrams"] = _as_records(r.get("engrams") if isinstance(r, dict) else None)
        out["forms"] = _as_records(r.get("forms") if isinstance(r, dict) else None)
        note = r.get("note") if isinstance(r, dict) else None
        out["note"] = note if isinstance(note, str) else None
        return out

    # ----- stores -----

    def create_store(self, user_id: Optional[str] = None) -> dict:
        """Create a store — the ``user_id`` you read and write under.

        Stores are explicit: a store must exist before you ``add`` to or ``search``
        it, otherwise those calls return 404. Idempotent — creating an existing
        store is a no-op. Every account already has a ``default`` store. ``user_id``
        is optional — omit it to create the client's default store.

            mem = Client(api_key="wos-...", user_id="alice")
            mem.create_store()              # creates "alice"
            mem.add("she prefers tea")      # no user_id needed

        Returns ``{"user_id", "status"}`` where ``status`` is ``"created"`` or ``"exists"``,
        plus ``canonical_id`` and ``note`` when the id is filed under a normalized form.
        """
        return self._post("/api/v1/memory/collection", {"user_id": self._uid(user_id)})

    def list_stores(self) -> list[dict]:
        """List your stores: ``[{"user_id", "created_at", "canonical_id"?}, ...]``
        (``default`` first). Each ``user_id`` is the id the store was created with;
        ``canonical_id`` appears when the normalized form differs."""
        return _as_records(self._request("GET", "/api/v1/memory/collections").get("collections"))

    def delete_store(self, user_id: str) -> dict:
        """Delete a store and ALL its memories. Returns ``{"user_id", "status"}``."""
        if not isinstance(user_id, str) or not user_id.strip():
            raise ValueError("user_id is required (a non-blank string) — delete_store never falls back to the default store.")
        # Destructive calls bypass _uid(), so they warn here.
        _warn_if_store_id_collapses(user_id)
        return self._request("DELETE", "/api/v1/memory/collection", json_body={"user_id": user_id})

    # ----- speakers (who said it) -----

    def add_speaker(self, speaker: str, user_id: Optional[str] = None) -> dict:
        """Register a person for this store. Speakers are explicit: register once,
        then store with ``speaker=``. ``"me"`` (the assistant itself) never needs
        registration. A store registers up to 50 people to start.

            mem.add_speaker("Bob")
            mem.add("Bob said the deadline moved", speaker="Bob")
        """
        return self._post("/api/v1/memory/speakers", {"user_id": self._uid(user_id), "speaker": speaker})

    def list_speakers(self, user_id: Optional[str] = None) -> dict:
        """The store's registered people, each with its memory count:
        ``{"speakers": [...], ...}``, with ``speakers`` always a list."""
        return _speakers_list(self._request("GET", "/api/v1/memory/speakers", params={"user_id": self._uid(user_id)}))

    def remove_speaker(self, speaker: str, user_id: Optional[str] = None) -> dict:
        """Unregister a person. Their memories stay; the name tag goes."""
        return self._request(
            "DELETE", "/api/v1/memory/speakers", json_body={"user_id": self._uid(user_id), "speaker": speaker}
        )

    def _request_bytes(self, path: str, body: dict, *, model: Optional[str] = None) -> tuple[bytes, str]:
        """One request that answers with BYTES rather than JSON (``/memory/image``).

        Errors still arrive as JSON, so a non-2xx goes through the usual ``_parse_error``
        and keeps ``NotFoundError`` / ``AuthenticationError`` behaving as everywhere else.
        It retries like every other POST.
        """
        status, headers, data = self._call("POST", path, json_body=body, model=model, reads=True)
        if not data:
            # An empty 200 would otherwise read as "here is your image" and write a
            # zero-byte file — indistinguishable from an image we lost.
            raise WosError(status, "empty image body — the service returned no bytes")
        return data, headers.get("content-type", "application/octet-stream")

    # ----- delete -----

    def delete(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *, model: Optional[str] = None) -> dict:
        """Delete a single memory by id. Omit ``user_id`` (keyword ``memory_id=``) for the default store."""
        # Same store-first argument order as get(): recover the lone-id call before
        # the guard, so `mem.delete(memory_id)` deletes that ONE memory instead of
        # raising. It cannot widen a delete — a lone UUID names a memory, and with
        # no id at all the guard below still fires.
        user_id, memory_id = _split_id_args(user_id, memory_id)
        # `.strip()` matters as much as the emptiness test: "   " is truthy, and a server
        # that trims it back to nothing would read the request as the whole-store form.
        if not isinstance(memory_id, str) or not memory_id.strip():
            raise ValueError(
                "memory_id is required (non-blank). To delete every memory in a store, call delete_all(user_id) explicitly. "
                "Note the argument order: delete(user_id, memory_id)."
            )
        memory_id = memory_id.strip()
        return self._post(
            "/api/v1/memory/forget", {"user_id": self._uid(user_id), "memory_id": memory_id}, model=model
        )

    def delete_all(self, user_id: str, *, model: Optional[str] = None) -> dict:
        """Delete ALL memories for a store (GDPR erase). ``user_id`` is required here on
        purpose — this is destructive, so it never falls back to the default store."""
        if not isinstance(user_id, str) or not user_id.strip():
            raise ValueError("user_id is required (a non-blank string) for delete_all — anything else would wipe the default store.")
        # Destructive calls bypass _uid(), so they warn here.
        _warn_if_store_id_collapses(user_id)
        return self._post("/api/v1/memory/forget", {"user_id": user_id}, model=model)

    # ----- internal -----

    def __setstate__(self, state: dict) -> None:
        # An unpickled client has a session of its own, dropped after fork() like any other.
        self.__dict__.update(state)
        _sessions.add(self._session)

    def __copy__(self) -> "Client":
        return _copy_client(self, "_owns_session", ("_session",), None)

    def __deepcopy__(self, memo: dict) -> "Client":
        return _copy_client(self, "_owns_session", ("_session",), memo)

    def _post(self, path: str, body: dict, model: Optional[str] = None,
              idempotency_key: Optional[str] = None) -> dict:
        return self._request("POST", path, json_body=body, model=model,
                             idempotency_key=idempotency_key)

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
        model: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        removes: Optional[bool] = None,
    ) -> dict:
        status, headers, payload = self._call(method, path, json_body=json_body, params=params,
                                              model=model, idempotency_key=idempotency_key,
                                              removes=removes)
        data = _parse_ok(status, payload.decode("utf-8", "replace"))
        # Surface whether the write was stored or replayed. The server says so with the
        # Idempotent-Replayed header, and the caller cannot tell otherwise. Only when the
        # body does not already carry it: a response may carry fields this client has
        # never seen, and the service's value wins over a guess.
        if headers.get("Idempotent-Replayed") == "true" and "replayed" not in data:
            data["replayed"] = True
        return data

    def _call(self, method: str, path: str, *, json_body: Optional[dict] = None,
              params: Optional[dict] = None, model: Optional[str] = None,
              idempotency_key: Optional[str] = None, removes: Optional[bool] = None,
              reads: bool = False) -> tuple:
        """One call with its retries: ``(status, headers, body)`` for a 2xx, the matching
        ``WosError`` for anything else."""
        # X-WOS-Model is set per request, not on the shared session, since clones
        # differ on it: a per-call model overrides the client default.
        eff_model = _check_model(model) if model else self._model
        headers = {"X-WOS-Model": eff_model} if eff_model else {}
        idem = _idem_headers(idempotency_key)
        if idem:
            headers.update(idem)
        data = _encode_json(json_body)
        url = f"{self._base}{path}"
        call = _Call(method, path, self._retries, self._deadline, removes=removes, reads=reads)
        for attempt in range(call.attempts):
            call.attempt = attempt
            clock = call.clock(self._timeout)
            start = time.monotonic()
            got = self._attempt(method, url, data, params, headers or None, clock, call)
            if isinstance(got, _Failure):
                time.sleep(call.after_failure(got, clock))
                continue
            status, rheaders, payload = got
            # `rate_limit` reports the most recent response that carried the headers.
            self._rate_limit = _parse_rate_limit(rheaders) or self._rate_limit
            _logger.debug("%s -> %d in %.0fms (attempt %d/%d)", call.where, status,
                          (time.monotonic() - start) * 1000, attempt + 1, call.attempts)
            if 200 <= status < 300:
                return got
            time.sleep(call.after_status(status, rheaders, payload))
        raise RuntimeError("retries exhausted")  # unreachable: the last attempt returns or raises

    def _attempt(self, method: str, url: str, data: Optional[bytes], params: Optional[dict],
                 headers: Optional[dict], clock: _Clock, call: _Call) -> Any:
        """One attempt, ended by a watchdog at ``clock.end``. Returns ``(status, headers,
        body)``, or a ``_Failure`` when the transport failed.

        Nothing here raises from inside an ``except`` block, so no error this client
        raises is chained to the transport's exception.
        """
        watchdog = _Watchdog(clock.end)
        _attempt_local.watchdog = watchdog
        r = None
        failure: Optional[_Failure] = None
        payload = b""
        try:
            try:
                left = max(0.001, clock.left())
                r = self._session.request(
                    method,
                    url,
                    data=data,
                    params=params,
                    headers=headers,
                    # (connect, read): a dead host does not get the whole budget to accept.
                    timeout=(min(10.0, left), left),
                    # Never follow a redirect: requests forwards custom headers (the API
                    # key) to wherever a 3xx points. The API never redirects.
                    allow_redirects=False,
                    # Streamed so the size cap and the clock apply while reading.
                    stream=True,
                )
            except requests.exceptions.RequestException as e:
                broke = isinstance(e, requests.exceptions.ConnectionError)
                never = broke and _never_sent(e)
                timed = not never and _timed_out(e)
                failure = _Failure(_transport_reason(e), never_sent=never, timed_out=timed,
                                   dropped=broke and not timed)
            if r is not None and not 300 <= r.status_code < 400:
                body_clock = clock
                if call.retries_regardless(r.status_code, r.headers):
                    body_clock = clock.within(_RETRYABLE_BODY_WAIT)
                    watchdog.sooner(body_clock.end)
                try:
                    payload = _read_body(r, body_clock, watchdog)
                except WosError as e:
                    # The size cap. On an error status the status is the answer.
                    if r.status_code < 400:
                        raise
                    call.unread = e.message
                except Exception as e:
                    # On an error status the body is only the message: the status stands.
                    # On a 2xx a body that stopped arriving is a transport failure, and a
                    # timeout is not a drop (see `_timed_out`).
                    if r.status_code < 400:
                        timed = isinstance(e, _Expired) or _timed_out(e)
                        failure = _Failure(_transport_reason(e), timed_out=timed, dropped=not timed)
                    elif body_clock.left() > 0:
                        call.unread = _transport_reason(e)
                    elif body_clock.end < clock.end:
                        call.unread = f"not received within {_RETRYABLE_BODY_WAIT:g}s"
                    else:
                        call.unread = clock.error().message
        finally:
            _attempt_local.watchdog = None
            fired = watchdog.stop()
            if r is not None:
                r.close()
        if failure is not None:
            if fired and not failure.never_sent:
                failure.timed_out, failure.dropped = True, False
            return failure
        return r.status_code, r.headers, payload


# Back-compat alias: older code used `WME`.
WME = Client


class _AsyncTransport:
    """The httpx client an AsyncClient and its clones share, the event loop it belongs to,
    whether its owner has closed it, and the process it was made in. ``factory`` builds
    another like it, and is None for an httpx client passed in. ``traces`` is set once a
    request has reported its progress through httpx's trace extension."""

    __slots__ = ("http", "loop", "closed", "pid", "factory", "traces")

    def __init__(self, http: Any, factory: Any = None):
        self.http = http
        self.loop: Any = None
        self.closed = False
        self.pid = os.getpid()
        self.factory = factory
        self.traces = False


def _new_async_http(httpx: Any, api_key: str) -> Any:
    """The httpx client an AsyncClient builds for itself."""
    return httpx.AsyncClient(
        headers={
            "X-API-Key": api_key,
            "Content-Type": "application/json",
            "User-Agent": _USER_AGENT,
            # One gzip layer at most, which this client decodes within the cap.
            "Accept-Encoding": "gzip",
        },
        verify=_async_tls_context(),
        follow_redirects=False,
    )


def _loop_ref(loop: Any) -> Any:
    try:
        return weakref.ref(loop)
    except TypeError:
        return lambda: loop


def _asyncio_loop() -> Any:
    """The running asyncio event loop, or None under another async library (trio)."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


async def _async_sleep(seconds: float) -> None:
    if _asyncio_loop() is not None:
        await asyncio.sleep(seconds)
    else:
        import anyio  # httpx depends on it

        await anyio.sleep(seconds)


async def _within(awaitable: Any, seconds: float) -> Any:
    """``awaitable``'s result, or None when it takes longer than ``seconds``."""
    if _asyncio_loop() is not None:
        try:
            return await asyncio.wait_for(awaitable, seconds)
        except asyncio.TimeoutError:
            return None
    import anyio  # httpx depends on it

    with anyio.move_on_after(seconds):
        return await awaitable
    return None


class AsyncClient:
    """Async twin of :class:`Client` — the exact same surface, awaitable.

    Requires the async extra (an ``httpx`` dependency)::

        pip install "wontopos[async]"

        from wontopos import AsyncClient
        async with AsyncClient(api_key="wos-...", user_id="alice") as mem:
            await mem.create_store()
            await mem.add("she prefers tea over coffee")
            hits = await mem.search("what does alice drink?")

    A client made before ``fork()`` works in the child with its own connections. It
    cannot be pickled; ``copy.copy()`` and ``copy.deepcopy()`` share its connections.

    One client serves one event loop: the loop it first runs on. Create one inside each
    ``asyncio.run()`` rather than sharing a module-level client across loops. A call on
    another loop, or after ``aclose()``, raises ``APIConnectionError`` before anything is
    sent.

    Same reliability and security posture as the sync client: retries with backoff
    (429 and a 409 for a write in flight on every call; 408/502/503/504 and connect
    errors only where a retry cannot apply a write twice; ``Retry-After`` honored), wall-clock ``timeout`` and ``deadline``, redirects refused, TLS 1.2 floor,
    64MB response cap, key masked in ``repr``. Close it with ``await mem.aclose()`` or
    use ``async with``.
    """

    DEFAULT_USER = "default"

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
        model: str = DEFAULT_MODEL,
        user_id: str = DEFAULT_USER,
        retries: Optional[int] = None,
        *,
        max_retries: Optional[int] = None,
        deadline: Optional[float] = None,
        _http: Any = None,
    ):
        # Same alias as the sync client: an explicit ``retries`` wins over ``max_retries``.
        if retries is None:
            retries = 2 if max_retries is None else max_retries
        try:
            import httpx

            for name in ("AsyncClient", "Timeout", "URL", "HTTPError"):
                getattr(httpx, name)
        except (ImportError, AttributeError) as e:
            raise ImportError(
                'AsyncClient needs httpx>=0.27,<1, from the async extra: pip install "wontopos[async]"'
            ) from e
        self._api_key = _clean_key(api_key)
        self._base = _check_base_url(base_url)
        _require_url(self._base, _async_scheme_host)
        self._timeout = _check_seconds(timeout, 30.0)
        self._deadline = _check_seconds(deadline, None)
        self._model = _check_model(model)
        # The same guard _uid applies per call, so the constructor and with_user() —
        # the documented per-tenant pattern — cannot land in the default store either.
        _assert_usable_store_id(user_id)
        self._user_id = user_id
        self._retries = max(0, int(retries))
        self._rate_limit: Optional[dict] = None
        self._httpx = httpx
        _warn_if_plain_http(self._base, _async_scheme_host)
        # A clone shares the parent's transport (connection pool + TLS reused). Model and
        # timeout are NOT baked into the shared client — they differ across clones and are
        # applied per request instead. Only the owner closes it.
        self._owns_http = _http is None
        if isinstance(_http, _AsyncTransport):
            self._transport = _http
        elif _http is not None:
            self._transport = _AsyncTransport(_http)
        else:
            api_key = self._api_key
            factory = lambda: _new_async_http(httpx, api_key)  # noqa: E731
            self._transport = _AsyncTransport(factory(), factory)

    @classmethod
    def from_env(cls, **kwargs: Any) -> "AsyncClient":
        """Like :meth:`Client.from_env`, async."""
        return cls(_env_key(), **kwargs)

    def __repr__(self) -> str:
        return (
            f"AsyncClient(base_url={_mask_userinfo(self._base)!r}, model={self._model!r}, "
            f"user_id={self._user_id!r}, api_key='{_mask_key(self._api_key)}')"
        )

    async def aclose(self) -> None:
        # A clone shares the parent's transport and does NOT close it — only the client
        # that created it does. After that, the owner and every clone refuse calls.
        t = self._transport
        if self._owns_http and not t.closed:
            t.closed = True
            # An httpx client inherited across fork() is left open: closing it could end
            # connections the parent still uses.
            if t.pid == os.getpid():
                await t.http.aclose()

    def __reduce__(self) -> Any:
        raise TypeError(
            "an AsyncClient cannot be pickled: it holds the API key and open connections. "
            "Build one per process instead, e.g. AsyncClient.from_env() in each worker."
        )

    def __copy__(self) -> "AsyncClient":
        return _copy_client(self, "_owns_http", ("_transport", "_httpx"), None)

    def __deepcopy__(self, memo: dict) -> "AsyncClient":
        return _copy_client(self, "_owns_http", ("_transport", "_httpx"), memo)

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    @property
    def rate_limit(self) -> Optional[dict]:
        """Quota from the most recent call: ``{"limit", "remaining", "reset"}`` (or
        ``None`` before the first call). Read it to self-throttle."""
        return self._rate_limit

    def _clone(self, **overrides: Any) -> "AsyncClient":
        """Everything else kept.

        One list, four callers. Four hand-written copies of "everything else" is how
        an option gets dropped by one clone and not the others: the failure reads as
        "with_model() ignores my timeout" and nothing points at the constructor call
        that forgot it. The transport is carried too — a clone that rebuilt it would
        drop a caller's session pooling.
        """
        kw: dict = dict(
            api_key=self._api_key, base_url=self._base, timeout=self._timeout,
            model=self._model, user_id=self._user_id, retries=self._retries,
            deadline=self._deadline, _http=self._transport,
        )
        kw.update(overrides)
        return AsyncClient(**kw)

    def with_deadline(self, deadline: float) -> "AsyncClient":
        """Like :meth:`Client.with_deadline`, async."""
        return self._clone(deadline=deadline)

    def with_user(self, user_id: str) -> "AsyncClient":
        """An async client bound to ``user_id`` as its default store (shares the pool)."""
        return self._clone(user_id=user_id)

    def with_model(self, model: str) -> "AsyncClient":
        """An async client that uses ``model`` for every call (shares the pool)."""
        return self._clone(model=model)

    def with_timeout(self, timeout: float) -> "AsyncClient":
        """An async client with a different per-attempt timeout in seconds (shares the pool)."""
        return self._clone(timeout=timeout)

    def with_retries(self, retries: int) -> "AsyncClient":
        """An async client with a different retry budget. 0 disables retries (shares the pool)."""
        return self._clone(retries=retries)

    def _uid(self, user_id: Optional[str]) -> str:
        # Same rule as the sync client: an omitted id uses the default, a PASSED blank id
        # is a bug at the call site and must not silently land in the default store.
        if user_id is not None:
            _assert_usable_store_id(user_id)
        sid = user_id if user_id else self._user_id
        _warn_if_store_id_collapses(sid)
        return sid

    # ----- write -----

    async def add(self, content: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
            idempotency_key: Optional[str] = None, metadata: Optional[dict] = None,
            image: Optional[dict] = None,
            **extra: Any) -> dict:
        """Store one memory. Extra keyword args become metadata (incl. ``speaker=``); the
        service keeps ``speaker``, ``event_date``, ``category`` and ``conversation_id``.

        ``image={"data": <base64>}`` attaches an image, on a model with the ``images``
        capability; the caption is still required. See ``Client.add``.
        """
        sid = self._uid(user_id)
        md = _metadata(metadata, extra)
        body: dict = {"user_id": sid, "content": content, "metadata": md}
        if image is not None:
            body["image"] = _normalize_image(image)
        return await self._post(
            "/api/v1/memory/store",
            body,
            model=model,
            idempotency_key=idempotency_key,
        )

    store = add  # alias (back-compat / preference)


    async def add_turn(
        self, user_msg: str = "", assistant_msg: str = "", user_id: Optional[str] = None, *,
        model: Optional[str] = None, idempotency_key: Optional[str] = None,
    ) -> dict:
        """Store a conversation turn (user + assistant). Payload first, ``user_id`` second."""
        return await self._post(
            "/api/v1/memory/store-turn",
            {"user_id": self._uid(user_id), "user_msg": user_msg, "assistant_msg": assistant_msg},
            model=model,
            idempotency_key=idempotency_key,
        )

    async def add_bulk(
        self, content: str, user_id: Optional[str] = None, category: Optional[str] = "general",
        timestamp: Optional[str] = None, *, model: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Bulk-ingest a large blob of text in one call. ``timestamp`` must be RFC3339;
        anything else is ignored and the memory is filed at upload time."""
        body: dict[str, Any] = {"user_id": self._uid(user_id), "content": content,
                                "category": "general" if category is None else category}
        if timestamp:
            body["timestamp"] = timestamp
        return await self._post("/api/v1/memory/bulk-store", body, model=model,
                                idempotency_key=idempotency_key)

    async def update(
        self, old_memory_id: str = "", new_content: str = "", user_id: Optional[str] = None, *,
        model: Optional[str] = None, idempotency_key: Optional[str] = None,
    ) -> dict:
        """Supersede an old memory with new content. Payload first, ``user_id`` second."""
        return await self._post(
            "/api/v1/memory/supersede",
            {"user_id": self._uid(user_id), "old_memory_id": old_memory_id, "new_content": new_content},
            model=model,
            idempotency_key=idempotency_key,
        )

    # ----- read -----

    async def search(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> list[dict]:
        """Search a store's memories. Returns them most relevant first.

        The list ARRIVES in that order — do not re-sort it. ``similarity`` on each
        memory is a raw closeness score, not the ranking key: what produces the order
        is internal and is not returned, so sorting by ``similarity`` overrides the
        ranking and makes results worse. There is no ``score`` field.

        Takes the same options as :meth:`Client.search` — ``filters``, ``cache_control``
        (repeated queries bill at 0.1×), ``speaker``, ``verify``, ``max_images`` —
        documented there. Filters apply to ``memories``; the assistant's own words are
        not filtered.
        """
        body = _search_body(self._uid(user_id), query, limit, opts, verify, max_images)
        return _merge_results(await self._post("/api/v1/memory/search", body, model=model))

    async def search_full(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> dict:
        """Search, keeping every field — the photos and ``verify_used`` included.
        See :meth:`Client.search_full`."""
        body = _search_body(self._uid(user_id), query, limit, opts, verify, max_images)
        r = await self._post("/api/v1/memory/search", body, model=model)
        r = dict(r) if isinstance(r, dict) else {}
        for field in ("memories", "self_memories", "images"):
            r[field] = _as_records(r.get(field))
        return r

    async def search_self(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> dict:
        """Search a model with the ``self_memories`` capability: both fields from ONE call.

        Returns ``{"memories": [...], "self_memories": [...]}`` — general memories plus
        the assistant's own words (``speaker="me"``) kept separate. ``self_memories`` is
        ``[]`` on a model without the capability. See ``Client.search_self``.
        """
        body = _search_body(self._uid(user_id), query, limit, opts, verify, max_images)
        r = await self._post("/api/v1/memory/search", body, model=model)
        return {
            "memories": _as_records(r.get("memories")),
            "self_memories": _as_records(r.get("self_memories")),
        }

    async def recall(
        self, query: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
        form: Optional[str] = None, tz: Optional[int] = None,
        limit: Optional[int] = None, context_limit: Optional[int] = None,
    ) -> dict:
        """One-call context for an LLM: ``{"short_term", "long_term", "context"}``.
        ``form``/``tz`` render long-term memory times (memoir/archive) on a model with the
        ``forms`` capability."""
        body = _recall_body(self._uid(user_id), query, form, tz, limit, context_limit)
        return await self._post("/api/v1/memory/recall", body, model=model)

    async def engram(
        self, name: str, query: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
        form: Optional[str] = None, tz: Optional[int] = None,
    ) -> dict:
        """Run a built-in engram; the service is the authority on the names it accepts.
        ``form``/``tz`` render memory times (memoir/archive) on a model with the ``forms``
        capability."""
        body = _form_body({"name": name, "user_id": self._uid(user_id), "query": query}, form, tz)
        return await self._post("/api/v1/engram/run", body, model=model)

    async def history(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """Recent conversation turns (short-term memory)."""
        return _as_records((await self._post("/api/v1/memory/history", {"user_id": self._uid(user_id)}, model=model)).get("turns"))

    async def stats(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> dict:
        """Memory counts for a store: ``{total_memories, short_term_turns}``."""
        return await self._post("/api/v1/memory/stats", {"user_id": self._uid(user_id)}, model=model)

    async def get(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *, model: Optional[str] = None) -> dict:
        """Fetch ONE memory by id — the text you stored, and its metadata.
        Same visibility as ``list_memories``: an unknown id, one from another store, or
        one ``list_memories`` does not return raises ``NotFoundError``."""
        user_id, memory_id = _split_id_args(user_id, memory_id)
        # A blank id is refused and a valid one is stripped, as in the sibling calls.
        if not isinstance(memory_id, str) or not memory_id.strip():
            raise ValueError(
                "memory_id is required — the id that add/store or list_memories returned. "
                "Note the argument order: get(user_id, memory_id). With a store set on the "
                'client, call get(memory_id="...").'
            )
        memory_id = memory_id.strip()
        r = await self._post(
            "/api/v1/memory/get", {"user_id": self._uid(user_id), "memory_id": memory_id}, model=model
        )
        return _memory_from_get(r)

    async def list_memories(
        self, user_id: Optional[str] = None, *, limit: Optional[int] = 100, cursor: Optional[str] = None,
        model: Optional[str] = None
    ) -> dict:
        """List a store's stored memories — the text you stored, and its metadata.
        Paginated via ``cursor``/``next_cursor``: stop when it is ``None``. It can be
        non-null on the last page (the next call returns an empty page); pass back only a
        cursor the service returned, with the model that returned it. ``limit`` is 1 to 500 (default 100, also for ``None``).
        Returns ``{"memories": [...], "count": int, "next_cursor": str | None}``.
        """
        limit = _list_size(limit, "limit")
        body: dict = {"user_id": self._uid(user_id), "limit": limit}
        if cursor:
            body["cursor"] = cursor
        return await self._post("/api/v1/memory/list", body, model=model)

    async def iter_memories(self, user_id: Optional[str] = None, *, page_size: Optional[int] = 100,
                            model: Optional[str] = None):
        """Async-yield every stored memory in a store, paging under the hood.

            async for m in mem.iter_memories():
                print(m["id"], m["content"])
        """
        page_size = _list_size(page_size, "page_size")
        cursor: Optional[str] = None
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = await self.list_memories(user_id, limit=page_size, cursor=cursor, model=model)
            rows = _as_records(page.get("memories"))
            for m in rows:
                yield m
            nxt = page.get("next_cursor")
            if not nxt or _cursor_repeats(nxt, seen, rows):
                return
            cursor = nxt
        raise _truncated(f"stopped after {_MAX_PAGES} pages — the store did not end")

    async def export_memories(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """Return ALL of a store's memories as a list (the text you stored, and its metadata)."""
        return [m async for m in self.iter_memories(user_id, model=model)]

    async def export_images(self, user_id: Optional[str] = None, *, page_size: Optional[int] = None,
                            model: Optional[str] = None) -> list[dict]:
        """Return ALL of a store's image memories as a list."""
        return [m async for m in self.iter_images(user_id, page_size=page_size, model=model)]

    # ----- images (models with the ``images`` capability) -----

    async def get_image(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                        model: Optional[str] = None) -> tuple[bytes, str]:
        """Fetch the bytes of an image memory → ``(bytes, content_type)``.

        This is the picture the SERVICE holds: anything over 1568px on its long edge was
        downscaled on the way in, and re-encoded to WebP unless it was a JPEG. Name the
        file from ``content_type``, not from what you uploaded. An image stored with a
        ``reference`` has no bytes here (``NotFoundError``). See ``Client.get_image``."""
        user_id, memory_id = _split_id_args(user_id, memory_id)
        memory_id = _require_memory_id(memory_id, "the id that add/store or list_images returned")
        return await self._request_bytes(
            "/api/v1/memory/image",
            {"user_id": self._uid(user_id), "memory_id": _reject_ctl("memory_id", memory_id)},
            model=model,
        )

    async def forget_image(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                           preview: bool = False, model: Optional[str] = None) -> dict:
        """Remove the PHOTO from a memory, keeping its text. ``preview=True`` reports what
        would happen and changes nothing."""
        user_id, memory_id = _split_id_args(user_id, memory_id)
        memory_id = _require_memory_id(memory_id, "the id of the memory whose image you want removed")
        body: dict = {"user_id": self._uid(user_id), "memory_id": _reject_ctl("memory_id", memory_id)}
        if preview:
            body["preview"] = True
        return await self._request("DELETE", "/api/v1/memory/image", json_body=body, model=model,
                                   removes=not preview)

    async def list_images(self, user_id: Optional[str] = None, *, limit: Optional[int] = None,
                          before: Optional[str] = None, skip_ids: Optional[list] = None,
                          model: Optional[str] = None) -> dict:
        """One page of image memories, newest first, plus the store's TOTAL ``count``.
        ``limit`` is 5 to 20. See ``Client.list_images``."""
        if limit is not None:
            _check_page(limit)
        body: dict = {"user_id": self._uid(user_id)}
        if limit is not None:
            body["limit"] = limit
        if before is not None:
            body["before"] = before
        if skip_ids is not None:
            body["skip_ids"] = skip_ids
        return await self._post("/api/v1/memory/images", body, model=model)

    async def iter_images(self, user_id: Optional[str] = None, *, page_size: Optional[int] = None,
                          model: Optional[str] = None):
        """Async-yield every image memory, paging under the hood."""
        if page_size is not None:
            _check_page(page_size, "page_size")
        before: Optional[str] = None
        skip: Optional[list] = None
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = await self.list_images(user_id, limit=page_size, before=before,
                                          skip_ids=skip, model=model)
            rows = _as_records(page.get("images"))
            for m in rows:
                yield m
            if not page.get("has_more") or not page.get("next_before"):
                return
            before = page.get("next_before")
            nxt = page.get("next_skip_ids")
            skip = nxt if isinstance(nxt, list) else None
            if _cursor_repeats((before, tuple(skip) if skip else ()), seen, rows):
                return
        raise _truncated(f"stopped after {_MAX_PAGES} pages — the store did not end")

    async def usage(self, days: int = 7) -> dict:
        """What this key has spent, and what is left — the numbers behind "keep going?".

        Free: no charge and no balance gate, because an account at zero still has to be
        able to find out why. Rate-limited instead.

        Scoped to THIS key: its own lifetime spend, plus its workspace and stores over
        the window. Never another key's. ``balance_cents`` is account-wide, since that
        is what gates the next call whichever key makes it.

        ``stores`` is highest spend first and at most 50 rows — a longer list is cut, so the
        rows need not sum to ``workspace``. A row named ``other`` is an overflow bucket,
        not a store: passing it as a store id finds nothing.

            u = await mem.usage(7)
            if u["balance_cents"] < 100:
                stop()
        """
        if not _is_int(days) or not 1 <= days <= 365:
            raise ValueError(f"days must be an integer between 1 and 365, got {days!r}.")
        return await self._request("GET", f"/api/v1/won/usage?days={days}")

    # ----- how much has this memory been edited -----

    async def revisions(self, user_id: Optional[str] = None, *, include: Optional[str] = None,
                        limit: Optional[int] = None, before: Optional[str] = None,
                        skip_ids: Optional[list] = None, model: Optional[str] = None) -> dict:
        """How much of this store has been altered since it was written.

        Counts only unless ``include`` asks for a page. See ``Client.revisions``."""
        _check_include(include)
        if limit is not None:
            _check_page(limit)
        body: dict = {"user_id": self._uid(user_id)}
        if include is not None:
            body["include"] = include
        if limit is not None:
            body["limit"] = limit
        if before is not None:
            body["before"] = before
        if skip_ids is not None:
            body["skip_ids"] = skip_ids
        return await self._post("/api/v1/won/revisions", body, model=model)

    async def lineage(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                      model: Optional[str] = None) -> dict:
        """The full chain of edits behind one memory, oldest first. See ``Client.lineage``."""
        user_id, memory_id = _split_id_args(user_id, memory_id)
        memory_id = _require_memory_id(memory_id, "the id that add/store or list_memories returned")
        return await self._post(
            "/api/v1/memory/lineage",
            {"user_id": self._uid(user_id), "memory_id": _reject_ctl("memory_id", memory_id)},
            model=model,
        )

    async def by_speaker(self, speaker: str, user_id: Optional[str] = None, *,
                         limit: Optional[int] = None, before: Optional[str] = None,
                         skip_ids: Optional[list] = None, model: Optional[str] = None) -> dict:
        """What one person said, newest first. See ``Client.by_speaker``."""
        if not isinstance(speaker, str) or not speaker.strip():
            raise ValueError('speaker is required — "me" for the assistant, or a person\'s name')
        if limit is not None:
            _check_page(limit)
        body: dict = {"user_id": self._uid(user_id), "speaker": _reject_ctl("speaker", speaker.strip())}
        if limit is not None:
            body["limit"] = limit
        if before is not None:
            body["before"] = before
        if skip_ids is not None:
            body["skip_ids"] = skip_ids
        return await self._post("/api/v1/memory/by-speaker", body, model=model)

    async def ping(self) -> bool:
        """Check connectivity AND that the API key works. ``True`` on success, else raises
        ``AuthenticationError`` / ``PaymentRequiredError`` / ``APIConnectionError``. Makes
        one ordinary (metered) request."""
        await self._request("GET", "/api/v1/memory/collections")
        return True

    # ----- models / stores -----

    async def list_engrams(self) -> dict:
        """The engrams + delivery forms the selected model can run (see sync client)."""
        r = await self._request("GET", "/api/v1/engram")
        # Start from the reply so fields this version does not name still reach the
        # caller; only the promised shapes are normalised.
        out = dict(r) if isinstance(r, dict) else {}
        out["engrams"] = _as_records(r.get("engrams") if isinstance(r, dict) else None)
        out["forms"] = _as_records(r.get("forms") if isinstance(r, dict) else None)
        note = r.get("note") if isinstance(r, dict) else None
        out["note"] = note if isinstance(note, str) else None
        return out

    async def list_models(self) -> list[dict]:
        """Available models, each with its ``capabilities``. Needs no API key. See
        ``Client.list_models``."""
        return _as_records((await self._request("GET", "/api/v1/models")).get("models"))

    async def create_store(self, user_id: Optional[str] = None) -> dict:
        """Create a store (explicit, idempotent). Returns ``{"user_id", "status"}``, plus
        ``canonical_id`` and ``note`` when the id is filed under a normalized form."""
        return await self._post("/api/v1/memory/collection", {"user_id": self._uid(user_id)})

    async def list_stores(self) -> list[dict]:
        """List your stores (``default`` first)."""
        return _as_records((await self._request("GET", "/api/v1/memory/collections")).get("collections"))

    async def delete_store(self, user_id: str) -> dict:
        """Delete a store and ALL its memories."""
        if not isinstance(user_id, str) or not user_id.strip():
            raise ValueError("user_id is required (a non-blank string) — delete_store never falls back to the default store.")
        # Same as the sync client: warn before dropping a whole store (see there).
        _warn_if_store_id_collapses(user_id)
        return await self._request("DELETE", "/api/v1/memory/collection", json_body={"user_id": user_id})

    # ----- speakers -----

    async def add_speaker(self, speaker: str, user_id: Optional[str] = None) -> dict:
        """Register a person for this store (explicit; ``"me"`` never needs it)."""
        return await self._post("/api/v1/memory/speakers", {"user_id": self._uid(user_id), "speaker": speaker})

    async def list_speakers(self, user_id: Optional[str] = None) -> dict:
        """The store's registered people, each with its memory count."""
        return _speakers_list(await self._request("GET", "/api/v1/memory/speakers", params={"user_id": self._uid(user_id)}))

    async def remove_speaker(self, speaker: str, user_id: Optional[str] = None) -> dict:
        """Unregister a person. Their memories stay; the name tag goes."""
        return await self._request(
            "DELETE", "/api/v1/memory/speakers", json_body={"user_id": self._uid(user_id), "speaker": speaker}
        )

    # ----- delete -----

    async def delete(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *, model: Optional[str] = None) -> dict:
        """Delete a single memory by id."""
        # Same store-first argument order as get(): recover the lone-id call before
        # the guard, so `mem.delete(memory_id)` deletes that ONE memory instead of
        # raising. It cannot widen a delete — a lone UUID names a memory, and with
        # no id at all the guard below still fires.
        user_id, memory_id = _split_id_args(user_id, memory_id)
        # `.strip()` matters as much as the emptiness test: "   " is truthy, and a server
        # that trims it back to nothing would read the request as the whole-store form.
        if not isinstance(memory_id, str) or not memory_id.strip():
            raise ValueError(
                "memory_id is required (non-blank). To delete every memory in a store, call delete_all(user_id) explicitly. "
                "Note the argument order: delete(user_id, memory_id)."
            )
        memory_id = memory_id.strip()
        return await self._post(
            "/api/v1/memory/forget", {"user_id": self._uid(user_id), "memory_id": memory_id}, model=model
        )

    async def delete_all(self, user_id: str, *, model: Optional[str] = None) -> dict:
        """Delete ALL memories for a store (GDPR erase). ``user_id`` required on purpose."""
        if not isinstance(user_id, str) or not user_id.strip():
            raise ValueError("user_id is required (a non-blank string) for delete_all — anything else would wipe the default store.")
        # Destructive calls bypass _uid(), so they warn here.
        _warn_if_store_id_collapses(user_id)
        return await self._post("/api/v1/memory/forget", {"user_id": user_id}, model=model)

    # ----- internal -----

    async def _post(self, path: str, body: dict, model: Optional[str] = None,
                    idempotency_key: Optional[str] = None) -> dict:
        return await self._request("POST", path, json_body=body, model=model,
                                   idempotency_key=idempotency_key)

    async def _request_bytes(self, path: str, body: dict, *, model: Optional[str] = None) -> tuple[bytes, str]:
        """One request that answers with BYTES rather than JSON — see ``Client._request_bytes``."""
        status, headers, data = await self._call("POST", path, json_body=body, model=model, reads=True)
        if not data:
            raise WosError(status, "empty image body — the service returned no bytes")
        return data, headers.get("content-type", "application/octet-stream")

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
        model: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        removes: Optional[bool] = None,
    ) -> dict:
        status, headers, payload = await self._call(method, path, json_body=json_body, params=params,
                                                    model=model, idempotency_key=idempotency_key,
                                                    removes=removes)
        data = _parse_ok(status, payload.decode("utf-8", "replace"))
        # Only when absent — see the sync client for why.
        if headers.get("Idempotent-Replayed") == "true" and "replayed" not in data:
            data["replayed"] = True
        return data

    def _http_for_this_loop(self) -> Any:
        """The shared httpx client, once it is clear it can serve a call on this loop.

        Refused before anything is sent: an httpx client belongs to the event loop it first
        ran on, and a closed one cannot send. In a forked child the connections belong to
        the parent, so the child gets an httpx client of its own.
        """
        t = self._transport
        if t.closed:
            raise APIConnectionError(
                0, "this AsyncClient was closed (aclose() or the end of `async with`); create a new one"
            )
        if t.pid != os.getpid():
            if t.factory is None:
                raise APIConnectionError(
                    0, "this AsyncClient was created in another process (before fork()); create one in this process"
                )
            # The inherited client is dropped without aclose(): closing it could end
            # connections the parent still uses.
            t.http, t.loop, t.pid = t.factory(), None, os.getpid()
        loop = _asyncio_loop()
        if loop is None:
            # Another async library (trio): there is no asyncio loop to bind to.
            return t.http
        if t.loop is None:
            t.loop = _loop_ref(loop)
        elif t.loop() is not loop:
            raise APIConnectionError(
                0,
                "this AsyncClient belongs to the event loop it first ran on. Create one inside "
                "each asyncio.run() (or per event loop) instead of sharing it across loops.",
            )
        return t.http

    async def _call(self, method: str, path: str, *, json_body: Optional[dict] = None,
                    params: Optional[dict] = None, model: Optional[str] = None,
                    idempotency_key: Optional[str] = None, removes: Optional[bool] = None,
                    reads: bool = False) -> tuple:
        """One call with its retries — see ``Client._call``."""
        http = self._http_for_this_loop()
        eff_model = _check_model(model) if model else self._model
        headers = {"X-WOS-Model": eff_model} if eff_model else {}
        idem = _idem_headers(idempotency_key)
        if idem:
            headers.update(idem)
        data = _encode_json(json_body)
        url = f"{self._base}{path}"
        call = _Call(method, path, self._retries, self._deadline, removes=removes, reads=reads)
        for attempt in range(call.attempts):
            call.attempt = attempt
            clock = call.clock(self._timeout)
            start = time.monotonic()
            got = await self._attempt(http, method, url, data, params, headers or None, clock, call)
            # The sleeps below run after the response is closed, so a backoff never holds
            # a pooled connection.
            if isinstance(got, _Failure):
                await _async_sleep(call.after_failure(got, clock))
                continue
            status, rheaders, payload = got
            self._rate_limit = _parse_rate_limit(rheaders) or self._rate_limit
            _logger.debug("%s -> %d in %.0fms (attempt %d/%d)", call.where, status,
                          (time.monotonic() - start) * 1000, attempt + 1, call.attempts)
            if 200 <= status < 300:
                return got
            await _async_sleep(call.after_status(status, rheaders, payload))
        raise RuntimeError("retries exhausted")  # unreachable: the last attempt returns or raises

    async def _attempt(self, http: Any, method: str, url: str, data: Optional[bytes],
                       params: Optional[dict], headers: Optional[dict], clock: _Clock,
                       call: _Call) -> Any:
        """One attempt, bounded in wall-clock time: httpx times out one read at a time, so
        a response that trickles in would otherwise never end."""
        head: list = []
        events: list = []  # httpx's progress through the request, from its trace extension
        t = self._transport

        async def trace(event: str, info: Any) -> None:
            events.append(event)
            t.traces = True

        exchange = self._exchange(http, method, url, data, params, headers, clock, call, head, trace)
        if _asyncio_loop() is not None:
            try:
                return await asyncio.wait_for(exchange, clock.left())
            except asyncio.TimeoutError:
                pass
        else:
            import anyio  # httpx depends on it

            with anyio.move_on_after(clock.left()):
                return await exchange
        if head and head[0] >= 400:
            # On an error status the body is only the message: the status stands.
            call.unread = clock.error().message
            return head[0], head[1], b""
        sent = any(e.endswith("send_request_headers.started") for e in events)
        # An httpx client passed in may not report progress; then the request may have
        # left, unless it has reported progress before.
        if not sent and (events or t.factory is not None or t.traces):
            # Waiting for a pooled connection, connecting or in the TLS handshake: the
            # request never left, so the connect-failure rules apply. Named as httpx
            # names the same timeout.
            why = "ConnectTimeout" if events else "PoolTimeout"
            return _Failure(f"{why} (timed out)", never_sent=True)
        return _Failure("timed out", timed_out=True)

    async def _exchange(self, http: Any, method: str, url: str, data: Optional[bytes],
                        params: Optional[dict], headers: Optional[dict], clock: _Clock,
                        call: _Call, head: list, trace: Any) -> Any:
        """Send and read one response: ``(status, headers, body)``, or a ``_Failure``.
        ``head`` receives the status and headers as soon as they arrive.
        Nothing raises from inside an ``except`` block, so no error is chained to httpx's."""
        httpx = self._httpx
        left = max(0.001, clock.left())
        try:
            async with http.stream(
                method, url, content=data, params=params, headers=headers,
                # Per request (not baked into the shared client) so with_timeout() clones work.
                timeout=httpx.Timeout(left, connect=min(10.0, left)),
                extensions={"trace": trace},
            ) as r:
                status, rheaders = r.status_code, r.headers
                head[:] = [status, rheaders]
                if 300 <= status < 400:
                    return status, rheaders, b""
                try:
                    if call.retries_regardless(status, rheaders):
                        wait = max(0.001, min(_RETRYABLE_BODY_WAIT, clock.left()))
                        payload = await _within(_aread_body(r), wait)
                        if payload is None:
                            payload = b""
                            call.unread = (f"not received within {_RETRYABLE_BODY_WAIT:g}s"
                                           if wait >= _RETRYABLE_BODY_WAIT else clock.error().message)
                    else:
                        payload = await _aread_body(r)
                except WosError as e:
                    # The size cap or an encoding refused. On an error status the status
                    # is the answer.
                    if status < 400:
                        raise
                    payload = b""
                    call.unread = e.message
                except _Undecodable:
                    # Damaged on the way, like a body that stopped arriving.
                    if status < 400:
                        return _Failure("DecodingError (could not decode the response body)",
                                        dropped=True)
                    payload = b""
                    call.unread = "DecodingError (could not decode the response body)"
                except httpx.HTTPError as e:
                    if status < 400:
                        return self._failure(e)
                    payload = b""
                    call.unread = self._failure(e).reason
                return status, rheaders, payload
        except httpx.HTTPError as e:
            return self._failure(e)

    def _failure(self, e: BaseException) -> _Failure:
        httpx = self._httpx
        # Connect-level, or no pooled connection in time: the request never left. A
        # read/write error or a protocol error broke the connection after it did. A
        # timeout is neither.
        never = isinstance(e, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout))
        timed = not never and isinstance(e, httpx.TimeoutException)
        dropped = isinstance(e, (httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError))
        return _Failure(_transport_reason(e), never_sent=never, timed_out=timed, dropped=dropped)
