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
it. Set a default on the client, override per call:

    mem = Client(api_key="wos-...", model="tablet-1")
    mem.recall("...", user_id="alice", model="tablet-1")   # this call only

Reliability: every call retries transient failures with exponential backoff +
jitter, honoring ``Retry-After`` — 429 always; 502/503 and connection errors only
when a retry can never double-process a write (idempotent calls, or a failure at
connect time). Tune with ``Client(retries=...)`` (0 disables) and ``timeout=``,
or per call site via the ``with_timeout()`` / ``with_retries()`` clones.

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
import json
import logging
import threading
import os
import platform
import random
import re
import ssl
import sys
import time
import warnings
from typing import Any, Optional
from urllib.parse import urlsplit

import requests

__version__ = "2.2.38"

# Without this, `from wontopos import *` also bound os, sys, json, re, time,
# random, logging, platform, ssl and requests in the caller's namespace, and they
# cluttered dir(wontopos) / autocomplete. Only the public surface is exported.
__all__ = [
    "Client", "AsyncClient", "WME",
    "WosError", "APIConnectionError", "AuthenticationError", "BadRequestError",
    "ConflictError", "NotFoundError", "PaymentRequiredError",
    "PermissionDeniedError", "RateLimitError", "ServerError",
    "DEFAULT_BASE_URL", "DEFAULT_MODEL", "__version__",
]

DEFAULT_BASE_URL = "https://api.wontopos.com"
# The engine every call uses unless the caller names another.
#
# Tablet 2 costs the same per token as Tablet 1 and is the one that
# serves images, re-ask passes (``verify``), and ``self_memories``, so a caller who
# names nothing gets the engine that can answer the most. All models read the same
# memory, so switching is a header, not a migration. Pin an older one explicitly
# with ``Client(key, model="tablet-1")`` or per call with ``model=``.
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
# 429 = rate-limited BEFORE processing, so retrying is always safe (no write
# landed, nothing billed). 502/503 are ambiguous for a write: the gateway may
# have returned them AFTER the backend already processed (and billed) the
# request, so retrying a POST could double-store / double-bill. We therefore
# retry 502/503 only for idempotent methods.
_RETRY_ALWAYS = (429,)
_RETRY_IF_IDEMPOTENT = (502, 503)
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS"})
_LOOPBACK_HOSTS = ("localhost", "127.0.0.1", "::1", "0.0.0.0")
# Refuse to buffer absurd responses (real ones are a few KB) — protects the
# process if a custom base_url points somewhere broken or hostile.
_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
# Backstop for the paging helpers. The cursor-repeat guard catches a server that
# returns the SAME cursor; it does not catch one that mints a FRESH cursor every page
# forever, so the walk needs a ceiling as well.
#
# 20,000 pages is two million memories at 100 per page — past any real store, and
# reached in minutes. The previous ceiling of 1,000,000 was not a stop at all: a
# hundred million memories, hours of requests, every one of them billed.
#
# Reaching it raises rather than ending the walk. A truncated list looks exactly like
# a complete one, and the caller writes it to a file believing they have the store.
_MAX_PAGES = 20_000
# What the API accepts as an ``Idempotency-Key``. Checked client-side so a bad key
# fails before the request instead of coming back as a 400 mid-retry.
# `fullmatch`, not `match`: in Python `$` also matches just BEFORE a trailing
# newline, so `match()` accepted "key\n" — which then died inside http.client as
# `ValueError: Invalid header value`, the opaque failure this check exists to
# prevent. The TypeScript and Rust clients rejected it all along.
_IDEMPOTENCY_KEY_RE = re.compile(r"[A-Za-z0-9._:\-]{1,128}")



# ── The boundaries that go wrong quietly, said out loud ─────────────────────

def _normalize_store_id(sid: str) -> str:
    """The transformation the API actually applies to a store id."""
    return re.sub(r"[^a-z0-9_]", "_", sid.lower())


# Ids already warned about, so each one is reported once. A dict is used as an
# ORDERED set (insertion order) purely so the oldest entry can be evicted.
#
# This has to be bounded. The warning below fires on ids that fold, and the shape
# it exists to catch is an email — which the SDK's own documented pattern ("one
# store per end user") means one entry per user, held for the life of the process.
# The cap keeps that bounded. Nothing is lost by it: the set exists so a developer
# is told once, and past a few hundred distinct ids that message has landed.
_WARNED_STORE_IDS_MAX = 1024
#: One lock for both process-global warn caches — see _warn_if_store_id_collapses.
_warn_lock = threading.Lock()
_warned_store_ids: "dict" = {}


class _Backoff(Exception):
    """Internal: leave an open streaming response before sleeping on a retry.

    Not an error anyone sees. The async client used to `await asyncio.sleep(...)` inside
    `async with client.stream(...)`, which holds a pooled connection for the whole
    backoff; a 429 burst then parked the entire httpx pool and every other request in
    the process failed PoolTimeout.
    """


def _assert_usable_store_id(user_id: Any) -> None:
    """A store id is usable when it is a non-blank string. Anything else is a bug at the
    call site, not a value to fall back from.

    Not just blank: any value that is not a usable store id. An integer primary key is
    the common one. ``user_id=0`` is falsy, so without this check it reaches the default
    store, and any other int reaches the warning helper and dies there with "'int' object
    has no attribute 'lower'". Neither says what was wrong, and one of them wrote a
    customer's memories somewhere else.
    """
    if not isinstance(user_id, str) or not user_id.strip():
        raise ValueError(
            f"user_id must be a non-blank string; got {user_id!r}. Omit it to use the client's default "
            "store, or pass a real store id — anything else would silently write into the default store."
        )


def _warn_if_store_id_collapses(sid: str) -> None:
    """The API lowercases a store id and rewrites anything outside ``[a-z0-9_]`` to ``_``,
    so ``Alice.Smith`` · ``alice-smith`` · ``alice_smith`` are **the same store**.

    The common way to use this SDK is one store per end user, which makes that a data
    exposure: ``bob.lee@x.com`` and ``bob-lee@x.com`` share every memory between them and
    nothing in the response says so (the notice only appears when ``create_store``
    actually creates one, and an app reaching for a store that already exists never gets
    to see it).

    Refusing is not an option — the API accepts these ids and code may already depend on
    them. So say it plainly, once, at the moment it happens."""
    if not sid:
        return
    normalized = _normalize_store_id(sid)
    if normalized == sid:
        return
    # Under a lock. The set is process-global and every request touches it, so a
    # threaded app can run membership, insert, iterate and evict at the same time.
    # Unlocked, eviction raises KeyError and `next(iter(...))` raises
    # `RuntimeError: dictionary changed size during iteration`.
    # A warning helper must never be the thing that raises inside a customer's request,
    # and this one raised before any network call was made.
    with _warn_lock:
        if sid in _warned_store_ids:
            return
        _warned_store_ids[sid] = None
        # Bounded: drop the oldest rather than stop recording, so a collision that
        # first appears late still gets its one warning.
        while len(_warned_store_ids) > _WARNED_STORE_IDS_MAX:
            _warned_store_ids.pop(next(iter(_warned_store_ids)))
    logging.getLogger("wontopos").warning(
        "store id %r is stored as %r (lowercased, and anything outside [a-z0-9_] becomes '_'). "
        "Ids that differ only by case or punctuation share ONE store and therefore one set of "
        "memories — if these ids come from your end users, normalize them yourself first so two "
        "people can never collide.",
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
            # 2.2.27 capped the store-id warn set and left this sibling unbounded. An app
            # forwarding user-supplied filter keys grows it forever, one entry per typo.
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


def _check_count(limit: int, name: str = "limit") -> None:
    """The 5-to-20 count shared by ``search`` and ``recall``, refused out of range
    rather than quietly adjusted.

    The service has refused anything else from the start, because asking for 20 and
    silently getting 10 reads as "that is all there is". Search had no contract at
    all: the SDKs sent whatever they were given, the MCP server allowed 1 to 60, and
    the service quietly capped at 50 with no floor. Four surfaces, four answers, and
    the caller could not tell which they got.

    ★ ``recall`` said it enforced this and did not. Its docstring promised "out of
    range is refused, not clamped" while the value went straight to the wire in all
    three SDKs, so ``limit=500`` travelled to the engine and died there. A docstring
    that describes a guard is not a guard — and this is the shape of defect a review
    of one diff can never see, because the promise and the missing code were written
    months apart.

    ``ValueError``, like every other argument check in this file. NOT ``WosError``:
    nothing was sent, and ``WosError`` with status 0 is ``APIConnectionError`` — "the
    request never got a response". Reusing that status for a typo told a caller
    branching on ``e.status == 0`` to retry a bad argument forever.

    Refusing rather than clamping is the same decision as 2.2.35's, where a Rust
    ``limit`` of 0 had been rewritten to 10 and the caller was handed ten memories
    they had not asked for, and the bill for them.
    """
    if not isinstance(limit, int) or isinstance(limit, bool):
        raise ValueError(f"{name} must be an int, got {type(limit).__name__}")
    if limit < SEARCH_LIMIT_MIN or limit > SEARCH_LIMIT_MAX:
        raise ValueError(
            f"{name} must be between {SEARCH_LIMIT_MIN} and {SEARCH_LIMIT_MAX}, got {limit}. "
            "Out of range is refused rather than adjusted, so a short answer always "
            "means the store was short."
        )


def _check_context_limit(n: int) -> None:
    """``recall``'s ``context_limit``, 0 to 20. Same reason as the count above: the
    docstring promises out-of-range is refused, and a promise the client does not
    keep is worse than no promise — the caller reads it, sends 50, and the failure
    arrives from the service with no hint the SDK knew all along.

    0 is a real answer ("attach none"), not a missing value, so it must pass.
    """
    if not isinstance(n, int) or isinstance(n, bool):
        raise ValueError(f"context_limit must be an int, got {type(n).__name__}")
    if n < CONTEXT_LIMIT_MIN or n > CONTEXT_LIMIT_MAX:
        raise ValueError(
            f"context_limit must be between {CONTEXT_LIMIT_MIN} and {CONTEXT_LIMIT_MAX}, got {n}."
        )


def _recall_body(store_id: str, query: str, form: Optional[str], tz: Optional[int],
                 limit: Optional[int], context_limit: Optional[int]) -> dict:
    """``recall``'s body, built once for the sync and async clients.

    Same reason ``_search_body`` exists: a guard written where the body is assembled
    lives in every caller, and a guard copied into each caller lives in all of them
    until someone adds a fifth. That is how this one went missing in the first place.
    """
    body = _form_body({"user_id": store_id, "query": query}, form, tz)
    if limit is not None:
        _check_count(limit)
        body["limit"] = limit
    if context_limit is not None:
        _check_context_limit(context_limit)
        body["context_limit"] = context_limit
    return body


_KNOWN_SEARCH_KEYS = frozenset(
    {"cache_control", "speaker", "filters", "verify", "max_images", "extra"}
)
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
                f"{k!r} is set by the call, not by options — pass it as an argument. "
                "An app forwarding untrusted input as options cannot steer the store, "
                "the query or the count, and this says so rather than dropping it silently."
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


def _search_body(store_id: str, query: str, limit: int, opts: dict,
                 verify: Optional[int] = None, max_images: Optional[int] = None) -> dict:
    """The /memory/search request body, built one way for all four search methods.

    sync/async x search/search_self is four call sites for one body. Writing the same
    merge four times is how a guard ends up living in three of them — the exact shape
    of bug this SDK has shipped before. One function, four callers.

    Reserved fields win over ``opts``: an app forwarding untrusted input as ``opts``
    must not be able to override the store, the query, or the limit. ``verify`` and
    ``max_images`` are set last for the same reason — a stray copy inside ``opts``
    cannot beat the named argument the caller actually wrote.
    """
    _check_search_opts(opts)
    _warn_on_unknown_filters(opts.get("filters"))
    _check_count(limit)
    extra = opts.get("extra") or {}
    known = {k: v for k, v in opts.items() if k != "extra"}
    body = {**extra, **known, "user_id": store_id, "query": query, "max_results": limit}
    if verify is not None:
        body["verify"] = verify
    if max_images is not None:
        body["max_images"] = max_images
    return body

def _reset_warning_state() -> None:
    """For tests — each warning is emitted once per process, globally."""
    _warned_store_ids.clear()
    _warned_filter_keys.clear()


def _normalize_image(image: Any) -> dict:
    """Put an image into the shape the API expects, checking only what cannot change.

    Deliberately NOT checked here: the byte ceiling. That is a server setting (``/health``
    reports it as ``memory.images.max_bytes``), so a number baked into the SDK would drift
    the first time the service is reconfigured and would refuse an image the service
    would have taken.

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
    # "data:image/png;base64," travels as part of the base64. The engine answers 400
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
    # ``isspace()`` is False for NUL, 0x01, DEL — which then travelled into the
    # X-API-Key header. This file already had ``_HEADER_CTL_RE`` and applied it to
    # ``memory_id`` and ``speaker``, values that ride in the JSON body, but not to the
    # one value that actually becomes a header.
    if _HEADER_CTL_RE.search(key):
        raise ValueError("api_key contains a control character - check for a paste error")
    # Keys are ASCII by construction, and a header value is latin-1 on the wire. A key
    # pasted from a rich-text doc, Slack or a PDF has had its hyphen turned into an en
    # dash, and that used to travel all the way into http.client and die there as
    # UnicodeEncodeError — not a WosError, so `except WosError` around the call missed
    # it, and the message never said "api_key". The test is ASCII rather than latin-1:
    # 'é' encodes as latin-1 and so would have sailed through to a mystery 401.
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
# `mem.get(memory_id)` is the call people actually write — and it landed the id in
# `user_id`, leaving memory_id empty, so it raised. Recover that case: a lone
# argument shaped like a memory id (a UUID, which is what the service mints) can
# only have been meant as the memory id, because the call raises either way
# without one. Never changes a call that already works.
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def _split_id_args(user_id: Optional[str], memory_id: str) -> tuple[Optional[str], str]:
    if not memory_id and isinstance(user_id, str) and _UUID_RE.match(user_id):
        return None, user_id
    return user_id, memory_id


def _merge_results(resp: Any) -> list:
    """Every memory a search returned, from both fields, as one list.

    Some models answer with the assistant's own words in `self_memories`, not
    repeated in `memories`. `search()` used to return `memories` alone, so an
    assistant turn stored with `add_turn` was missing from its results on those
    models while the same query returned it on others — upgrading made search
    return LESS, and what went missing had already been retrieved and paid for.

    Both fields are returned here, de-duplicated by id, each memory carrying its
    `speaker` so a caller can still tell who said what. Callers who want them
    kept apart use `search_self()`, which is what that method is for.
    """
    if not isinstance(resp, dict):
        return []
    main = _as_records(resp.get("memories"))
    mine = _as_records(resp.get("self_memories"))
    if not mine:
        return main
    seen = {m["id"] for m in main if isinstance(m.get("id"), str)}
    return main + [m for m in mine if not (isinstance(m.get("id"), str) and m["id"] in seen)]


def _form_body(body: dict, form: Optional[str], tz: Optional[int]) -> dict:
    """Attach a delivery-form override (``"memoir"``/``"archive"``) and timezone to a
    request body, the same way search passes them. On Scroll 1.2+ this renders every
    returned memory's time in that form. Server validates the form (400 on unknown)."""
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
    """A single-memory call must name a memory.

    ``delete`` and ``get`` already refused a blank or non-string id; the image and
    lineage calls did not, so a missing id travelled as ``""`` and the failure
    surfaced from the server (or, for a non-string, as ``'int' object has no
    attribute 'search'`` from deep inside the header check) rather than as a
    sentence about the argument.
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


_TLS_CONTEXT: Optional[ssl.SSLContext] = None


def _tls_context() -> ssl.SSLContext:
    """System trust store, TLS 1.2 floor, hostname verification — and no knob to
    turn any of it off. Built ONCE and shared: loading the system CA store is a
    few ms, and an SSLContext is safe to reuse across sessions and threads, so a
    fresh one per client just re-pays that cost for nothing."""
    global _TLS_CONTEXT
    if _TLS_CONTEXT is None:
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        _TLS_CONTEXT = ctx
    return _TLS_CONTEXT


class _TLSAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args: Any, **kwargs: Any):
        kwargs["ssl_context"] = _tls_context()
        return super().init_poolmanager(*args, **kwargs)


def _warn_if_plain_http(base: str) -> None:
    # An API key on plain HTTP travels readable by anyone on the path. Loopback
    # is fine (local dev, or a proxy on the same box); anything else gets a
    # warning, not an error, so private-network gateways keep working.
    parts = urlsplit(base)
    host = (parts.hostname or "").lower()
    # Scheme compare is case-insensitive: HTTP stacks normalize scheme case, so
    # `HTTP://` connects in plaintext just like `http://` — warn on both.
    if parts.scheme.lower() == "http" and host not in _LOOPBACK_HOSTS:
        warnings.warn(
            "wontopos: base_url uses plain HTTP on a non-local host, so the API key "
            "travels unencrypted. Use https://.",
            stacklevel=3,
        )


def _never_sent(exc: requests.exceptions.ConnectionError) -> bool:
    """True when the failure happened while ESTABLISHING the connection (DNS,
    refused, connect timeout) — the request never reached the server, so a retry
    can't double-process a write. A mid-stream drop ("Connection aborted") is
    ambiguous: the server may already have processed (and billed) the request."""
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


def _backoff(attempt: int, retry_after: Optional[str] = None) -> float:
    """Seconds to sleep before retry ``attempt`` (0-based). Honors Retry-After
    in BOTH RFC 9110 forms: delta-seconds and an HTTP-date. Capped at 30s."""
    if retry_after:
        try:
            return min(30.0, max(0.0, float(retry_after)))
        except ValueError:
            pass
        # HTTP-date form, e.g. "Wed, 21 Oct 2015 07:28:00 GMT" → delta from now.
        try:
            from datetime import datetime, timezone
            from email.utils import parsedate_to_datetime

            dt = parsedate_to_datetime(retry_after)
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                delta = (dt - datetime.now(timezone.utc)).total_seconds()
                return min(30.0, max(0.0, delta))
        except (TypeError, ValueError):
            pass
    return min(8.0, 0.5 * (2**attempt)) + random.random() * 0.25


_MAX_ERR_MSG = 4096  # a hostile server's error body shouldn't become a giant exception/log line


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
    except ValueError as e:
        raise WosError(status, f"invalid JSON in response: {e}") from e
    if not isinstance(data, dict):
        raise WosError(status, f"expected a JSON object in the response, got {type(data).__name__}")
    return data


def _parse_error(status: int, text: str) -> "WosError":
    # Server may return either:
    #   Anthropic-style envelope: {"type":"error","error":{"type":...,"message":...,"request_id":...}}
    #   Spec simple:             {"error":"reason string"}
    # Fall back to raw text. Cap the fallback so a 64MB error body (within the
    # response cap) can't become a 64MB exception string.
    text = text if len(text) <= _MAX_ERR_MSG else text[:_MAX_ERR_MSG] + "…(truncated)"
    msg, request_id = text, None
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            err = data.get("error")
            if isinstance(err, dict):
                msg = err.get("message") or err.get("type") or msg
                rid = err.get("request_id")
                request_id = rid if isinstance(rid, str) else None
            elif isinstance(err, str):
                msg = err
            elif "message" in data:
                msg = data["message"]
    except Exception:
        pass
    return _make_error(status, msg, request_id=request_id)


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
    one — include it when contacting support.
    """

    def __init__(self, status: int, message: str, request_id: Optional[str] = None):
        self.status = status
        self.message = message
        self.request_id = request_id
        suffix = f" (request_id: {request_id})" if request_id else ""
        super().__init__(f"[{status}] {message}{suffix}")


# Typed error subclasses so callers can branch on the failure instead of reading
# ``.status`` by hand — ``except RateLimitError`` / ``except AuthenticationError``.
# Every one is a WosError, so ``except WosError`` still catches them all.
class APIConnectionError(WosError):
    """The request never got a response (DNS/TLS/timeout/connection). ``status`` is 0."""


class BadRequestError(WosError):
    """400 — the request was malformed (bad arguments)."""


class AuthenticationError(WosError):
    """401 — the API key is missing, wrong, or revoked."""


class PaymentRequiredError(WosError):
    """402 — no card on file or the balance is depleted. Top up to continue."""


class PermissionDeniedError(WosError):
    """403 — the key/model isn't allowed to do this."""


class NotFoundError(WosError):
    """404 — the store or resource doesn't exist (create the store first)."""


class ConflictError(WosError):
    """409 — a concurrent write to the same store. Retry."""


class RateLimitError(WosError):
    """429 — too many requests. Back off and retry (the client already retries these)."""


class ServerError(WosError):
    """5xx — the service failed. Safe to retry."""


_STATUS_ERRORS = {
    400: BadRequestError,
    401: AuthenticationError,
    402: PaymentRequiredError,
    403: PermissionDeniedError,
    404: NotFoundError,
    409: ConflictError,
    429: RateLimitError,
}


def _deadline_at(deadline: Optional[float]) -> Optional[float]:
    """When this call's budget runs out, or ``None`` when it has none.

    Computed once per call, never per attempt — a budget recomputed each attempt is
    not a budget, it is the per-attempt timeout wearing a different name.
    """
    return None if deadline is None else time.monotonic() + deadline


def _attempt_budget(timeout: float, deadline_at: Optional[float], deadline: Optional[float]) -> float:
    """How long THIS attempt may take: the per-attempt timeout, or whatever is left of
    the overall budget, whichever is smaller.

    Raises when the budget is already gone, so no socket is opened that there is no
    time to use.
    """
    if deadline_at is None:
        return timeout
    left = deadline_at - time.monotonic()
    if left <= 0:
        raise APIConnectionError(0, f"deadline of {deadline}s exhausted")
    return min(timeout, left)


def _timeout_pair(timeout: float, deadline_at: Optional[float],
                  deadline: Optional[float]) -> tuple[float, float]:
    """``requests``' (connect, read) pair for one attempt, inside the budget."""
    t = _attempt_budget(timeout, deadline_at, deadline)
    return (min(10.0, t), t)


def _sleep_within(secs: float, deadline_at: Optional[float],
                  deadline: Optional[float] = None) -> float:
    """A backoff that never sleeps past the budget — sleeping through the deadline
    spends the caller's whole allowance on waiting.

    Raises when the wait does not fit. Clamping it to what is left and retrying anyway
    is the same answer as having no deadline: the call still spends the whole
    allowance, and the attempt it buys has nothing left to finish in. ``_attempt_budget``
    would refuse that attempt one line later, so all the clamped sleep bought was the
    delay before saying so.
    """
    if deadline_at is None:
        return secs
    left = deadline_at - time.monotonic()
    if secs > left:
        raise APIConnectionError(0, f"deadline of {deadline}s exhausted")
    return secs


def _make_error(status: int, message: str, request_id: Optional[str] = None) -> WosError:
    if status == 0:
        cls: type = APIConnectionError
    elif status in _STATUS_ERRORS:
        cls = _STATUS_ERRORS[status]
    elif 500 <= status < 600:
        cls = ServerError
    else:
        cls = WosError
    return cls(status, message, request_id=request_id)


class Client:
    """Wontopos memory client.

    Store and recall memories, isolated per ``user_id``.

        mem = Client(api_key="wos-...")
        mem.add("she prefers tea over coffee", user_id="alice")
        hits = mem.search("what does alice drink?", user_id="alice")

    The API key picks *which memory* (your account). ``model`` picks *which engine*
    reads it. All models share one memory, so you can store
    with one and recall with another. Set a default on the client, override per call:

        mem = Client(api_key="wos-...")                        # tablet-2
        mem.recall("...", user_id="alice")                     # tablet-2
        mem.recall("...", user_id="alice", model="tablet-1")   # this call only

    Args:
        api_key:  your Wontopos API key (sent as ``X-API-Key``).
        base_url: API base URL (defaults to the hosted service).
        timeout:  per-request timeout in seconds (applies to each retry attempt).
        model:    default model for every call (sent as ``X-WOS-Model``).
                  See ``list_models()`` for what's available.
        user_id:  default store for every call. Pass ``user_id=`` on a single
                  call to override it. Defaults to the account's ``default`` store.
        retries:  how many times to retry transient failures before raising —
                  429 always; 502/503 and connection errors only when a retry
                  can never double-process a write. 0 disables retries.
        deadline: a TOTAL budget for one call, in seconds, across every attempt.
                  ``timeout`` bounds one attempt; at the defaults (30s, two retries)
                  a call can hold for 30 + backoff + 30 + backoff + 30, over a
                  minute, and a request handler awaiting it had no way to say how
                  long it actually had. ``None`` means no overall budget.
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
        # ``max_retries`` is an alias: the TypeScript SDK names this option
        # ``maxRetries`` and the three SDKs ship as "the same surface", so code
        # ported between them would otherwise raise TypeError here instead of just
        # working. Both names are accepted; an explicit ``retries`` wins.
        #
        # The default is None rather than 2 so that "the caller said 2" and "the
        # caller said nothing" are different states. Comparing against 2 could not
        # tell them apart, so ``Client(key, retries=2, max_retries=5)`` retried five
        # times against an explicit instruction to retry twice.
        if retries is None:
            retries = 2 if max_retries is None else max_retries
        self._api_key = _clean_key(api_key)
        self._base = base_url.rstrip("/")
        # A non-positive/NaN timeout would fail every request instantly — fall
        # back to the default instead (same guard as retries below).
        self._timeout = timeout if isinstance(timeout, (int, float)) and timeout > 0 else 30.0
        # A non-positive budget would fail every call before it started — that reads as
        # "no budget", the same fallback the timeout above takes.
        self._deadline = deadline if isinstance(deadline, (int, float)) and deadline > 0 else None
        self._model = _check_model(model)
        # The store every call uses unless one passes user_id=. "default" is the
        # account's built-in store, so the zero-config path needs no create call.
        # The same guard _uid applies per call. It used to live only there, so the
        # constructor and with_user() — the documented per-tenant pattern — walked past
        # it: user_id=0 and user_id="" became the shared `default` store with no warning,
        # while add(text, user_id=0) raised. A destination that depends on WHICH door the
        # id came through is the worst kind of silent redirect.
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
            self._session.mount("https://", _TLSAdapter())
            self._session.headers.update({
                "X-API-Key": self._api_key,
                "Content-Type": "application/json",
                "User-Agent": _USER_AGENT,
            })
        _warn_if_plain_http(self._base)

    @classmethod
    def from_env(cls, **kwargs: Any) -> "Client":
        """A client whose key comes from ``WONTOPOS_API_KEY`` (or ``WOS_API_KEY``) —
        keeps keys out of source code. Other constructor args pass through:

            mem = Client.from_env(user_id="alice")
        """
        return cls(_env_key(), **kwargs)

    def __repr__(self) -> str:  # never show the key — repr ends up in logs
        return (
            f"Client(base_url={self._base!r}, model={self._model!r}, "
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
        call can hold for 30 + backoff + 30 + backoff + 30, over a minute, and a
        request handler awaiting it had no way to say how long it actually had.
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
        """A client with a different per-request timeout in seconds (everything else kept).
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

            mem.add("moved to Berlin", user_id="alice", event_date="2026-03-01T00:00:00Z")  # event_date is RFC3339
            mem.add("I promised the report by Friday", user_id="alice", speaker="me")   # the assistant's own words
            mem.add("Bob said the deadline moved", user_id="alice", speaker="Bob")      # a person (up to 50 per store)

        ``user_id`` is optional — omit it to use the client's default store.

        ``idempotency_key`` makes repeating THIS EXACT write safe: the API replays the
        first response instead of storing again (10 minutes), and answers 422 if the same
        key arrives with a different body. Use it when the retry is yours — a job that died
        and was re-run, a queue that redelivers. The SDK retries a write on exactly one
        status: 429, which the service answers before it processes anything, so nothing was
        stored. It never retries a write on 502 / 503 or a dropped body, where the request
        may already have been stored and billed — without a key it cannot know whether that
        first attempt landed.

        Derive the key from the thing being stored (``f"import:{row.id}"``), never a
        constant: reusing one key for two different writes replays the first and the second
        is silently lost. It is declared as a keyword here for a reason — left to
        ``**metadata`` it would have been stored as a metadata field instead of sent.

        ``metadata=`` also works, and means the same as the positional argument the
        TypeScript and Rust SDKs take::

            mem.add("...", "alice", metadata={"speaker": "me"})   # same as speaker="me"

        Declared explicitly so it does not land in ``**metadata`` under the key
        ``"metadata"``, which would store a nested dict and leave the speaker tag with
        nothing to apply to. Code ported from the other two SDKs writes it this
        way naturally, which is exactly when it broke. Both forms merge; a loose keyword
        wins on a conflict, because it is the more specific thing the caller just typed.

        ``image=`` attaches an image (Tablet 2 and newer)::

            mem.add("at the beach", image={"data": b64})   # caption + image
            mem.add("", image={"data": b64})               # the image IS the memory

        ``{"data": <base64>}`` is the only required part; a ``data:image/...;base64,``
        prefix is accepted and stripped. Optional: ``reference`` (where YOUR copy of the
        original lives — stored as a string, never fetched by us) and ``taken_at``
        (RFC3339, usually from EXIF; it fills ``event_date`` when that is empty, so the
        memory sorts by when the image was TAKEN rather than when it was uploaded).

        What we keep is NOT your original. Over 1568px on the long edge the picture is
        downscaled to 1568 on the way in, and downscaling means re-encoding: lossless
        formats are written as WebP, so a PNG comes back from ``get_image`` as
        ``image/webp``; JPEG stays JPEG. Under 1568px the bytes are untouched. This is a
        memory engine, not a photo host — put the full-resolution file somewhere of your
        own and its URL in ``reference``.
        """
        md = {**metadata, **extra} if metadata else extra
        body: dict = {"user_id": self._uid(user_id), "content": content, "metadata": md}
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
        self, content: str, user_id: Optional[str] = None, category: str = "general",
        timestamp: Optional[str] = None, *, model: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Bulk-ingest a large blob of text in one call.

        Use this to backfill long histories in one call. ``timestamp`` is an RFC3339
        string. ``user_id`` is optional — omit it to use the client's default store.
        """
        body: dict[str, Any] = {"user_id": self._uid(user_id), "content": content, "category": category}
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
        self, query: str, user_id: Optional[str] = None, limit: int = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> list[dict]:
        """Search a store's memories. Returns them most relevant first.

        The list ARRIVES in that order — do not re-sort it. ``similarity`` on each
        memory is a raw closeness score, not the ranking key: what produces the order
        is internal and is not returned, so sorting by ``similarity`` overrides the
        ranking and makes results worse. There is no ``score`` field.

        ``user_id`` is optional — omit it to use the client's default store.

        ``limit`` is 5-20, and out of range is refused rather than clamped: asking for
        50 and silently receiving 20 reads as "that is all there is". The default is 10,
        so a call that passes no count is unaffected. Before 2.2.35 the count was sent
        on unchecked.

        ``limit`` bounds ``memories``, not the returned list. On a model that keeps
        the assistant's own words separate (Scroll 1.2+) those come back as well, so
        the list can hold more than ``limit``. They were retrieved and billed either
        way; dropping them would only hide what you already paid for. Size a prompt
        window on the list you get back, not on ``limit``. :meth:`search_self` hands
        the two back apart.

        ``filters`` chooses what is searched, not what is kept afterwards — a narrow
        filter still returns your full ``limit`` when that many matches sit inside it::

            mem.search("what did we decide", filters={
                "categories": ["work"],
                "event_from": "2026-01-01",   # WHEN IT HAPPENED (metadata.event_date),
                "event_to": "2026-06-30",     # not when it was written
            })

        Accepted keys: ``categories``, ``event_from``, ``event_to``, ``time_from``,
        ``time_to``, ``min_importance``. Filtering behaves identically in every language.
        Unlisted keys are dropped by the API rather
        than rejected — a typo silently widens the search, so spell them exactly.

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
        result that predates a new memory. Accepted on the shared-pool models (Tablet /
        Scroll); a model that does not support it refuses the option rather than
        accepting it and silently doing nothing.

        ``speaker`` recalls one person's words only — ``"me"`` for the assistant's own,
        or a name registered with :meth:`add_speaker`.

        Both are named arguments now. They used to ride in ``**opts``, which merged
        anything unrecognized into the request body, and they were in the README but in
        no docstring — which is what autocomplete and ``help()`` show, so the option
        that cuts the bill by 10× was invisible exactly where a caller writing this line
        would look for it.

        ``verify`` (0–3) asks again after the first answer, up to that many times, and
        each extra pass reaches memories the earlier ones did not. No LLM runs at any
        value. It stops early when
        a pass finds nothing new, and ``verify_used`` in the response says how many ran.
        Each pass is another engine call that can add up to ``limit`` more memories, so
        it costs more; you are billed for what is delivered. Default 0. It helps most on
        questions needing several distinct memories from far apart in the history and
        does little on a single-fact lookup.

        ``max_images`` (0–5) is how many image memories the answer may carry; omit it
        and the service uses 1, ``0`` asks for none. The MCP server defaults its own
        tool to 0 instead, so most searches through it carry no image rows. Out of range
        is refused rather than clamped — silently cutting 5 to 1 would leave you
        believing you got five.

        Both need a capable model and are REFUSED (403) on one without it, instead of
        being accepted and quietly doing nothing.

        A MISSPELLING is refused here, before anything is sent. ``verfy=3`` used to be
        absorbed by ``**opts``, travel to the API, be ignored as an unknown field and
        come back 200 having re-asked nothing — you paid for the search and believed
        verification ran. It now raises ``ValueError`` and names the key you probably
        meant. Pass a genuinely new option under ``extra``.
        """
        # Reserved fields win over **opts: an app that forwards untrusted input as
        # opts must not be able to override the store (user_id), query, or limit.
        body = _search_body(self._uid(user_id), query, limit, opts, verify, max_images)
        r = self._post("/api/v1/memory/search", body, model=model)
        return _merge_results(r)

    def search_full(
        self, query: str, user_id: Optional[str] = None, limit: int = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> dict:
        """Search, and keep every field the answer came with.

        Same request as :meth:`search` — reach for this one when the options make the
        merged list an incomplete answer::

            r = mem.search_full("the day we moved", "alice", max_images=3, verify=2)
            r["images"]        # the photos, which search() drops
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
        self, query: str, user_id: Optional[str] = None, limit: int = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> dict:
        """Search a self-memory model (Scroll 1.2+): both fields from ONE call.

        Returns ``{"memories": [...], "self_memories": [...]}``. ``memories`` is what
        others said and general memories; ``self_memories`` is the assistant's OWN
        words (stored with ``speaker="me"``), kept apart so whoever reads them never
        confuses who said what — never mixed into ``memories``. On a model that does
        not keep them apart (e.g. tablet-1) ``self_memories`` is ``[]``.
        ``user_id`` is optional — omit it to use the client's default store.

            r = mem.with_model("scroll-1.2").search_self("what did I promise Alice?")
            for m in r["memories"]: ...        # partner / general memories
            for m in r["self_memories"]: ...   # the assistant's own words
        """
        body = _search_body(self._uid(user_id), query, limit, opts, verify, max_images)
        r = self._post("/api/v1/memory/search", body, model=model)
        # `or []` on each field: a broken proxy sending null for either yields [], not None.
        return {"memories": _as_records(r.get("memories")), "self_memories": _as_records(r.get("self_memories"))}

    def recall(
        self, query: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
        form: Optional[str] = None, tz: Optional[int] = None,
        limit: Optional[int] = None, context_limit: Optional[int] = None,
    ) -> dict:
        """One-call context for an LLM: short-term turns + long-term matches + surrounding context.

        Returns ``{"short_term": ..., "long_term": ..., "context": ...}``. No extra
        round-trips. ``user_id`` is optional — omit it to use the client's default store.
        ``form`` (``"memoir"``/``"archive"``, Scroll 1.2+) renders each long-term memory's
        time in that form; ``tz`` is your UTC-offset hours for that rendering.

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
        (memoir/archive) on Scroll 1.2+, same as search/recall.
        """
        body = _form_body({"name": name, "user_id": self._uid(user_id), "query": query}, form, tz)
        return self._post("/api/v1/engram/run", body, model=model)

    def history(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """Recent conversation turns (short-term memory). Omit ``user_id`` for the default store."""
        return _as_records(self._post("/api/v1/memory/history", {"user_id": self._uid(user_id)}, model=model).get("turns"))

    def stats(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> dict:
        """Memory counts for a store: ``{total_memories, short_term_turns}``. Omit ``user_id`` for the default."""
        return self._post("/api/v1/memory/stats", {"user_id": self._uid(user_id)}, model=model)

    def get(self, user_id: Optional[str] = None, memory_id: str = "", *, model: Optional[str] = None) -> dict:
        """Fetch ONE memory by id — the text you stored, and its metadata.

        The id is the one ``add``/``store`` or ``list_memories`` returned. Same
        visibility as ``list_memories``: an id from another store, an internal-only id,
        or an invalidated memory raises ``NotFoundError``. Omit
        ``user_id`` (keyword ``memory_id=``) for the default store.

            m = mem.get(memory_id="9b2d8c1e-...")
            print(m["content"], m["is_superseded"])
        """
        user_id, memory_id = _split_id_args(user_id, memory_id)
        # `"   "` is truthy, so the old test passed it through as the id. The four
        # sibling calls go through _require_memory_id, which refuses a blank one and
        # returns it stripped; get did neither, so an id pasted from a log with a stray
        # space was answered here and 404'd in TypeScript and Rust.
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
        return _as_dict(r.get("memory"))

    def list_memories(
        self, user_id: Optional[str] = None, *, limit: int = 100, cursor: Optional[str] = None, model: Optional[str] = None
    ) -> dict:
        """List a store's stored memories — the original text you saved plus its
        metadata. Paginated: pass the returned ``next_cursor`` back
        as ``cursor`` for the next page; a ``None`` cursor means the last page. Use it
        to browse or export a store. Omit ``user_id`` for the client's default store.

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
        body: dict = {"user_id": self._uid(user_id), "limit": limit}
        if cursor:
            body["cursor"] = cursor
        return self._post("/api/v1/memory/list", body, model=model)

    def iter_memories(self, user_id: Optional[str] = None, *, page_size: int = 100, model: Optional[str] = None):
        """Yield every stored memory in a store, paging under the hood — no cursor
        bookkeeping. The text you stored and its metadata only.

            for m in mem.iter_memories():
                print(m["id"], m["content"])
        """
        cursor: Optional[str] = None
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = self.list_memories(user_id, limit=page_size, cursor=cursor, model=model)
            for m in _as_records(page.get("memories")):
                yield m
            nxt = page.get("next_cursor")
            # Stop on last page OR a server that repeats a cursor (would loop forever).
            if not nxt or nxt in seen:
                break
            seen.add(nxt)
            cursor = nxt
        else:
            raise RuntimeError(
                f"stopped after {_MAX_PAGES} pages — the store did not end. This is a "
                "truncated answer, not the whole store."
            )

    def export_memories(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """Return ALL of a store's memories as a list (the text you stored, and its
        metadata). Convenience over ``iter_memories`` for dumping a whole store."""
        return list(self.iter_memories(user_id, model=model))

    # ----- images (Tablet 2 and newer) -----

    def get_image(self, user_id: Optional[str] = None, memory_id: str = "", *,
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
        When the format changed, the response also carries an
        ``x-wos-image-converted-from`` header naming what you uploaded.
        Raises ``NotFoundError`` when the memory has no image, or when this service
        keeps no image bytes at all — it says "no" rather than handing back
        something empty, so "a memory with no image" never looks like "an image we lost".
        """
        user_id, memory_id = _split_id_args(user_id, memory_id)
        memory_id = _require_memory_id(memory_id, "the id that add/store or list_images returned")
        return self._request_bytes(
            "/api/v1/memory/image",
            {"user_id": self._uid(user_id), "memory_id": _reject_ctl("memory_id", memory_id)},
            model=model,
        )

    def forget_image(self, user_id: Optional[str] = None, memory_id: str = "", *,
                     preview: bool = False, model: Optional[str] = None) -> dict:
        """Remove the PHOTO from a memory, keeping its text.

        Except when there is no text: an image stored without a caption *is* the memory,
        so deleting the image deletes it. That is the case worth checking first, which
        is what ``preview=True`` is for — it reports ``memory_kept`` and changes nothing::

            if mem.forget_image(memory_id=mid, preview=True)["memory_kept"] is False:
                ...  # this would delete the whole memory, not just the image
        """
        user_id, memory_id = _split_id_args(user_id, memory_id)
        memory_id = _require_memory_id(memory_id, "the id of the memory whose image you want removed")
        body: dict = {"user_id": self._uid(user_id), "memory_id": _reject_ctl("memory_id", memory_id)}
        if preview:
            body["preview"] = True
        return self._request("DELETE", "/api/v1/memory/image", json_body=body, model=model)

    def list_images(self, user_id: Optional[str] = None, *, limit: Optional[int] = None,
                    before: Optional[str] = None, skip_ids: Optional[list] = None,
                    model: Optional[str] = None) -> dict:
        """One page of image memories, newest first, plus the store's TOTAL image count.

        ``count`` is the total, not the size of the page — so "142 images" needs one call,
        not a walk. Paging is by cursor: hand ``next_before`` and ``next_skip_ids`` back as
        ``before`` / ``skip_ids``. Both are needed because several images can share a
        timestamp, and a timestamp alone would repeat or skip them.
        """
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
        """Iterate every image memory, paging under the hood."""
        before: Optional[str] = None
        skip: Optional[list] = None
        # A server that keeps handing back the same cursor would otherwise replay the
        # same page up to _MAX_PAGES times, yielding duplicates and billing for every
        # request. ``iter_memories`` has guarded this from the start; the image walk
        # never got it, in either client. TypeScript and Rust key on the same pair.
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = self.list_images(user_id, limit=page_size, before=before,
                                    skip_ids=skip, model=model)
            for m in _as_records(page.get("images")):
                yield m
            if not page.get("has_more") or not page.get("next_before"):
                break
            before = page.get("next_before")
            nxt = page.get("next_skip_ids")
            skip = nxt if isinstance(nxt, list) else None
            key = (before, tuple(skip) if skip else ())
            if key in seen:
                break
            seen.add(key)
        else:
            raise RuntimeError(
                f"stopped after {_MAX_PAGES} pages — the store did not end. This is a "
                "truncated answer, not the whole store."
            )

    def usage(self, days: int = 7) -> dict:
        """What this key has spent, and what is left — the numbers behind "keep going?".

        Free: no charge and no balance gate, because an account at zero still has to be
        able to find out why. Rate-limited instead.

        Scoped to THIS key: its own lifetime spend, plus its workspace and stores over
        the window. Never another key's. ``balance_cents`` is account-wide, since that
        is what gates the next call whichever key makes it.

        ``stores`` is busiest first and at most 50 rows — a longer list is cut, so the
        rows need not sum to ``workspace``. A row named ``other`` is an overflow bucket,
        not a store: passing it as a store id finds nothing.

            u = mem.usage(7)
            if u["balance_cents"] < 100:
                stop()
        """
        if not isinstance(days, int) or isinstance(days, bool) or not 1 <= days <= 365:
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
        memories behind that number — at most 20 per call, one side per call. There is no
        way to ask for both lists in a single response. Page by cursor, handing
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
        it, and the address says so. The old path still answers, for clients published
        before 2026-08-18, and both share one rate-limit budget.

        Counts memories a transform touched (supersede, update, retract, image removed).
        Deletions are NOT counted — a deleted memory leaves nothing to count. Neither are
        the internal records derived from what you stored: nobody stored those directly,
        so they do not belong in a ratio that answers "how much of MY memory changed".
        """
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

    def lineage(self, user_id: Optional[str] = None, memory_id: str = "", *,
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
        words, otherwise a person's name. Same cursor paging as ``list_images``.

        ``chunks`` / ``points_to_delete`` report how many underlying records a delete would
        actually remove — usually larger than
        ``returned``, and worth showing before anyone confirms one.
        """
        if not isinstance(speaker, str) or not speaker.strip():
            raise ValueError('speaker is required — "me" for the assistant, or a person\'s name')
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
        """Available models: ``[{"id", "name", "available", "memory"}, ...]``.

        ``memory`` is ``"shared"`` (models that read the same store) or ``"isolated"``
        (a model with its own dedicated memory). Needs no API key.
        """
        return _as_list(self._request("GET", "/api/v1/models").get("models"))

    def list_engrams(self) -> dict:
        """The engrams (and delivery forms) the selected model can actually run.

        ``engram()`` could always RUN one, but there was no way to ASK what exists — so
        callers hard-coded names from the docs and every engram shipped afterwards stayed
        invisible to them. The MCP server hit exactly this and stopped hard-coding for the
        same reason. The service is the authority, and the answer depends on the model
        (delivery forms need Scroll 1.2+), so use ``with_model()`` for another model::

            cat = mem.list_engrams()
            [e["name"] for e in cat["engrams"]]

        Returns ``{"engrams": [...], "forms": [...], "note": str | None}``; ``note``
        explains an empty ``engrams`` on a model without engram support.
        """
        r = self._request("GET", "/api/v1/engram")
        return {
            "engrams": _as_list(r.get("engrams")),
            "forms": _as_list(r.get("forms")),
            "note": r.get("note"),
        }

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

        Returns ``{"user_id", "status"}`` where ``status`` is ``"created"`` or ``"exists"``.
        """
        return self._post("/api/v1/memory/collection", {"user_id": self._uid(user_id)})

    def list_stores(self) -> list[dict]:
        """List your stores: ``[{"user_id", "created_at"}, ...]`` (``default`` first)."""
        return _as_list(self._request("GET", "/api/v1/memory/collections").get("collections"))

    def delete_store(self, user_id: str) -> dict:
        """Delete a store and ALL its memories. Returns ``{"user_id", "status"}``."""
        if not isinstance(user_id, str) or not user_id.strip():
            raise ValueError("user_id is required (a non-blank string) — delete_store never falls back to the default store.")
        # Destructive calls bypass _uid(), so the collision warning has to be repeated
        # here. delete_all already did; this one did not, and it is the call that drops
        # a whole store: Alice.Smith and alice_smith are ONE store.
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
        """The store's registered people, each with its memory count."""
        return self._request("GET", "/api/v1/memory/speakers", params={"user_id": self._uid(user_id)})

    def remove_speaker(self, speaker: str, user_id: Optional[str] = None) -> dict:
        """Unregister a person. Their memories stay; the name tag goes."""
        return self._request(
            "DELETE", "/api/v1/memory/speakers", json_body={"user_id": self._uid(user_id), "speaker": speaker}
        )

    def _request_bytes(self, path: str, body: dict, *, model: Optional[str] = None) -> tuple[bytes, str]:
        """One request that answers with BYTES rather than JSON.

        Only ``/memory/image`` does this, and it is why it cannot go through ``_request``:
        that path parses the body as JSON and raises on anything else, so a JPEG would
        surface as a parse error on a call that actually succeeded.

        Errors still arrive as JSON, so a non-2xx is handed to the usual ``_parse_error``
        and keeps ``NotFoundError`` / ``AuthenticationError`` behaving as everywhere else.

        Retries 429 and connect-level failures, like every other call. This said "no
        retries — the body can be megabytes and a blind retry would pay for it twice",
        which is true of a retry AFTER bytes have arrived and not of these two: a 429
        carries no image, and a connect failure never reached the server. So the
        largest and most rate-limit-prone call in the SDK was the only one that gave up
        on the first 429, while the module docstring promised the opposite. A read
        timeout is still final — that one is ambiguous.
        """
        eff_model = _check_model(model) if model else self._model
        headers = {"X-WOS-Model": eff_model} if eff_model else None
        attempts = self._retries + 1
        deadline_at = _deadline_at(self._deadline)
        for attempt in range(attempts):
            try:
                r = self._session.request(
                    "POST",
                    f"{self._base}{path}",
                    json=body,
                    headers=headers,
                    timeout=_timeout_pair(self._timeout, deadline_at, self._deadline),
                    allow_redirects=False,
                    # Streamed, like the JSON path. Without this ``requests`` has
                    # already downloaded the entire body by the time it returns, so
                    # the cap in ``_read_capped_bytes`` measures bytes that are
                    # ALREADY in memory — exactly the case it exists to prevent, on
                    # the one route that returns megabytes.
                    stream=True,
                )
            except requests.exceptions.RequestException as e:
                if (
                    isinstance(e, requests.exceptions.ConnectionError)
                    and _never_sent(e)
                    and attempt + 1 < attempts
                ):
                    time.sleep(_sleep_within(_backoff(attempt, None), deadline_at, self._deadline))
                    continue
                raise APIConnectionError(0, f"network error: {e}") from e
            # `stream=True` keeps a pooled connection checked out until the response is
            # closed, and two paths here used to raise without closing: the redirect, and
            # the size cap inside the reader. A misconfigured base_url behind a proxy that
            # 302s every call then leaked one connection per get_image() until GC, and
            # urllib3 began logging "Connection pool is full, discarding connection".
            # The JSON path has always had this `finally`.
            closed = False
            try:
                # The quota this call just spent. rate_limit promises the MOST RECENT
                # call; the async client and Rust record it here and this one did not, so
                # a sync image loop self-throttling on it read a snapshot frozen at
                # whatever JSON call came before — or None.
                self._rate_limit = _parse_rate_limit(r.headers) or self._rate_limit
                if 300 <= r.status_code < 400:
                    raise _make_error(
                        r.status_code, "the API answered with a redirect; refusing to follow it"
                    )
                if r.status_code in _RETRY_ALWAYS and attempt + 1 < attempts:
                    retry_after = r.headers.get("Retry-After")
                    r.close()
                    closed = True
                    time.sleep(_sleep_within(_backoff(attempt, retry_after), deadline_at, self._deadline))
                    continue
                if r.status_code >= 400:
                    # Capped like every other error body: the response is streamed now,
                    # so ``r.text`` would buffer whatever a broken host sends.
                    raise _parse_error(r.status_code, Client._read_capped(r))
                data = Client._read_capped_bytes(r)
            finally:
                if not closed:
                    r.close()
            break
        else:  # pragma: no cover — the loop always breaks or raises
            raise RuntimeError("retries exhausted")
        if not data:
            # An empty 200 would otherwise read as "here is your image" and write a
            # zero-byte file — indistinguishable from an image we lost.
            raise WosError(r.status_code, "empty image body — the service returned no bytes")
        return data, r.headers.get("content-type", "application/octet-stream")

    # ----- delete -----

    def delete(self, user_id: Optional[str] = None, memory_id: str = "", *, model: Optional[str] = None) -> dict:
        """Delete a single memory by id. Omit ``user_id`` (keyword ``memory_id=``) for the default store."""
        # Same store-first argument order as get(): recover the lone-id call before
        # the guard, so `mem.delete(memory_id)` deletes that ONE memory instead of
        # raising. It cannot widen a delete — a lone UUID names a memory, and with
        # no id at all the wipe guard below still fires.
        user_id, memory_id = _split_id_args(user_id, memory_id)
        # `.strip()` matters as much as the emptiness test: "   " is truthy, so it used
        # to pass this guard and travel as memory_id. A server that trims it back to
        # nothing reads the request as the whole-store form. delete_all() below already
        # stripped; the more dangerous path was the one that did not.
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
        # Destructive calls bypass _uid(), so the collision warning never fired on the
        # two that erase data. Alice.Smith and alice_smith are ONE store.
        _warn_if_store_id_collapses(user_id)
        return self._post("/api/v1/memory/forget", {"user_id": user_id}, model=model)

    # ----- internal -----

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
    ) -> dict:
        # X-WOS-Model is set per-request (not on the shared session, since clones
        # differ on it): a per-call model overrides the client default.
        eff_model = _check_model(model) if model else self._model
        headers = {"X-WOS-Model": eff_model} if eff_model else None
        idem = _idem_headers(idempotency_key)
        if idem:
            headers = {**(headers or {}), **idem}
        attempts = self._retries + 1
        deadline_at = _deadline_at(self._deadline)
        for attempt in range(attempts):
            start = time.monotonic()
            try:
                r = self._session.request(
                    method,
                    f"{self._base}{path}",
                    json=json_body,
                    params=params,
                    headers=headers,
                    # (connect, read): don't spend the whole budget waiting for a
                    # dead host to accept — same 10s connect phase as the async
                    # client and the Rust SDK.
                    timeout=_timeout_pair(self._timeout, deadline_at, self._deadline),
                    # Never follow a redirect: requests forwards custom headers
                    # (the API key) to wherever a 3xx points. The API never
                    # legitimately redirects.
                    allow_redirects=False,
                    # Streamed so the size cap below can refuse before buffering.
                    stream=True,
                )
            except requests.exceptions.InvalidJSONError as e:
                # A non-serializable request body is a CLIENT bug, not a network
                # failure — surface it as such so no one debugs their connection.
                raise TypeError(f"request body is not JSON-serializable: {e}") from e
            except requests.exceptions.ConnectionError as e:
                # Retry only when it cannot double-process a write: idempotent
                # methods always; writes only when the failure was at CONNECT
                # time (the request never reached the server). A mid-stream drop
                # on a POST is ambiguous — the write may have landed and billed.
                safe = method.upper() in _IDEMPOTENT_METHODS or _never_sent(e)
                if safe and attempt + 1 < attempts:
                    delay = _backoff(attempt)
                    _logger.debug(
                        "%s %s: %s — retrying in %.1fs (attempt %d/%d)",
                        method, path, type(e).__name__, delay, attempt + 1, attempts,
                    )
                    time.sleep(_sleep_within(delay, deadline_at, self._deadline))
                    continue
                raise APIConnectionError(0, f"network error: {e}") from e
            except requests.RequestException as e:
                # Read timeouts etc. are ambiguous (the write may have landed) — don't retry.
                raise APIConnectionError(0, f"network error: {e}") from e
            try:
                retryable = r.status_code in _RETRY_ALWAYS or (
                    r.status_code in _RETRY_IF_IDEMPOTENT and method.upper() in _IDEMPOTENT_METHODS
                )
                if retryable and attempt + 1 < attempts:
                    retry_after = r.headers.get("Retry-After")
                    r.close()
                    delay = _backoff(attempt, retry_after)
                    _logger.debug(
                        "%s %s -> %d — retrying in %.1fs (attempt %d/%d)",
                        method, path, r.status_code, delay, attempt + 1, attempts,
                    )
                    time.sleep(_sleep_within(delay, deadline_at, self._deadline))
                    continue
                if 300 <= r.status_code < 400:
                    raise WosError(
                        r.status_code,
                        "unexpected redirect — refused (the API key never follows a redirect). "
                        "Check base_url: exact host, https://.",
                    )
                # Stash the quota headers from this (final) response so callers can
                # self-throttle via `client.rate_limit`. Keep the last value if this
                # response didn't carry them.
                self._rate_limit = _parse_rate_limit(r.headers) or self._rate_limit
                try:
                    text = self._read_capped(r)
                except WosError:
                    raise  # the size cap — already the right error
                except Exception as e:
                    # A drop while READING the body is a transport failure — surface
                    # it as APIConnectionError, not a raw urllib3 internal. Ambiguous
                    # (the response existed), so never retried.
                    raise APIConnectionError(0, f"network error: {e}") from e
            finally:
                r.close()
            _logger.debug(
                "%s %s -> %d in %.0fms (attempt %d/%d)",
                method, path, r.status_code, (time.monotonic() - start) * 1000, attempt + 1, attempts,
            )
            if not (200 <= r.status_code < 300):
                raise _parse_error(r.status_code, text)
            data = _parse_ok(r.status_code, text)
            # Surface whether the write was stored or replayed. The server says so with
            # the Idempotent-Replayed header, and the caller cannot tell otherwise.
            # Only when the body does not already carry it: these responses are
            # widening — a response may carry fields this client has never seen — and
            # a client that writes into the service's object is one release away from
            # overwriting a real answer with its own guess.
            if r.headers.get("Idempotent-Replayed") == "true" and "replayed" not in data:
                data["replayed"] = True
            return data
        raise RuntimeError("retries exhausted")  # unreachable; keeps type-checkers happy

    @staticmethod
    def _read_capped(r: requests.Response) -> str:
        return Client._read_capped_bytes(r).decode("utf-8", "replace")

    @staticmethod
    def _read_capped_bytes(r: requests.Response) -> bytes:
        """The same ceiling, for a body that is not text.

        The image routes used ``r.content``, which buffers whatever arrives. The cap is
        there for a hostile or broken ``base_url``, and an image endpoint is exactly
        where that shows up — the JSON paths were guarded while the one path that
        returns megabytes was not. One implementation, two callers, so the two cannot
        drift apart.
        """
        cl = r.headers.get("Content-Length", "")
        if cl.isdigit() and int(cl) > _MAX_RESPONSE_BYTES:
            raise WosError(r.status_code, f"response too large ({cl} bytes) — refusing to buffer it")
        # Stream and cap INCREMENTALLY (like the async client). A single
        # read(_MAX+1, decode_content=True) can materialize a whole DECOMPRESSED
        # body before the size check, so a small gzip bomb — whose Content-Length
        # (compressed) sails under the cap — could OOM the process. iter_content
        # decompresses chunk by chunk, so we stop the instant we cross the ceiling.
        raw = bytearray()
        for chunk in r.iter_content(chunk_size=65536):
            if not chunk:
                continue
            # Refuse the chunk that would cross the line rather than absorbing it first:
            # with decoded (decompressed) chunks, "check after extend" means the memory
            # is already committed.
            if len(raw) + len(chunk) > _MAX_RESPONSE_BYTES:
                raise WosError(r.status_code, "response too large — refusing to buffer it")
            raw.extend(chunk)
        return bytes(raw)


# Back-compat alias: older code used `WME`.
WME = Client


class AsyncClient:
    """Async twin of :class:`Client` — the exact same surface, awaitable.

    Requires the async extra (an ``httpx`` dependency)::

        pip install "wontopos[async]"

        from wontopos import AsyncClient
        async with AsyncClient(api_key="wos-...", user_id="alice") as mem:
            await mem.create_store()
            await mem.add("she prefers tea over coffee")
            hits = await mem.search("what does alice drink?")

    Same reliability and security posture as the sync client: retries with
    backoff (429/502/503 + connect errors, ``Retry-After`` honored), redirects
    refused, TLS 1.2 floor, 64MB response cap, key masked in ``repr``.
    Close it with ``await mem.aclose()`` or use ``async with``.
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
        # Same alias as the sync client, and now the same resolution. This used to be
        # ``retries: int = 2`` compared against 2, which cannot tell "the caller said 2"
        # from "the caller said nothing" — so ``AsyncClient(key, retries=2,
        # max_retries=5)`` retried five times against an explicit instruction to retry
        # twice. The sync client's comment explained exactly that, and this comment
        # pointed at it while keeping the bug.
        if retries is None:
            retries = 2 if max_retries is None else max_retries
        try:
            import httpx
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                'AsyncClient needs the async extra: pip install "wontopos[async]"'
            ) from e
        self._api_key = _clean_key(api_key)
        self._base = base_url.rstrip("/")
        # A non-positive/NaN timeout would fail every request instantly — fall
        # back to the default instead (same guard as the sync client).
        self._timeout = timeout if isinstance(timeout, (int, float)) and timeout > 0 else 30.0
        # A non-positive budget would fail every call before it started — that reads as
        # "no budget", the same fallback the timeout above takes.
        self._deadline = deadline if isinstance(deadline, (int, float)) and deadline > 0 else None
        self._model = _check_model(model)
        # The same guard _uid applies per call. It used to live only there, so the
        # constructor and with_user() — the documented per-tenant pattern — walked past
        # it: user_id=0 and user_id="" became the shared `default` store with no warning,
        # while add(text, user_id=0) raised. A destination that depends on WHICH door the
        # id came through is the worst kind of silent redirect.
        _assert_usable_store_id(user_id)
        self._user_id = user_id
        self._retries = max(0, int(retries))
        self._rate_limit: Optional[dict] = None
        self._httpx = httpx
        _warn_if_plain_http(self._base)
        # A clone shares the parent's httpx client (connection pool + TLS reused).
        # Model and timeout are NOT baked into the shared client — they differ
        # across clones and are applied per-request instead. Only the owner closes.
        self._owns_http = _http is None
        if _http is not None:
            self._http = _http
        else:
            self._http = httpx.AsyncClient(
                headers={
                    "X-API-Key": self._api_key,
                    "Content-Type": "application/json",
                    "User-Agent": _USER_AGENT,
                },
                verify=_tls_context(),
                follow_redirects=False,
            )

    @classmethod
    def from_env(cls, **kwargs: Any) -> "AsyncClient":
        """Like :meth:`Client.from_env`, async."""
        return cls(_env_key(), **kwargs)

    def __repr__(self) -> str:
        return (
            f"AsyncClient(base_url={self._base!r}, model={self._model!r}, "
            f"user_id={self._user_id!r}, api_key='{_mask_key(self._api_key)}')"
        )

    async def aclose(self) -> None:
        # A clone shares the parent's httpx client and does NOT close it — only
        # the client that created it does.
        if self._owns_http:
            await self._http.aclose()

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
            deadline=self._deadline, _http=self._http,
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
        """An async client with a different per-request timeout in seconds (shares the pool)."""
        return self._clone(timeout=timeout)

    def with_retries(self, retries: int) -> "AsyncClient":
        """An async client with a different retry budget. 0 disables retries (shares the pool)."""
        return self._clone(retries=retries)

    def _uid(self, user_id: Optional[str]) -> str:
        # Same rule as the sync client: an omitted id uses the default, a PASSED blank id
        # is a bug at the call site and must not silently land in the default store.
        # (This copy had no docstring, which is why a text-matched fix skipped it — the
        # two clients drifting apart is exactly what the parity tests exist to catch.)
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
        """Store one memory. Extra keyword args become metadata (incl. ``speaker=``).

        ``image={"data": <base64>}`` attaches an image (Tablet 2 and newer); ``content``
        may be empty, in which case the image is the memory. See ``Client.add``.
        """
        md = {**metadata, **extra} if metadata else extra
        body: dict = {"user_id": self._uid(user_id), "content": content, "metadata": md}
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
        self, content: str, user_id: Optional[str] = None, category: str = "general",
        timestamp: Optional[str] = None, *, model: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Bulk-ingest a large blob of text in one call."""
        body: dict[str, Any] = {"user_id": self._uid(user_id), "content": content, "category": category}
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
        self, query: str, user_id: Optional[str] = None, limit: int = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> list[dict]:
        """Search a store's memories. Returns them most relevant first.

        The list ARRIVES in that order — do not re-sort it. ``similarity`` on each
        memory is a raw closeness score, not the ranking key: what produces the order
        is internal and is not returned, so sorting by ``similarity`` overrides the
        ranking and makes results worse. There is no ``score`` field.

        Takes the same options as :meth:`Client.search` — ``filters``, ``cache_control``
        (repeated queries bill at 0.1×), ``speaker`` — documented there rather than
        duplicated here. This one-line docstring was the only thing an async caller
        saw, so those options were invisible from this side of the SDK.
        """
        body = _search_body(self._uid(user_id), query, limit, opts, verify, max_images)
        return _merge_results(await self._post("/api/v1/memory/search", body, model=model))

    async def search_full(
        self, query: str, user_id: Optional[str] = None, limit: int = 10, *,
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
        self, query: str, user_id: Optional[str] = None, limit: int = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> dict:
        """Search a self-memory model (Scroll 1.2+): both fields from ONE call.

        Returns ``{"memories": [...], "self_memories": [...]}`` — general memories plus
        the assistant's own words (``speaker="me"``) kept separate. ``self_memories``
        is ``[]`` on models that do not keep them apart. See ``Client.search_self``.
        """
        body = _search_body(self._uid(user_id), query, limit, opts, verify, max_images)
        r = await self._post("/api/v1/memory/search", body, model=model)
        return {"memories": _as_records(r.get("memories")), "self_memories": _as_records(r.get("self_memories"))}

    async def recall(
        self, query: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
        form: Optional[str] = None, tz: Optional[int] = None,
        limit: Optional[int] = None, context_limit: Optional[int] = None,
    ) -> dict:
        """One-call context for an LLM: ``{"short_term", "long_term", "context"}``.
        ``form``/``tz`` render long-term memory times (memoir/archive) on Scroll 1.2+."""
        body = _recall_body(self._uid(user_id), query, form, tz, limit, context_limit)
        return await self._post("/api/v1/memory/recall", body, model=model)

    async def engram(
        self, name: str, query: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
        form: Optional[str] = None, tz: Optional[int] = None,
    ) -> dict:
        """Run a built-in engram; the service is the authority on the names it accepts.
        ``form``/``tz`` render memory times (memoir/archive) on Scroll 1.2+."""
        body = _form_body({"name": name, "user_id": self._uid(user_id), "query": query}, form, tz)
        return await self._post("/api/v1/engram/run", body, model=model)

    async def history(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """Recent conversation turns (short-term memory)."""
        return _as_records((await self._post("/api/v1/memory/history", {"user_id": self._uid(user_id)}, model=model)).get("turns"))

    async def stats(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> dict:
        """Memory counts for a store: ``{total_memories, short_term_turns}``."""
        return await self._post("/api/v1/memory/stats", {"user_id": self._uid(user_id)}, model=model)

    async def get(self, user_id: Optional[str] = None, memory_id: str = "", *, model: Optional[str] = None) -> dict:
        """Fetch ONE memory by id — the text you stored, and its metadata.
        Same visibility as ``list_memories``; unknown, foreign, or internal-only ids
        raise ``NotFoundError``."""
        user_id, memory_id = _split_id_args(user_id, memory_id)
        # `"   "` is truthy, so the old test passed it through as the id. The four
        # sibling calls go through _require_memory_id, which refuses a blank one and
        # returns it stripped; get did neither, so an id pasted from a log with a stray
        # space was answered here and 404'd in TypeScript and Rust.
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
        return _as_dict(r.get("memory"))

    async def list_memories(
        self, user_id: Optional[str] = None, *, limit: int = 100, cursor: Optional[str] = None, model: Optional[str] = None
    ) -> dict:
        """List a store's stored memories — the text you stored, and its metadata.
        Paginated via ``cursor``/``next_cursor`` (a ``None`` cursor is the last page).
        Returns ``{"memories": [...], "count": int, "next_cursor": str | None}``.
        """
        body: dict = {"user_id": self._uid(user_id), "limit": limit}
        if cursor:
            body["cursor"] = cursor
        return await self._post("/api/v1/memory/list", body, model=model)

    async def iter_memories(self, user_id: Optional[str] = None, *, page_size: int = 100, model: Optional[str] = None):
        """Async-yield every stored memory in a store, paging under the hood.

            async for m in mem.iter_memories():
                print(m["id"], m["content"])
        """
        cursor: Optional[str] = None
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = await self.list_memories(user_id, limit=page_size, cursor=cursor, model=model)
            for m in _as_records(page.get("memories")):
                yield m
            nxt = page.get("next_cursor")
            # Stop on last page OR a server that repeats a cursor (would loop forever).
            if not nxt or nxt in seen:
                break
            seen.add(nxt)
            cursor = nxt
        else:
            raise RuntimeError(
                f"stopped after {_MAX_PAGES} pages — the store did not end. This is a "
                "truncated answer, not the whole store."
            )

    async def export_memories(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """Return ALL of a store's memories as a list (the text you stored, and its metadata)."""
        return [m async for m in self.iter_memories(user_id, model=model)]

    # ----- images (Tablet 2 and newer) -----

    async def _read_capped_bytes(self, r) -> bytes:
        """The response ceiling, for a body that is not text — httpx side.

        Separate from ``Client._read_capped_bytes`` on purpose: that one calls
        ``iter_content`` on a ``requests`` response, which an httpx response does not
        have. Sharing it raises ``AttributeError`` on the first image an async
        caller fetched, and no offline test would have said so — the async image path is
        not exercised by the mock server. Two transports, two readers, one rule.

        Same rule as ``_request`` above: bound each yield and refuse the chunk that would
        cross the line, because ``aiter_bytes`` hands back DECODED bytes and checking
        after ``extend`` means the memory is already committed.
        """
        cl = r.headers.get("Content-Length", "")
        if cl.isdigit() and int(cl) > _MAX_RESPONSE_BYTES:
            raise WosError(r.status_code, f"response too large ({cl} bytes) — refusing to buffer it")
        raw = bytearray()
        async for chunk in r.aiter_bytes(chunk_size=65536):
            if not chunk:
                continue
            if len(raw) + len(chunk) > _MAX_RESPONSE_BYTES:
                raise WosError(r.status_code, "response too large — refusing to buffer it")
            raw.extend(chunk)
        return bytes(raw)

    async def get_image(self, user_id: Optional[str] = None, memory_id: str = "", *,
                        model: Optional[str] = None) -> tuple[bytes, str]:
        """Fetch the bytes of an image memory → ``(bytes, content_type)``.

        This is the picture the SERVICE holds: anything over 1568px on its long edge was
        downscaled on the way in, and re-encoded to WebP unless it was a JPEG. Name the
        file from ``content_type``, not from what you uploaded. See ``Client.get_image``."""
        user_id, memory_id = _split_id_args(user_id, memory_id)
        memory_id = _require_memory_id(memory_id, "the id that add/store or list_images returned")
        return await self._request_bytes(
            "/api/v1/memory/image",
            {"user_id": self._uid(user_id), "memory_id": _reject_ctl("memory_id", memory_id)},
            model=model,
        )

    async def forget_image(self, user_id: Optional[str] = None, memory_id: str = "", *,
                           preview: bool = False, model: Optional[str] = None) -> dict:
        """Remove the PHOTO, keeping the text — unless the image IS the memory.
        ``preview=True`` reports ``memory_kept`` and changes nothing. See ``Client.forget_image``."""
        user_id, memory_id = _split_id_args(user_id, memory_id)
        memory_id = _require_memory_id(memory_id, "the id of the memory whose image you want removed")
        body: dict = {"user_id": self._uid(user_id), "memory_id": _reject_ctl("memory_id", memory_id)}
        if preview:
            body["preview"] = True
        return await self._request("DELETE", "/api/v1/memory/image", json_body=body, model=model)

    async def list_images(self, user_id: Optional[str] = None, *, limit: Optional[int] = None,
                          before: Optional[str] = None, skip_ids: Optional[list] = None,
                          model: Optional[str] = None) -> dict:
        """One page of image memories, newest first, plus the store's TOTAL ``count``.
        See ``Client.list_images``."""
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
        before: Optional[str] = None
        skip: Optional[list] = None
        # A server that keeps handing back the same cursor would otherwise replay the
        # same page up to _MAX_PAGES times, yielding duplicates and billing for every
        # request. ``iter_memories`` has guarded this from the start; the image walk
        # never got it, in either client. TypeScript and Rust key on the same pair.
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = await self.list_images(user_id, limit=page_size, before=before,
                                          skip_ids=skip, model=model)
            for m in _as_records(page.get("images")):
                yield m
            if not page.get("has_more") or not page.get("next_before"):
                break
            before = page.get("next_before")
            nxt = page.get("next_skip_ids")
            skip = nxt if isinstance(nxt, list) else None
            key = (before, tuple(skip) if skip else ())
            if key in seen:
                break
            seen.add(key)
        else:
            raise RuntimeError(
                f"stopped after {_MAX_PAGES} pages — the store did not end. This is a "
                "truncated answer, not the whole store."
            )

    async def usage(self, days: int = 7) -> dict:
        """What this key has spent, and what is left — the numbers behind "keep going?".

        Free: no charge and no balance gate, because an account at zero still has to be
        able to find out why. Rate-limited instead.

        Scoped to THIS key: its own lifetime spend, plus its workspace and stores over
        the window. Never another key's. ``balance_cents`` is account-wide, since that
        is what gates the next call whichever key makes it.

        ``stores`` is busiest first and at most 50 rows — a longer list is cut, so the
        rows need not sum to ``workspace``. A row named ``other`` is an overflow bucket,
        not a store: passing it as a store id finds nothing.

            u = await mem.usage(7)
            if u["balance_cents"] < 100:
                stop()
        """
        if not isinstance(days, int) or isinstance(days, bool) or not 1 <= days <= 365:
            raise ValueError(f"days must be an integer between 1 and 365, got {days!r}.")
        return await self._request("GET", f"/api/v1/won/usage?days={days}")

    # ----- how much has this memory been edited -----

    async def revisions(self, user_id: Optional[str] = None, *, include: Optional[str] = None,
                        limit: Optional[int] = None, before: Optional[str] = None,
                        skip_ids: Optional[list] = None, model: Optional[str] = None) -> dict:
        """How much of this store has been altered since it was written.

        Counts only unless ``include`` asks for a page. See ``Client.revisions``."""
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

    async def lineage(self, user_id: Optional[str] = None, memory_id: str = "", *,
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
        return {
            "engrams": _as_list(r.get("engrams")),
            "forms": _as_list(r.get("forms")),
            "note": r.get("note"),
        }

    async def list_models(self) -> list[dict]:
        """Available models. Needs no API key."""
        return _as_list((await self._request("GET", "/api/v1/models")).get("models"))

    async def create_store(self, user_id: Optional[str] = None) -> dict:
        """Create a store (explicit, idempotent). Returns ``{"user_id", "status"}``."""
        return await self._post("/api/v1/memory/collection", {"user_id": self._uid(user_id)})

    async def list_stores(self) -> list[dict]:
        """List your stores (``default`` first)."""
        return _as_list((await self._request("GET", "/api/v1/memory/collections")).get("collections"))

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
        return await self._request("GET", "/api/v1/memory/speakers", params={"user_id": self._uid(user_id)})

    async def remove_speaker(self, speaker: str, user_id: Optional[str] = None) -> dict:
        """Unregister a person. Their memories stay; the name tag goes."""
        return await self._request(
            "DELETE", "/api/v1/memory/speakers", json_body={"user_id": self._uid(user_id), "speaker": speaker}
        )

    # ----- delete -----

    async def delete(self, user_id: Optional[str] = None, memory_id: str = "", *, model: Optional[str] = None) -> dict:
        """Delete a single memory by id."""
        # Same store-first argument order as get(): recover the lone-id call before
        # the guard, so `mem.delete(memory_id)` deletes that ONE memory instead of
        # raising. It cannot widen a delete — a lone UUID names a memory, and with
        # no id at all the wipe guard below still fires.
        user_id, memory_id = _split_id_args(user_id, memory_id)
        # `.strip()` matters as much as the emptiness test: "   " is truthy, so it used
        # to pass this guard and travel as memory_id. A server that trims it back to
        # nothing reads the request as the whole-store form. delete_all() below already
        # stripped; the more dangerous path was the one that did not.
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
        # Destructive calls bypass _uid(), so the collision warning never fired on the
        # two that erase data. Alice.Smith and alice_smith are ONE store.
        _warn_if_store_id_collapses(user_id)
        return await self._post("/api/v1/memory/forget", {"user_id": user_id}, model=model)

    # ----- internal -----

    async def _post(self, path: str, body: dict, model: Optional[str] = None,
                    idempotency_key: Optional[str] = None) -> dict:
        return await self._request("POST", path, json_body=body, model=model,
                                   idempotency_key=idempotency_key)

    async def _request_bytes(self, path: str, body: dict, *, model: Optional[str] = None) -> tuple[bytes, str]:
        """One request that answers with BYTES rather than JSON — see ``Client._request_bytes``.

        Retries 429 and connect-level failures, like every other call. The old note here
        said "no retries — the body can be megabytes and a blind retry would pay for it
        twice", which is true of a retry AFTER bytes have arrived and not of these two: a
        429 carries no image, and a connect failure never reached the server. The sync
        client and the TypeScript client both corrected this; this one had not, so
        ``async for m in mem.iter_images(): await mem.get_image(...)`` — the most
        rate-limit-prone loop in the SDK — died on the first 429 the others rode through.
        """
        eff_model = _check_model(model) if model else self._model
        headers = {"X-WOS-Model": eff_model} if eff_model else None
        attempts = self._retries + 1
        deadline_at = _deadline_at(self._deadline)
        for attempt in range(attempts):
            try:
                return await self._request_bytes_once(path, body, headers, deadline_at)
            except RateLimitError as e:
                if attempt + 1 >= attempts:
                    raise
                await asyncio.sleep(
                    _sleep_within(_backoff(attempt, getattr(e, "_retry_after", None)), deadline_at, self._deadline)
                )
            except APIConnectionError as e:
                # Only a failure that never reached the server. A read timeout is
                # ambiguous and a mid-stream drop may already have been billed.
                if attempt + 1 >= attempts or not getattr(e, "_never_sent", False):
                    raise
                await asyncio.sleep(_sleep_within(_backoff(attempt), deadline_at, self._deadline))
        raise APIConnectionError(0, "retries exhausted")  # pragma: no cover — the loop returns or raises

    async def _request_bytes_once(
        self, path: str, body: dict, headers: Optional[dict], deadline_at: Optional[float]
    ) -> tuple[bytes, str]:
        # Outside the try: `except Exception` below would relabel an exhausted budget
        # as a network error, which is the one thing it certainly is not.
        _budget = _attempt_budget(self._timeout, deadline_at, self._deadline)
        try:
            # Streamed, like the sync client. ``post()`` is httpx's non-streaming path:
            # it awaits ``aread()`` and DECOMPRESSES the whole body before returning, so
            # ``_read_capped_bytes`` was measuring bytes already committed to memory —
            # the cap could not protect the one route that returns megabytes. Measured
            # on a 600MB decompressed body: sync +68MB, async +1274MB.
            req = self._http.build_request(
                "POST", f"{self._base}{path}", json=body, headers=headers,
                timeout=self._httpx.Timeout(_budget, connect=min(10.0, _budget)),
            )
            r = await self._http.send(req, stream=True)
        except Exception as e:  # httpx transport errors
            err = APIConnectionError(0, f"network error: {e}")
            # Connect-level only: the request never left, so re-sending cannot
            # double-process anything. Everything else stays final.
            err._never_sent = isinstance(e, (self._httpx.ConnectError, self._httpx.ConnectTimeout))
            raise err from e
        # A streamed response holds a connection until it is closed, and ``r.text``
        # raises on one that has not been read — so both the error path and the happy
        # path go through the capped reader, and the close happens either way.
        try:
            retry_after = r.headers.get("Retry-After")
            self._rate_limit = _parse_rate_limit(r.headers) or self._rate_limit
            if 300 <= r.status_code < 400:
                raise _make_error(
                    r.status_code, "the API answered with a redirect; refusing to follow it"
                )
            data = await self._read_capped_bytes(r)
            if r.status_code >= 400:
                err = _parse_error(r.status_code, data.decode("utf-8", "replace"))
                # Carried on the exception, not on the client. Two awaits sit between
                # reading this header and the retry that needs it, so a second concurrent
                # image call could overwrite a shared field in between and make this one
                # sleep on the wrong Retry-After.
                err._retry_after = retry_after
                raise err
            if not data:
                raise WosError(r.status_code, "empty image body — the service returned no bytes")
            ctype = r.headers.get("content-type", "application/octet-stream")
        finally:
            await r.aclose()
        return data, ctype

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
        model: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        eff_model = _check_model(model) if model else self._model
        headers = {"X-WOS-Model": eff_model} if eff_model else None
        idem = _idem_headers(idempotency_key)
        if idem:
            headers = {**(headers or {}), **idem}
        attempts = self._retries + 1
        deadline_at = _deadline_at(self._deadline)
        for attempt in range(attempts):
            start = time.monotonic()
            # Outside the try for the same reason as the bytes path: an exhausted
            # budget must not come back wearing "network error".
            _budget = _attempt_budget(self._timeout, deadline_at, self._deadline)
            pending_delay: Optional[float] = None
            try:
                async with self._http.stream(
                    method, f"{self._base}{path}", json=json_body, params=params, headers=headers,
                    # Per-request (not baked into the shared client) so with_timeout() clones work.
                    timeout=self._httpx.Timeout(_budget, connect=min(10.0, _budget)),
                ) as r:
                    retryable = r.status_code in _RETRY_ALWAYS or (
                        r.status_code in _RETRY_IF_IDEMPOTENT and method.upper() in _IDEMPOTENT_METHODS
                    )
                    if retryable and attempt + 1 < attempts:
                        retry_after = r.headers.get("Retry-After")
                        delay = _backoff(attempt, retry_after)
                        _logger.debug(
                            "%s %s -> %d — retrying in %.1fs (attempt %d/%d)",
                            method, path, r.status_code, delay, attempt + 1, attempts,
                        )
                        # Sleep AFTER the `async with` releases the connection, not inside
                        # it. Sleeping here kept one pooled connection checked out for the
                        # whole backoff — with `Retry-After: 30` and enough concurrent
                        # tasks that is the entire httpx pool asleep, and every other
                        # request in the process fails PoolTimeout, un-retried. The sync
                        # client closes first for this reason (`r.close()` before sleep).
                        pending_delay = _sleep_within(delay, deadline_at, self._deadline)
                        # Leave the `async with`; the sleep happens below, unpooled.
                        raise _Backoff
                    if 300 <= r.status_code < 400:
                        raise WosError(
                            r.status_code,
                            "unexpected redirect — refused (the API key never follows a redirect). "
                            "Check base_url: exact host, https://.",
                        )
                    self._rate_limit = _parse_rate_limit(r.headers) or self._rate_limit
                    cl = r.headers.get("Content-Length", "")
                    if cl.isdigit() and int(cl) > _MAX_RESPONSE_BYTES:
                        raise WosError(r.status_code, f"response too large ({cl} bytes) — refusing to buffer it")
                    raw = bytearray()
                    # `aiter_bytes()` yields DECODED bytes — httpx has already undone any
                    # gzip — so checking the total AFTER `extend` would mean the allocation
                    # already happened. Refuse the chunk that would cross the line instead
                    # of absorbing it first.
                    #
                    # ⚠️`chunk_size` bounds what the ITERATOR hands back, not what the
                    # decoder allocates: httpx's GZipDecoder calls decompress() with no
                    # max_length, so one socket read can expand in a single allocation
                    # before this loop sees anything. Measured on the JSON path: a bomb
                    # that should stop at 64MB peaks around 490MB here, where the sync
                    # client (urllib3 decodes incrementally) holds flat at the cap. This
                    # is a ceiling on what is KEPT, not on what is allocated to get there.
                    async for chunk in r.aiter_bytes(chunk_size=65536):
                        if not chunk:
                            continue
                        if len(raw) + len(chunk) > _MAX_RESPONSE_BYTES:
                            raise WosError(r.status_code, "response too large — refusing to buffer it")
                        raw.extend(chunk)
                    text = raw.decode("utf-8", "replace")
                    _logger.debug(
                        "%s %s -> %d in %.0fms (attempt %d/%d)",
                        method, path, r.status_code, (time.monotonic() - start) * 1000, attempt + 1, attempts,
                    )
                    if not (200 <= r.status_code < 300):
                        raise _parse_error(r.status_code, text)
                    data = _parse_ok(r.status_code, text)
                    # Surface whether the write was stored or replayed. The server says so
                    # with the Idempotent-Replayed header, and the caller cannot tell
                    # otherwise.
                    # Only when absent — see the sync client for why.
                    if r.headers.get("Idempotent-Replayed") == "true" and "replayed" not in data:
                        data["replayed"] = True
                    return data
            except _Backoff:
                # The status said retry. We are out of the `async with`, so the pooled
                # connection is back before we sleep on it.
                await asyncio.sleep(pending_delay or 0.0)
                continue
            except (self._httpx.ConnectError, self._httpx.ConnectTimeout) as e:
                # Connect-level failure — the server never processed anything.
                if attempt + 1 < attempts:
                    delay = _backoff(attempt)
                    _logger.debug(
                        "%s %s: %s — retrying in %.1fs (attempt %d/%d)",
                        method, path, type(e).__name__, delay, attempt + 1, attempts,
                    )
                    await asyncio.sleep(_sleep_within(delay, deadline_at, self._deadline))
                    continue
                raise APIConnectionError(0, f"network error: {e}") from e
            except (self._httpx.ReadError, self._httpx.WriteError, self._httpx.RemoteProtocolError) as e:
                # The connection dropped MID-STREAM: the server may already have
                # processed (and billed) the request, so a write must not be
                # retried. An idempotent method can be — replaying a GET or a
                # DELETE cannot double-process anything.
                #
                # The sync client has always retried these for idempotent methods
                # (`method in _IDEMPOTENT_METHODS or _never_sent(e)`); this client
                # caught only connect-level failures, so the same dropped GET was
                # retried by one and raised by the other. Two clients published as
                # "the same surface" should not disagree about that. Timeouts stay
                # out of it in both: those are ambiguous, not merely idempotent.
                if method.upper() in _IDEMPOTENT_METHODS and attempt + 1 < attempts:
                    delay = _backoff(attempt)
                    _logger.debug(
                        "%s %s: %s — retrying in %.1fs (attempt %d/%d)",
                        method, path, type(e).__name__, delay, attempt + 1, attempts,
                    )
                    await asyncio.sleep(_sleep_within(delay, deadline_at, self._deadline))
                    continue
                raise APIConnectionError(0, f"network error: {e}") from e
            except self._httpx.HTTPError as e:
                # Read timeouts etc. are ambiguous (the write may have landed) — don't retry.
                raise APIConnectionError(0, f"network error: {e}") from e
        raise RuntimeError("retries exhausted")  # unreachable; keeps type-checkers happy
