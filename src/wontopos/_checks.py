from __future__ import annotations

import copy as _copy
import logging
import math
import numbers
import re
import threading
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Optional

from . import _JS_SPACE, _JS_SPACE_CLASS, _NO_ID, _as_list, _check_flag, _is_int, _is_whole, _jq, _js_str, _js_trim
from .sleep import _SLEEP_SLOTS, _sleep_date

_IDEMPOTENCY_KEY_RE = re.compile(r"[A-Za-z0-9._:\-]{1,128}")
_STORE_ID_FORMAT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")


def _normalize_store_id(sid: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", sid.lower())


_WARNED_STORE_IDS_MAX = 1024
_warn_lock = threading.Lock()
_warned_store_ids: "dict" = {}


def _assert_usable_store_id(user_id: Any) -> None:
    if not isinstance(user_id, str) or not _js_trim(user_id):
        raise ValueError(
            f"user_id must be a non-blank string; got {user_id!r}. Omit it to use the client's default "
            "store, or pass a real store id — anything else would silently write into the default store."
        )


def _warn_if_store_id_collapses(sid: str) -> None:
    if not sid:
        return
    valid = _STORE_ID_FORMAT.match(sid) is not None
    normalized = _normalize_store_id(sid)
    if valid and normalized == sid:
        return
    with _warn_lock:
        if sid in _warned_store_ids:
            return
        _warned_store_ids[sid] = None
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


def _check_filters(filters: Any) -> None:
    _warn_on_unknown_filters(filters)
    m = filters.get("min_importance") if isinstance(filters, dict) else None
    if m is not None and (isinstance(m, bool) or not isinstance(m, (int, float)) or not 0 <= m <= 1):
        shown = _jq(m) if isinstance(m, str) else _js_str(m)
        raise ValueError(f"filters.min_importance must be a number between 0 and 1, got {shown}.")


_KNOWN_FILTER_KEYS = frozenset({"categories", "event_from", "event_to", "time_from", "time_to", "min_importance", "speakers"})
_warned_filter_keys: set = set()
_WARNED_FILTER_KEYS_MAX = 1024


def _first_sighting(seen: set, key: Any) -> bool:
    with _warn_lock:
        if key in seen:
            return False
        seen.add(key)
        while len(seen) > _WARNED_FILTER_KEYS_MAX:
            seen.pop()
    return True


def _warn_on_unknown_filters(filters: Any) -> None:
    if not isinstance(filters, dict):
        return
    for k in filters:
        if k in _KNOWN_FILTER_KEYS or not _first_sighting(_warned_filter_keys, k):
            continue
        logging.getLogger("wontopos").warning(
            "unknown search filter %r — the API drops keys it does not know, so this filter has "
            "NO effect and the search is wider than you think. Known keys: %s",
            k, ", ".join(sorted(_KNOWN_FILTER_KEYS)),
        )


SEARCH_LIMIT_MIN = 5
SEARCH_LIMIT_MAX = 20

CONTEXT_LIMIT_MIN = 0
CONTEXT_LIMIT_MAX = 20


def _json_default(o: Any) -> Any:
    if _is_int(o):
        return int(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _check_count(limit: int, name: str = "limit") -> None:
    if not _is_int(limit):
        raise ValueError(f"{name} must be an int, got {type(limit).__name__}")
    if limit < SEARCH_LIMIT_MIN or limit > SEARCH_LIMIT_MAX:
        raise ValueError(
            f"{name} must be an integer between {SEARCH_LIMIT_MIN} and {SEARCH_LIMIT_MAX}, got {limit}. "
            "Out of range is refused rather than adjusted, so a short answer always "
            "means the store was short."
        )


def _check_context_limit(n: int) -> None:
    if not _is_int(n):
        raise ValueError(f"context_limit must be an int, got {type(n).__name__}")
    if n < CONTEXT_LIMIT_MIN or n > CONTEXT_LIMIT_MAX:
        raise ValueError(f"context_limit must be an integer between {CONTEXT_LIMIT_MIN} and {CONTEXT_LIMIT_MAX}, got {n}.")


_PAGE_MIN, _PAGE_MAX = 5, 20
_LIST_MIN, _LIST_MAX, _LIST_DEFAULT = 1, 500, 100
_MAX_IMAGES = 5
_VERIFY_MAX = 3


def _check_int(value: Any, name: str, lo: int, hi: int) -> None:
    if not _is_int(value) or not lo <= value <= hi:
        raise ValueError(f"{name} must be an integer between {lo} and {hi}, got {_js_str(value)}.")


def _check_page(value: Any, name: str = "limit") -> None:
    _check_int(value, name, _PAGE_MIN, _PAGE_MAX)


def _page_fields(limit: Any, before: Any, skip_ids: Any) -> dict:
    out: dict = {}
    if limit is not None:
        _check_page(limit)
        out["limit"] = limit
    if before is not None:
        out["before"] = before
    if skip_ids is not None:
        out["skip_ids"] = skip_ids
    return out


def _check_include(include: Any) -> None:
    if include is not None and include not in ("revised", "unrevised"):
        raise ValueError(f'include must be "revised" or "unrevised", got {include!r}.')


def _list_size(value: Any, name: str) -> int:
    if value is None:
        return _LIST_DEFAULT
    _check_int(value, name, _LIST_MIN, _LIST_MAX)
    return value


def _recall_body(store_id: str, query: str, form: Optional[str], tz: Optional[float],
                 limit: Optional[int], context_limit: Optional[int],
                 rooms: Any = None, client_tz: Optional[float] = None, *, room: Any = None,
                 room_limit: Any = None, room_expand_tokens: Any = None, long_term: Any = None,
                 long_term_cutoff: Any = None, today_start: Any = None, now: Any = None,
                 short_term_limit: Any = None, short_term_budget_tokens: Any = None,
                 max_images: Any = None, filters: Any = None, speaker: Any = None,
                 include_superseded: Any = None) -> dict:
    body: dict = {"user_id": store_id, "query": query}
    if limit is not None:
        _check_count(limit)
        body["limit"] = limit
    if context_limit is not None:
        _check_context_limit(context_limit)
        body["context_limit"] = context_limit
    for name, v, lo, hi in (("room_limit", room_limit, 1, 20),
                            ("room_expand_tokens", room_expand_tokens, 0, 20000),
                            ("short_term_limit", short_term_limit, 0, 500),
                            ("short_term_budget_tokens", short_term_budget_tokens, 0, 8000),
                            ("max_images", max_images, 0, _MAX_IMAGES)):
        if v is not None:
            _check_int(v, name, lo, hi)
            body[name] = v
    for name, v in (("long_term", long_term), ("long_term_cutoff", long_term_cutoff),
                    ("include_superseded", include_superseded)):
        if v is not None:
            body[name] = _check_flag(v, name)
    for name, v in (("today_start", today_start), ("now", now)):
        if v is not None:
            body[name] = _required(v, name)
    if filters is not None:
        _check_filters(filters)
        body["filters"] = filters
    if speaker is not None:
        body["speaker"] = speaker
    _look(body, rooms, tz, client_tz, room)
    return _form_body(body, form, tz if tz is not None else client_tz)


_ROOM_NAME = re.compile(r"[man][0-2]")
_ROOMS_MAX = 9
_AROUND_MIN, _AROUND_MAX = -200, 200


def _tz_minutes(tz: Any, name: str = "tz") -> int:
    if isinstance(tz, bool) or not isinstance(tz, (int, float)) or not -14 <= tz <= 14:
        raise ValueError(f"{name} must be a UTC offset in hours between -14 and 14 (e.g. 9, -4, 5.5), got {_js_str(tz)}.")
    minutes = tz * 60
    whole = math.floor(minutes)
    return whole + 1 if minutes - whole >= 0.5 else whole


def _clock_minutes(tz: Optional[float], client_tz: Optional[float]) -> Optional[int]:
    t = tz if tz is not None else client_tz
    return None if t is None else _tz_minutes(t)


def _default_tz(body: dict, client_tz: Optional[float]) -> dict:
    if body.get("tz") is None and client_tz is not None:
        body["tz"] = client_tz
    return body


def _filled(value: Any, name: str) -> str:
    if not isinstance(value, str) or not _js_trim(value):
        raise ValueError(f"{name} is required.")
    return value


def _required(value: Any, name: str) -> str:
    return _js_trim(_filled(value, name))


_WINDOW_ROLE = re.compile(r"[A-Za-z0-9_]{1,32}")


def _window_line(body: dict, window: Optional[bool], window_role: Optional[str]) -> dict:
    if window is not None:
        _check_flag(window, "window")
    if window_role is not None and window is not True:
        raise ValueError("window_role needs window=True: it names who said the line in the window.")
    if window is True:
        body["window"] = True
        if window_role is not None:
            if not isinstance(window_role, str) or not _WINDOW_ROLE.fullmatch(window_role):
                raise ValueError("window_role must be 1-32 characters of [A-Za-z0-9_].")
            body["window_role"] = window_role
    return body


def _write_extras(body: dict, tz: Optional[float], client_tz: Optional[float],
                  work_id: Optional[str]) -> dict:
    m = _clock_minutes(tz, client_tz)
    if m is not None:
        body["tz_offset_minutes"] = m
    if work_id is not None:
        body["work_id"] = _required(work_id, "work_id")
    return body


_SPEAKER_BREAK = re.compile("[\u0000-\u001f\u007f-\u009f\N{LINE SEPARATOR}\N{PARAGRAPH SEPARATOR}]")


def _one_line_speaker(name: str, what: str) -> str:
    if _SPEAKER_BREAK.search(name):
        raise ValueError(f"{what} contains a line break or a control character.")
    return name


def _turn_speaker(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip(_JS_SPACE):
        raise ValueError(f"{name} is required (a non-blank string).")
    s = value.strip(_JS_SPACE)
    if len(s.encode("utf-8", "surrogatepass")) > 256:
        raise ValueError(f"{name} is at most 256 bytes.")
    return _one_line_speaker(s, name)


_RFC3339_TIME = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-]([0-9]{2}):([0-9]{2}))"
)


def _real_time(s: str) -> bool:
    try:
        datetime(int(s[0:4]), int(s[5:7]), int(s[8:10]), int(s[11:13]), int(s[14:16]), int(s[17:19]))
    except ValueError:
        return False
    return True


def _turn_time(value: Any, name: str) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must carry a time zone (datetime.now(timezone.utc), not datetime.now()).")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    s = value.strip(_JS_SPACE) if isinstance(value, str) else None
    m = _RFC3339_TIME.fullmatch(s) if s else None
    if m is None or not _real_time(s) or int(m.group(1) or 0) > 23 or int(m.group(2) or 0) > 59:
        raise ValueError(
            f"{name} must be a datetime with a time zone or an RFC 3339 time with Z or a ±hh:mm offset, "
            f"such as 2026-05-02T09:00:00Z; a plain date or a time with no offset is refused, got {value!r}."
        )
    return s


def _turn_body(body: dict, created_at: Any, user_speaker: Any, assistant_speaker: Any) -> dict:
    if not any(isinstance(body[k], str) and _js_trim(body[k]) for k in ("user_msg", "assistant_msg")):
        raise ValueError("user_msg and assistant_msg are both blank: there is no turn to store.")
    if created_at is not None:
        body["created_at"] = _turn_time(created_at, "created_at")
    if user_speaker is not None:
        body["user_speaker"] = _turn_speaker(user_speaker, "user_speaker")
    if assistant_speaker is not None:
        body["assistant_speaker"] = _turn_speaker(assistant_speaker, "assistant_speaker")
    return body


def _check_rooms(rooms: Any) -> Any:
    if rooms is True or rooms is False or (isinstance(rooms, str) and rooms == "all"):
        return rooms
    many = isinstance(rooms, (list, tuple))
    names = list(rooms) if many else [rooms]
    if not 1 <= len(names) <= _ROOMS_MAX or not all(isinstance(r, str) and _ROOM_NAME.fullmatch(r) for r in names):
        raise ValueError(
            'rooms must be "all" or room names like "m0" (this morning) or "a1" and "n1" '
            f"(yesterday afternoon and night), got {_jq(names if many else rooms)}."
        )
    return names if many else rooms


def _one_room(room: Any) -> dict:
    if not isinstance(room, str) or not _ROOM_NAME.fullmatch(room):
        raise ValueError(f'room must be one room name like "m0" or "n1", got {_jq(room)}.')
    return {"days_ago": int(room[1]), "slot": "man".index(room[0])}


def _look(body: dict, rooms: Any, tz: Optional[float], client_tz: Optional[float],
          room: Any = None) -> dict:
    m = _clock_minutes(tz, client_tz)
    if room is not None:
        if rooms is False:
            raise ValueError("room names the one room to look in, so it cannot go with rooms=False.")
        body["room"] = _one_room(room)
    elif rooms is None:
        return body
    elif rooms is False:
        body["rooms"] = False
        return body
    if rooms is not None:
        body["rooms"] = _check_rooms(rooms)
    if m is not None:
        body["tz_offset_minutes"] = m
    return body


def _put_offsets(body: dict, from_: Any, to: Any, center: Any) -> dict:
    for wire, arg, v in (("from", "from_", from_), ("to", "to", to), ("center", "center", center)):
        if v is not None:
            _check_int(v, arg, _AROUND_MIN, _AROUND_MAX)
            body[wire] = v
    return body


def _around_body(store_id: str, memory_id: Any, from_: Any, to: Any, center: Any,
                 held: Any) -> dict:
    body: dict = {
        "user_id": store_id,
        "memory_id": _require_memory_id(memory_id, "the id a search, recall or work returned"),
    }
    _put_offsets(body, from_, to, center)
    if held is not None:
        if not isinstance(held, (list, tuple)) or len(held) != 2:
            raise ValueError("held must be [first, last].")
        for i, v in enumerate(held):
            _check_int(v, f"held[{i}]", _AROUND_MIN, _AROUND_MAX)
        body["held"] = list(held)
    return body


def _retrievals_body(store_id: str, limit: Any, before: Any) -> dict:
    body: dict = {"user_id": store_id}
    if limit is not None:
        _check_int(limit, "limit", 1, 200)
        body["limit"] = limit
    if before is not None:
        body["before"] = _required(before, "before")
    return body


def _work_around_body(store_id: str, work_id: Any, n: Any, from_: Any, to: Any,
                      center: Any) -> dict:
    _check_int(n, "n", 1, 200)
    body: dict = {"user_id": store_id, "work_id": _required(work_id, "work_id"), "n": n}
    return _put_offsets(body, from_, to, center)


def _close_work_body(store_id: str, work_id: Any, answer_id: Any) -> dict:
    body: dict = {"user_id": store_id, "work_id": _required(work_id, "work_id")}
    if answer_id is not None:
        body["answer_id"] = _required(answer_id, "answer_id")
    return body


_DIVERSITIES = ("none", "mmr", "temporal", "full")


def _tools_diversity(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or value not in _DIVERSITIES:
        raise ValueError(f'diversity must be "none", "mmr", "temporal" or "full", got {_jq(value)}.')
    return str(value)


def _open_work_body(store_id: str, question: Any, rooms: Any, tz: Optional[float], client_tz: Optional[float],
                    max_images: Optional[int] = None, own_words: Optional[bool] = None,
                    diversity: Optional[str] = None) -> dict:
    body: dict = {"user_id": store_id, "question": _required(question, "question")}
    if max_images is not None:
        _check_int(max_images, "max_images", 0, _MAX_IMAGES)
        body["max_images"] = max_images
    body = _look(body, rooms, tz, client_tz)
    if own_words is not None and not _check_flag(own_words, "own_words"):
        body["own_words"] = False
    diversity = _tools_diversity(diversity)
    if diversity is not None:
        body["diversity"] = diversity
    return body


_EVIDENCE_MAX, _EVIDENCE_CHARS_MAX = 200, 64


def _sleep_verify_body(store_id: str, sleep_id: Any, evidence: Any) -> dict:
    body: dict = {"user_id": store_id, "sleep_id": _required(sleep_id, "sleep_id")}
    if not isinstance(evidence, (list, tuple)) or len(evidence) > _EVIDENCE_MAX:
        got = len(evidence) if isinstance(evidence, (list, tuple)) else type(evidence).__name__
        raise ValueError(f"evidence must be a list of up to {_EVIDENCE_MAX} strings, got {got}.")
    for i, e in enumerate(evidence):
        size = len(e) if isinstance(e, str) else None
        if size is None or not 1 <= size <= _EVIDENCE_CHARS_MAX:
            raise ValueError(
                f"evidence[{i}] must be a string of 1 to {_EVIDENCE_CHARS_MAX} characters, got "
                + (type(e).__name__ if size is None else f"{size}") + "."
            )
    body["evidence"] = list(evidence)
    return body


def _work_result(r: Any) -> dict:
    out = dict(r) if isinstance(r, dict) else {}
    if "memories" in out:
        out["memories"] = _as_records(out["memories"])
    return out


_KNOWN_SEARCH_KEYS = frozenset({"cache_control", "speaker", "filters", "verify", "max_images", "form", "tz", "extra"})
_NOT_METADATA = frozenset({"userid", "storeid", "idempotencykey"})


def _names_a_store(key: Any) -> bool:
    return isinstance(key, str) and re.sub(f"[{_JS_SPACE_CLASS}_.\\-]", "", key.lower()) in _NOT_METADATA


_KEPT_METADATA_KEYS = frozenset({"speaker", "event_date", "category", "conversation_id", "created_at"})
_TURN_SIDES = ("user_speaker", "assistant_speaker")
_warned_metadata_keys: set = set()


def _warn_on_unknown_metadata(md: dict) -> None:
    for k in md:
        if k in _KEPT_METADATA_KEYS or not _first_sighting(_warned_metadata_keys, k):
            continue
        logging.getLogger("wontopos").warning(
            "metadata key %r is not kept: the service stores only %s and drops other keys, "
            "so this value is lost.",
            k, ", ".join(sorted(_KEPT_METADATA_KEYS)),
        )


def _metadata(metadata: Optional[dict], extra: dict, created_at: Any = None) -> dict:
    bad = sorted(k for k in {**(metadata or {}), **extra} if _names_a_store(k))
    extra = {k: v for k, v in extra.items() if v is not None}
    md = {**metadata, **extra} if metadata else extra
    if bad:
        raise ValueError(
            f"{bad[0]!r} is not a metadata field: pass the store as user_id= and an "
            "idempotency key as idempotency_key=. Nothing was sent."
        )
    for side in _TURN_SIDES:
        if md.get(side) is not None:
            raise ValueError(f"{side} is for add_turn: it names a side of a turn. On add, name who said it with speaker=.")
    if created_at is not None:
        md["created_at"] = created_at
    if md.get("created_at") is not None:
        md["created_at"] = _turn_time(md["created_at"], "created_at")
    if isinstance(md.get("speaker"), str):
        md["speaker"] = _turn_speaker(md["speaker"], "speaker")
    _warn_on_unknown_metadata(md)
    return md
_RESERVED_SEARCH_KEYS = frozenset({"user_id", "query", "max_results"})


def _edit_within(a: str, b: str, max_edits: int) -> bool:
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
    for k in opts:
        if k in _RESERVED_SEARCH_KEYS:
            raise ValueError(f"{k!r} is set by the call, not by options — pass it as an argument.")
        if k in _KNOWN_SEARCH_KEYS:
            continue
        near = next((n for n in sorted(_KNOWN_SEARCH_KEYS) if n != "extra" and _edit_within(k, n, 2)), None)
        raise ValueError(
            f"unknown search option {k!r}"
            + (f" — did you mean {near!r}?" if near else "")
            + ". The service drops keys it does not know and answers anyway, so this "
            "would have looked like it worked. Pass it under 'extra' if the service "
            "accepts it and this client does not know it yet."
        )


def _search_body(store_id: str, query: str, limit: Optional[int], opts: dict,
                 verify: Optional[int] = None, max_images: Optional[int] = None) -> dict:
    _check_search_opts(opts)
    extra = opts.get("extra") or {}
    known = {k: v for k, v in opts.items() if k != "extra" and v is not None}
    body = {**extra, **known}
    _check_filters(body.get("filters"))
    if limit is None:
        limit = 10
    _check_count(limit)
    body.update(user_id=store_id, query=query, max_results=limit)
    if verify is not None:
        body["verify"] = verify
    if max_images is not None:
        body["max_images"] = max_images
    if body.get("max_images") is not None:
        _check_int(body["max_images"], "max_images", 0, _MAX_IMAGES)
    if body.get("verify") is not None:
        _check_int(body["verify"], "verify", 0, _VERIFY_MAX)
    if body.get("tz") is not None:
        _tz_minutes(body["tz"])
    return body


_DEFAULT_SPEAKER_KEYS = ("user", "assistant")


def _default_speakers(value: Any) -> dict:
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f'default_speakers must be a dict with "user", "assistant" or both, got {value!r}.')
    out: dict = {}
    for k, v in value.items():
        if k not in _DEFAULT_SPEAKER_KEYS:
            raise ValueError(f'default_speakers takes "user" and "assistant" only, got {k!r}.')
        if v is not None and (not isinstance(v, str) or not _js_trim(v)):
            raise ValueError(f"default_speakers[{k!r}] must be a speaker's name, or None to clear it, got {v!r}.")
        out[k] = None if v is None else _turn_speaker(v, f"default_speakers[{k!r}]")
    return out


def _settings_body(store_id: str, time_zone: Any, default_speakers: Any, confirmed_by_user: Any) -> dict:
    if time_zone is None and default_speakers is None:
        raise ValueError("set_settings needs time_zone, default_speakers or both.")
    body: dict = {"user_id": store_id}
    if time_zone is not None:
        body["time_zone"] = _required(time_zone, "time_zone")
    if default_speakers is not None:
        body["default_speakers"] = _default_speakers(default_speakers)
    if confirmed_by_user is not None:
        body["confirmed_by_user"] = _check_flag(confirmed_by_user, "confirmed_by_user")
    return body


_RELABEL_IDS_MAX = 1000


def _relabel_body(store_id: str, from_speaker: Any, to_speaker: Any, memory_ids: Any) -> dict:
    body: dict = {"user_id": store_id, "from": _required(from_speaker, "from_speaker"),
                  "to": _turn_speaker(_required(to_speaker, "to_speaker"), "to_speaker")}
    if memory_ids is not None:
        if not isinstance(memory_ids, (list, tuple)):
            raise ValueError(f"memory_ids must be a list of memory ids, got {type(memory_ids).__name__}.")
        if not 1 <= len(memory_ids) <= _RELABEL_IDS_MAX:
            raise ValueError(f"memory_ids must hold 1 to {_RELABEL_IDS_MAX} memory ids, got {len(memory_ids)}.")
        body["memory_ids"] = [_required(m, f"memory_ids[{i}]") for i, m in enumerate(memory_ids)]
    return body


def _speaker_names(speaker: Any) -> list:
    many = isinstance(speaker, (list, tuple))
    names = list(speaker) if many else [speaker]
    if not names:
        raise ValueError("speaker must be a name or a list of names, got an empty list.")
    return [_required(n, f"speaker[{i}]" if many else "speaker") for i, n in enumerate(names)]


def _named_store(user_id: Any, refusal: str) -> str:
    if not isinstance(user_id, str) or not _js_trim(user_id):
        raise ValueError(refusal)
    _warn_if_store_id_collapses(user_id)
    return user_id


def _forget_speaker_body(speaker: Any, user_id: Any, preview: Any) -> dict:
    user_id = _named_store(
        user_id, "user_id is required (a non-blank string) for forget_speaker — it never falls back to the default store."
    )
    body: dict = {"user_id": user_id, "speakers": _speaker_names(speaker)}
    if _check_flag(preview, "preview"):
        body["preview"] = True
    return body


_RESENDS_WHILE_INCOMPLETE = 3


def _incomplete(r: Any) -> bool:
    return isinstance(r, dict) and r.get("complete") is False


_SEGMENT_KEYS = ("content", "speaker", "created_at")


def _segments(segments: Any) -> list:
    if not isinstance(segments, (list, tuple)):
        raise ValueError(f"segments must be a list of pieces, got {type(segments).__name__}.")
    if not segments:
        raise ValueError("segments must hold at least one piece; for one text, pass content instead.")
    out = []
    for i, s in enumerate(segments):
        if not isinstance(s, Mapping):
            raise ValueError(
                f'segments[{i}] must be a dict like {{"content": ..., "speaker": ..., "created_at": ...}}, '
                f"got {type(s).__name__}."
            )
        for k in s:
            if k not in _SEGMENT_KEYS:
                raise ValueError(f"segments[{i}] has {k!r}, which a piece does not take: only content, "
                                 "speaker and created_at.")
        content = s.get("content")
        if not isinstance(content, str) or not _js_trim(content):
            raise ValueError(f'segments[{i}]["content"] is required (a non-blank string).')
        piece: dict = {"content": content}
        if s.get("speaker") is not None:
            piece["speaker"] = _turn_speaker(s["speaker"], f'segments[{i}]["speaker"]')
        if s.get("created_at") is not None:
            piece["created_at"] = _required(s["created_at"], f'segments[{i}]["created_at"]')
        out.append(piece)
    return out


def _bulk_body(store_id: str, content: Any, category: Optional[str], timestamp: Optional[str],
               segments: Any, speaker: Any) -> dict:
    if segments is not None:
        if content is not None:
            raise ValueError("add_bulk takes content or segments, not both.")
        if speaker is not None:
            raise ValueError("speaker goes with content; with segments, each piece names its own speaker.")
        body: dict = {"user_id": store_id, "segments": _segments(segments)}
    elif content is None:
        raise ValueError("add_bulk needs content (one text) or segments (a conversation in its pieces).")
    else:
        body = {"user_id": store_id, "content": content}
        if speaker is not None:
            body["metadata"] = {"speaker": _turn_speaker(speaker, "speaker")}
    body["category"] = "general" if category is None else category
    if timestamp:
        body["timestamp"] = timestamp
    return body


def _normalize_image(image: Any) -> dict:
    if not isinstance(image, dict):
        raise TypeError("image must be a dict — {'data': '<base64>'}")
    data = image.get("data")
    if not isinstance(data, str) or not _js_trim(data):
        raise ValueError("image['data'] is required — base64 of the image (a data: URL is fine)")
    data = _js_trim(data)
    if data.startswith("data:"):
        comma = data.find(",")
        if comma != -1:
            data = data[comma + 1:]
    data = re.sub(f"[{_JS_SPACE_CLASS}]+", "", data)
    if not data:
        raise ValueError("image['data'] is empty after stripping its data: prefix")
    out: dict = {"data": data}
    for k in ("reference", "taken_at"):
        if image.get(k) is not None:
            out[k] = image[k]
    return out


def _idem_headers(key: Optional[str]) -> Optional[dict]:
    if key is None:
        return None
    if not isinstance(key, str) or not _IDEMPOTENCY_KEY_RE.fullmatch(key):
        raise ValueError(f"invalid idempotency_key: {key!r} — 1-128 chars of [A-Za-z0-9._:-]")
    return {"Idempotency-Key": key}
_NO_BODY_STATUS = frozenset({204, 205, 304})
_MODEL_RE = re.compile(r"[A-Za-z0-9._-]+")
_ENV_KEYS = ("WONTOPOS_API_KEY", "WOS_API_KEY")


def _mask_key(key: str) -> str:
    return f"***{key[-4:]}" if len(key) >= 24 else "***"


def _clean_key(api_key: str) -> str:
    if not api_key or not _js_trim(api_key):
        raise ValueError("api_key is required")
    key = _js_trim(api_key)
    if any(c in _JS_SPACE for c in key):
        raise ValueError("api_key contains whitespace - check for a stray newline or paste error")
    if _HEADER_CTL_RE.search(key):
        raise ValueError("api_key contains a control character - check for a paste error")
    if not key.isascii():
        bad = next(c for c in key if not c.isascii())
        raise ValueError(
            f"api_key contains a non-ASCII character ({bad!r}) - rich text turns '-' into an en dash; "
            "copy the key from a plain-text field"
        )
    if key.startswith("sk-"):
        raise ValueError("api_key starts with sk-, so it is a model provider's key: give the Wontopos key here (it starts "
                         "with wos-).")
    return key


def _check_model(model: Optional[str]) -> Optional[str]:
    if model and not _MODEL_RE.fullmatch(model):
        raise ValueError(f"invalid model name: {model!r} (letters, digits, '.', '_', '-' only)")
    return model


def _as_dict(x: Any) -> dict:
    return x if isinstance(x, dict) else {}


def _memory_from_get(r: Any) -> dict:
    r = _as_dict(r)
    if isinstance(r.get("memory"), dict):
        return r["memory"]
    return r if isinstance(r.get("id"), str) else {}


def _copy_client(obj: Any, owner_flag: str, shared: tuple, memo: Optional[dict]) -> Any:
    new = object.__new__(type(obj))
    if memo is not None:
        memo[id(obj)] = new
    for k, v in obj.__dict__.items():
        new.__dict__[k] = v if memo is None or k in shared else _copy.deepcopy(v, memo)
    new.__dict__[owner_flag] = False
    return new


def _as_records(x: Any) -> list:
    return [m for m in _as_list(x) if isinstance(m, dict)]


def _with_count(p: Any, key: str, rows: list) -> dict:
    out = dict(p) if isinstance(p, dict) else {}
    out[key] = rows
    count = out.get("count")
    if isinstance(count, bool) or not isinstance(count, (int, float)):
        out["count"] = len(rows)
    return out


def _recall_shape(r: Any) -> dict:
    out = dict(r) if isinstance(r, dict) else {}
    for part, key in (("short_term", "turns"), ("long_term", "memories"), ("context", "around_top_memory")):
        p = out.get(part)
        rows = _as_list(p.get(key) if isinstance(p, dict) else None)
        out[part] = _with_count(p, key, [x for x in rows if isinstance(x, str)] if part == "context" else _as_records(rows))
    for part in ("rooms", "images"):
        p = out.get(part)
        if isinstance(p, dict):
            out[part] = _with_count(p, "memories", _as_records(p.get("memories")))
    return out


def _memory_page(r: Any) -> dict:
    out = _with_count(r, "memories", _as_records(r.get("memories") if isinstance(r, dict) else None))
    if out.get("next_cursor") is None or out.get("next_cursor") == "":
        out["next_cursor"] = None
    return out


_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def _split_id_args(user_id: Optional[str], memory_id: str) -> tuple[Optional[str], str]:
    if memory_id is _NO_ID and isinstance(user_id, str) and _UUID_RE.match(user_id):
        return None, user_id
    return user_id, memory_id


def _merge_results(resp: Any, filtered: bool = False) -> list:
    if not isinstance(resp, dict):
        return []
    out = _as_records(resp.get("memories"))
    seen = {m["id"] for m in out if isinstance(m.get("id"), str)}
    own = [] if filtered else _as_records(resp.get("self_memories"))
    for m in own + _as_records(resp.get("images")):
        mid = m.get("id")
        if isinstance(mid, str):
            if mid in seen:
                continue
            seen.add(mid)
        out.append(m)
    return out


def _form_body(body: dict, form: Optional[str], tz: Optional[float]) -> dict:
    if form is not None:
        body["form"] = form
    if tz is not None:
        _tz_minutes(tz)
        body["tz"] = tz
    return body


_HEADER_CTL_RE = re.compile(r"[\x00-\x1f\x7f]")


def _require_memory_id(memory_id: object, came_from: str) -> str:
    if not isinstance(memory_id, str) or not _js_trim(memory_id):
        raise ValueError(f"memory_id is required (non-blank) — {came_from}.")
    return _js_trim(memory_id)


def _supersede_body(client: Any, user_id: Optional[str], old_memory_id: Any, new_content: Any, tz: Optional[float],
                    work_id: Optional[str]) -> dict:
    if work_id is not None:
        raise ValueError("work_id is for add and add_turn: a correction does not answer a work.")
    body = {"user_id": client._uid(user_id), "old_memory_id": _required(old_memory_id, "old_memory_id"),
            "new_content": _filled(new_content, "new_content")}
    return _write_extras(body, tz, client._tz, None)



def _check_keys(d: Mapping, known: tuple, what: str) -> None:
    for k in d:
        if k in known:
            continue
        near = next((n for n in known if isinstance(k, str) and _edit_within(k, n, 2)), None)
        hint = f" — did you mean {_jq(near)}?" if near else ""
        raise ValueError(
            f"unknown {what} option {_jq(k)}{hint}. It would have had no effect, and the call would have "
            "looked like it worked."
        )


def _byte_length(s: str) -> int:
    return len(s.encode("utf-8", "surrogatepass"))


def _sleep_slot(slot: Any, what: str = "slot") -> int:
    if not _is_int(slot) or not 0 <= slot <= 4:
        raise ValueError(f"{what} must be 0 (the core) or 1 to 4 (a box), got {slot!r}.")
    return int(slot)


def _sleep_open_body(store: str, slot: Any, version: Any) -> dict:
    body: dict = {"user_id": store, "slot": _sleep_slot(slot)}
    if version is not None:
        if not _is_int(version) or version < 1:
            raise ValueError(f"version must be a whole number of 1 or more, got {version!r}.")
        body["version"] = int(version)
    return body


_SLEEP_WRITE_KEYS = ("slot", "base_version", "title", "body", "evidence", "day")


def _sleep_writes(writes: Any) -> list:
    if not isinstance(writes, (list, tuple)) or not 1 <= len(writes) <= len(_SLEEP_SLOTS):
        raise ValueError(f"writes must be a list of 1 to {len(_SLEEP_SLOTS)} writes, one a file.")
    out, slots = [], set()
    for i, w in enumerate(writes):
        what = f"writes[{i}]"
        if not isinstance(w, Mapping):
            raise ValueError(f"{what} must be a dict.")
        _check_keys(w, _SLEEP_WRITE_KEYS, what)
        slot = _sleep_slot(w.get("slot"), f"{what}.slot")
        if slot in slots:
            raise ValueError(f"{what}.slot {slot} is written twice: one write a file.")
        slots.add(slot)
        base = w.get("base_version")
        if not _is_int(base) or base < 0:
            raise ValueError(f"{what}.base_version must be the version read, 0 or more, got {base!r}.")
        entry: dict = {"slot": slot, "base_version": int(base)}
        for key in ("title", "body"):
            v = w.get(key, "" if key == "title" else None)
            if not isinstance(v, str):
                raise ValueError(f"{what}.{key} must be a str.")
            entry[key] = v
        ev = w.get("evidence", [])
        if not isinstance(ev, (list, tuple)) or not all(isinstance(x, str) and x for x in ev):
            raise ValueError(f"{what}.evidence must be a list of memory ids.")
        entry["evidence"] = list(ev)
        if w.get("day") is not None:
            if _sleep_date(w["day"]) is None:
                raise ValueError(f'{what}.day must be "YYYY-MM-DD", got {w["day"]!r}.')
            entry["day"] = w["day"]
        out.append(entry)
    return out


def _sleep_notes_body(store: str, base_version: Any, notes: Any) -> dict:
    if not _is_int(base_version) or base_version < 0:
        raise ValueError(f"base_version must be the version read, 0 or more, got {base_version!r}.")
    if not isinstance(notes, Mapping):
        raise ValueError("notes must be a dict: proposals and mixed.")
    return {"user_id": store, "base_version": int(base_version), "notes": dict(notes)}
_GET_NEEDS_ID = (
    "memory_id is required — the id that add/store or list_memories returned. "
    "Note the argument order: get(user_id, memory_id). With a store set on the "
    'client, call get(memory_id="...").'
)
_DELETE_NEEDS_ID = (
    "memory_id is required (non-blank). To delete every memory in a store, call delete_all(user_id) explicitly. "
    "Note the argument order: delete(user_id, memory_id)."
)
_LISTED_NEEDS_ID = "memory_id is required (non-blank) — the id that add/store or list_memories returned."
_IMAGE_NEEDS_ID = "memory_id is required (non-blank) — the id that add/store or list_images returned."
_UNIMAGE_NEEDS_ID = "memory_id is required (non-blank) — the id of the memory whose image you want removed."
_DELETE_STORE_NEEDS_ID = "user_id is required (a non-blank string) — delete_store never falls back to the default store."
_DELETE_ALL_NEEDS_ID = ("user_id is required (a non-blank string) for delete_all — anything else would wipe the default "
                        "store.")


def _id_body(client: Any, user_id: Optional[str], memory_id: Any, refusal: str) -> dict:
    user_id, memory_id = _split_id_args(user_id, memory_id)
    if not isinstance(memory_id, str) or not _js_trim(memory_id):
        raise ValueError(refusal)
    return {"user_id": client._uid(user_id), "memory_id": _js_trim(memory_id)}


_MAX_SECONDS = 2147483


def _shown_arg(value: Any) -> str:
    return _jq(value) if isinstance(value, str) else _js_str(value)


def _check_seconds(value: Any, name: str) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, numbers.Real) or math.isnan(value) or value < 0:
        raise ValueError(f"{name} must be a number of seconds, 0 or more, got {_shown_arg(value)}.")
    if value == 0:
        return None
    return value if value <= _MAX_SECONDS else _MAX_SECONDS


def _check_retries(value: Any, name: str) -> Optional[int]:
    if value is None:
        return None
    if not _is_whole(value) or value < 0:
        raise ValueError(f"{name} must be a whole number, 0 or more, got {_shown_arg(value)}.")
    return int(value)


def _client_retries(retries: Any, max_retries: Any) -> int:
    max_retries = _check_retries(max_retries, "max_retries")
    retries = _check_retries(retries, "retries")
    if retries is None:
        retries = 2 if max_retries is None else max_retries
    return retries
