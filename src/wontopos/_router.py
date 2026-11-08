from __future__ import annotations

import asyncio
import inspect
import os
import re
import threading
import time
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence, Union

from . import (
    AskSignal, ModelCall, WosError, _ANTHROPIC_BASE_URL, _ASK_MODEL, _AskClock, _EFFORTS, _EXCHANGE_ANSWER_CHARS,
    _EXCHANGE_MAX_CHARS, _FRAME_TOKENS, _JS_SPACE, _JS_SPACE_CLASS, _KNOWN_LIMIT_KEYS, _KNOWN_ROUTER_KEYS, _MAX_SECONDS,
    _MODEL_MAX_TOKENS, _ROUTER_MAPS_MAX, _ROUTER_MAX_TOKENS, _ROUTER_PROMPT_MAX_TOKENS, _ROUTER_STABLE_MAX_TOKENS,
    _ROUTER_TEXT, _SKILLS_EXPORT_VERSION, _SKILL_FIELDS, _SKILL_MAX_TOKENS, _SKILL_MD_NOT_RUN, _SKILL_NAME_RE,
    _SPEAKER_MAX_BYTES, _TIMEOUT_ERRORS, _TOKEN_COUNTS_MAX, _as_caller, _async_sleep, _await_within, _awaited,
    _check_model_name, _check_whole, _copy_json, _first_set, _is_async_callable, _is_number, _is_whole, _jq, _js_json,
    _js_str, _js_string, _js_trim, _nests_deeper, _provider_base, _reply_shape, _thread_within, _time, _utf16_len,
)
from ._callers import (
    _AnthropicCaller, _CONTEXT_WORDS, _PROVIDER_REASONS, _check_model_key, _clean_capped, _provider_url,
)
from ._checks import (
    _as_dict, _as_records, _byte_length, _check_keys, _edit_within, _normalize_store_id, _one_line_speaker, _required,
    _tz_minutes, _warn_if_store_id_collapses,
)
from ._memory_tools import _MISSING, _call_field, _instant, _json_loads, _kept_in, _utc_label


class SkillContext:
    """What a code skill's function gets beside its input."""

    __slots__ = ("store", "request", "note", "ask_id", "speaker", "signal")

    def __init__(self, store: str, request: str, note: str, ask_id: str, speaker: Optional[str],
                 signal: AskSignal):
        self.store = store
        self.request = request
        self.note = note
        self.ask_id = ask_id
        self.speaker = speaker
        self.signal = signal

    def __repr__(self) -> str:
        return f"SkillContext(store={self.store!r}, ask_id={self.ask_id!r}, speaker={self.speaker!r})"


def ask_for_model(r: Mapping) -> dict:
    """What ``ask`` gives the model that asked."""
    return {"ok": r.get("ok"), **({"action": r["action"]} if r.get("action") else {}), "text": r.get("text"),
            **({"skill": r["skill"]} if r.get("skill") else {}), **({"truncated": True} if r.get("truncated") else {})}


def _model_failure(e: Any) -> "_Fail":
    status = getattr(e, "status", None)
    status = status if _is_number(status) else 0
    etype = getattr(e, "type", None)
    etype = etype if isinstance(etype, str) else ""
    m = getattr(e, "message", None)
    msg = _clean_capped(m if isinstance(m, str) else str(e), 512)
    detail = f"{_js_json(status)}{' ' + etype if etype else ''}: {msg}" if status else msg
    wait = getattr(e, "retry_after", None)
    wait = wait if _is_number(wait) and wait >= 0 else None
    reason = getattr(e, "reason", None)
    if isinstance(reason, str) and reason in _PROVIDER_REASONS:
        return _Fail(reason, detail, wait if reason == "rate_limit" else None)
    if not status:
        return _Fail("timeout" if isinstance(e, _TIMEOUT_ERRORS) else "network", detail)
    if etype == "billing_error" or status == 402 or (status == 400 and re.search("credit balance", msg, re.I)):
        return _Fail("credit", detail)
    if status == 400 and re.search(r"(usage|spend|spending) limit|spend cap", msg, re.I):
        return _Fail("spend_limit", detail)
    if status in (401, 403) or etype in ("authentication_error", "permission_error"):
        return _Fail("auth", detail)
    if status == 404:
        return _Fail("bad_model", detail)
    if status == 413:
        return _Fail("too_large", detail)
    if status == 429:
        if wait is None:
            return _Fail("spend_limit", f"{detail} (no Retry-After)")
        return _Fail("rate_limit", detail, wait)
    if status in (408, 529) or status >= 500 or etype == "overloaded_error":
        return _Fail("overloaded", detail)
    if status == 400 and _CONTEXT_WORDS.search(msg):
        return _Fail("too_large", detail)
    if status == 400:
        return _Fail("bad_request", detail)
    return _Fail("failed", detail)


class _Fail:

    __slots__ = ("reason", "detail", "retry_after")

    def __init__(self, reason: str, detail: Optional[str] = None, retry_after: Optional[float] = None):
        self.reason = reason
        self.detail = detail
        self.retry_after = retry_after


class _FnCaller:

    __slots__ = ("call",)
    model = None
    base_url = None
    count_tokens = None

    def __init__(self, fn: Any):
        self.call = fn


class _SkillRegistry:

    def __init__(self) -> None:
        self._stores: dict = {}

    def list(self, store: str) -> list:
        s = self._stores.get(_normalize_store_id(store))
        return [s[k] for k in sorted(s)] if s else []

    def get(self, store: str, name: str) -> Optional[dict]:
        s = self._stores.get(_normalize_store_id(store))
        return None if s is None else s.get(name)

    def add(self, store: str, defs: list) -> None:
        s = self._stores.setdefault(_normalize_store_id(store), {})
        for d in defs:
            s[d["name"]] = d

    def remove(self, store: str, name: str) -> bool:
        k = _normalize_store_id(store)
        s = self._stores.get(k)
        if not s or name not in s:
            return False
        del s[name]
        if not s:
            del self._stores[k]
        return True

    def any(self) -> bool:
        return bool(self._stores)


def _set_bounded(m: dict, k: Any, v: Any, cap: int) -> None:
    m.pop(k, None)
    m[k] = v
    while len(m) > cap:
        del m[next(iter(m))]


def _set_fresh(m: dict, k: Any, v: Any, cap: int, stale: Any) -> None:
    m.pop(k, None)
    if len(m) >= cap:
        for old in [x for x, y in m.items() if stale(y)]:
            del m[old]
    _set_bounded(m, k, v, cap)


def _check_limit_seconds(v: Any, what: str) -> float:
    if not _is_number(v) or v <= 0 or v > _MAX_SECONDS:
        raise ValueError(f"{what} must be a number of seconds over 0, at most {_MAX_SECONDS}, got {_js_str(v)}.")
    return float(v)


def _secs(x: float) -> str:
    return f"{x:g}"


class _Reserved:

    __slots__ = ("budget", "_answer", "_lock")

    def __init__(self, budget: float, answer: Optional[dict], lock: Any):
        self.budget = budget
        self._answer = answer
        self._lock = lock

    def done(self, spent: float) -> None:
        if self._answer is not None:
            with self._lock:
                self._answer["spent"] += max(0.0, spent)


class _RouterClock:

    __slots__ = ("label", "zone", "offset")

    def __init__(self, label: str, zone: Any = None, offset: int = 0):
        self.label = label
        self.zone = zone
        self.offset = offset


class _RouterState:

    def __init__(self, opts: Optional[Mapping], wontopos_base: str, wontopos_key: Optional[str] = None):
        if opts is not None and not isinstance(opts, Mapping):
            raise TypeError("router must be a dict of router options.")
        self.lock = threading.RLock()
        self.skills = _SkillRegistry()
        self.handlers: dict = {}
        self.callers: dict = {}
        self._tokens: dict = {}
        self._answers: dict = {}
        self._windows: dict = {}
        self._days: dict = {}
        self.configured = opts is not None
        o = {k: v for k, v in (opts or {}).items()}
        _check_keys(o, _KNOWN_ROUTER_KEYS, "router")
        given_base = o.get("provider_base_url")
        provider_base = _ANTHROPIC_BASE_URL if given_base is None else _provider_url(given_base, wontopos_base, False)

        def anthropic(k: Any, what: str) -> Any:
            key = _check_model_key(k, what)
            if key == wontopos_key:
                raise ValueError(f"{what} is this client's Wontopos key: a Wontopos key never goes to a model provider. "
                                 "Give the provider's own key.")
            return _AnthropicCaller(key, _provider_url(provider_base, wontopos_base), None, "5m")

        def own(c: Any, what: str) -> Any:
            caller = _as_caller(c, what)
            base = getattr(caller, "base_url", None)
            if base is not None:
                _provider_base(base, wontopos_base)
            return caller

        by_key = None if o.get("key") is None else anthropic(o["key"], "router.key")
        self.router = own(o["router_caller"], "router.router_caller") if o.get("router_caller") is not None else by_key
        if o.get("skill_caller") is not None:
            self.skill = own(o["skill_caller"], "router.skill_caller")
        elif o.get("skill_key") is not None:
            self.skill = anthropic(o["skill_key"], "router.skill_key")
        else:
            self.skill = by_key
        self.router_model = _check_model_name(_first_set(o.get("router_model"), getattr(self.router, "model", None),
                                                         _ASK_MODEL), "router.router_model")
        self.skill_model = _check_model_name(_first_set(o.get("skill_model"), getattr(self.skill, "model", None),
                                                        _ASK_MODEL), "router.skill_model")
        callers = o.get("callers")
        if callers is not None:
            if not isinstance(callers, Mapping):
                raise TypeError("router.callers must map names to callers.")
            for name, c in callers.items():
                if not isinstance(name, str) or not _SKILL_NAME_RE.fullmatch(name):
                    raise ValueError(f"router.callers: {_jq(name)} is not a name (1-32 of [a-z0-9_], a letter first).")
                self.callers[name] = own(c, f"router.callers.{name}")
        mt = o.get("max_tokens")
        self.max_tokens = _ROUTER_MAX_TOKENS if mt is None else _check_whole(
            mt, "router.max_tokens", _ROUTER_MAX_TOKENS, _MODEL_MAX_TOKENS)
        prompt = o.get("prompt")
        if prompt is not None and (not isinstance(prompt, str) or not _js_trim(prompt)):
            raise ValueError("router.prompt must be the whole router prompt: a non-blank string.")
        self.prompt = _ROUTER_TEXT["prompt"] if prompt is None else prompt
        name = o.get("name")
        if name is not None and (not isinstance(name, str) or not _js_trim(name)):
            raise ValueError("router.name must be a non-blank string.")
        self.name = None if name is None else _js_trim(name)
        tz = o.get("time_zone")
        self.time_zone, self.zone = (None, None) if tz is None else _check_time_zone(tz)
        handlers = o.get("handlers")
        if handlers is not None:
            if not isinstance(handlers, Mapping):
                raise TypeError("router.handlers must map skill names to functions.")
            for hname, fn in handlers.items():
                if not isinstance(hname, str) or not _SKILL_NAME_RE.fullmatch(hname):
                    raise ValueError(f"router.handlers: {_jq(hname)} is not a skill name (1-32 of [a-z0-9_], a letter first).")
                if not callable(fn):
                    raise TypeError(f"router.handlers.{hname} must be a function.")
                self.handlers[hname] = fn
        lim = o.get("limits")
        lim = {} if lim is None else lim
        if not isinstance(lim, Mapping):
            raise TypeError("router.limits must be a dict.")
        _check_keys(lim, _KNOWN_LIMIT_KEYS, "router.limits")

        def whole(k: str, default: int, hi: int) -> int:
            return default if lim.get(k) is None else _check_whole(lim[k], f"router.limits.{k}", 1, hi)

        def seconds(k: str, default: float) -> float:
            return default if lim.get(k) is None else _check_limit_seconds(lim[k], f"router.limits.{k}")

        self.limits = {
            "per_answer": whole("per_answer", 3, 1_000),
            "per_window": whole("per_window", 3, 1_000),
            "window": seconds("window", 60.0),
            "time": seconds("time", 75.0),
            "per_day": whole("per_day", 200, 1_000_000),
        }

    def __repr__(self) -> str:
        return f"<wontopos router: {'configured' if self.router is not None else 'not configured'}>"

    def __copy__(self) -> "_RouterState":
        return self

    def __deepcopy__(self, memo: dict) -> "_RouterState":
        return self

    def __reduce__(self) -> Any:
        if self.configured or self.skills.any():
            raise TypeError(
                "a client with a router or skills cannot be pickled: the router holds model keys and the "
                "app's functions. Build the client, its router and its skills in each process."
            )
        return (_RouterState, (None, ""))

    def tokens_key(self, c: ModelCall) -> str:
        import hashlib

        blob = _js_json([c.model, c.system, c.schema]).encode("utf-8", "surrogatepass")
        return hashlib.sha256(blob).hexdigest()

    def tokens_get(self, key: str) -> Optional[int]:
        return self._tokens.get(key)

    def tokens_put(self, key: str, n: int) -> None:
        with self.lock:
            _set_bounded(self._tokens, key, n, _TOKEN_COUNTS_MAX)

    def reserve(self, store: str, turn: Optional[str], day: str, now: float) -> Union[_Reserved, str]:
        with self.lock:
            L = self.limits
            d = self._days.get(store)
            today = d if d is not None and d["day"] == day else {"day": day, "count": 0}
            if today["count"] >= L["per_day"]:
                return f"this store has asked {L['per_day']} times today"
            budget = L["time"]
            answer = None
            if turn is not None:
                k = f"{store}\x00{turn}"
                answer = self._answers.get(k) or {"count": 0, "spent": 0.0}
                if answer["count"] >= L["per_answer"]:
                    return f"this answer has asked {L['per_answer']} times"
                budget = L["time"] - answer["spent"]
                if budget <= 0:
                    return f"this answer has used its {_secs(L['time'])} s"
                answer["count"] += 1
                answer["day"] = day
                _set_fresh(self._answers, k, answer, _ROUTER_MAPS_MAX, lambda a: a["day"] < day)
            else:
                w = [t for t in self._windows.get(store, ()) if now - t < L["window"]]
                if len(w) >= L["per_window"]:
                    return f"this store has asked {L['per_window']} times in {_secs(L['window'])} s"
                w.append(now)
                _set_fresh(self._windows, store, w, _ROUTER_MAPS_MAX, lambda ts: all(now - t >= L["window"] for t in ts))
            today["count"] += 1
            _set_fresh(self._days, store, today, _ROUTER_MAPS_MAX, lambda d: d["day"] < day)
            return _Reserved(budget, answer, self.lock)


_WEEKDAYS = ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")


def _check_time_zone(zone: Any) -> tuple:
    if not isinstance(zone, str) or not _js_trim(zone):
        raise ValueError('router.time_zone must be an IANA time zone name such as "Asia/Seoul".')
    name = _js_trim(zone)
    try:
        from zoneinfo import ZoneInfo, available_timezones

        try:
            return name, ZoneInfo(name)
        except Exception:
            folded = {z.lower(): z for z in available_timezones()}.get(name.lower())
            if folded is None:
                raise
            return name, ZoneInfo(folded)
    except Exception:
        raise ValueError(
            f'router.time_zone {_jq(zone)} is not a time zone this runtime knows: use an IANA name such as '
            '"Asia/Seoul".'
        ) from None


def _router_clock(client: Any) -> Optional[_RouterClock]:
    st = client._router
    if st.time_zone is not None:
        return _RouterClock(st.time_zone, zone=st.zone)
    if client._tz is None:
        return None
    m = _tz_minutes(client._tz)
    return _RouterClock(_utc_label(m), offset=m)


def _clock_parts(clock: _RouterClock, t: float) -> tuple:
    if clock.zone is not None:
        d = datetime.fromtimestamp(t, clock.zone)
    else:
        d = datetime.fromtimestamp(t, timezone.utc) + timedelta(minutes=clock.offset)
    return (f"{d.year:04d}-{d.month:02d}-{d.day:02d}", _WEEKDAYS[d.isoweekday() % 7],
            f"{d.hour:02d}:{d.minute:02d}:{d.second:02d}")


def _router_turn(exchanges: list, clock: _RouterClock, now: float, name: Optional[str], request: str) -> str:
    T = _ROUTER_TEXT
    parts = []
    if exchanges:
        lines = [T["earlier"]]
        for e in exchanges:
            at = _instant(e.get("at")) if isinstance(e.get("at"), str) else None
            try:
                date, _, hms = ("", "", "") if at is None else _clock_parts(clock, at / 1000)
            except (OverflowError, ValueError, OSError):
                at = None
            when = "?" if at is None else f"{date} {hms}"
            lines.append(f"[{when}] request: {_kept_in(e['request'])}\nanswer: {_kept_in(e['answer'])}")
        parts.append("\n".join(lines))
    else:
        parts.append(T["noEarlier"])
    date, weekday, hms = _clock_parts(clock, now)
    parts.append(f"{T['now']} {date} ({weekday}) {hms} {clock.label}")
    if name is not None:
        parts.append(f"{T['name']} {_kept_in(name)}")
    parts.append(f"{T['request']}\n{request}")
    return "\n\n".join(parts)


def _skill_list(skills: list) -> str:
    T = _ROUTER_TEXT
    lines = [T["skills"]]
    for s in skills:
        lines.append(f"- {s['name']}: {_kept_in(s['description'])}\n  {T['skillInput']} {_js_json(s['input'])}")
    return "\n".join(lines)


def _router_schema(names: list) -> dict:
    return {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["skill", "explain", "cannot"]},
            "skill": {"type": "string", "enum": ["", *names]},
            "input": {"type": "string"},
            "note": {"type": "string"},
            "text": {"type": "string"},
        },
        "required": ["action", "skill", "input", "note", "text"],
        "additionalProperties": False,
    }


_BRACE_SCAN = re.compile(r'[{}"\\]')


def _join_scans(into: dict, state: str, g: list) -> None:
    h = into.get(state)
    if h is None:
        into[state] = g
        return
    shift = h[0] - g[0]
    h[1] = sorted(h[1] + [(t + shift, s) for t, s in g[1]], key=lambda x: x[0])


def _brace_ends(text: str, starts: list) -> dict:
    ends: dict = {}
    pending = set(starts)
    groups: dict = {}
    for m in _BRACE_SCAN.finditer(text, starts[0]):
        i, c = m.start(), m.group()
        esc = groups.get("esc")
        if esc is not None and esc[2] < i - 1:
            del groups["esc"]
            _join_scans(groups, "in", esc)
        if c == "{" and i in pending:
            pending.discard(i)
            out = groups.setdefault("out", [0, [], -1])
            out[1].append((out[0], i))
        moved: dict = {}
        for state, g in groups.items():
            nxt = state
            if state == "out":
                if c == "{":
                    g[0] += 1
                elif c == "}":
                    g[0] -= 1
                    while g[1] and g[1][-1][0] == g[0]:
                        ends[g[1].pop()[1]] = i
                elif c == '"':
                    nxt = "in"
            elif state == "in":
                if c == "\\":
                    nxt, g[2] = "esc", i
                elif c == '"':
                    nxt = "out"
            else:
                nxt = "in"
            if g[1]:
                _join_scans(moved, nxt, g)
        groups = moved
        if not groups and not pending:
            break
    return ends


def _json_object(text: str) -> Optional[dict]:
    try:
        v = _json_loads(text)
    except (ValueError, RecursionError):
        return None
    return v if isinstance(v, dict) else None


def _first_json_object(text: str) -> Optional[dict]:
    v = _json_object(text)
    if v is not None:
        return v
    starts = [m.start() for m in re.finditer("{", text)][:64]
    ends = _brace_ends(text, starts) if starts else {}
    for i in starts:
        end = ends.get(i)
        if end is not None:
            v = _json_object(text[i:end + 1])
            if v is not None:
                return v
    return None


def _parse_decision(text: str) -> Optional[dict]:
    d = _first_json_object(text)
    if d is None or d.get("action") not in ("skill", "explain", "cannot"):
        return None

    def text_of(k: str) -> str:
        v = d.get(k)
        return v if isinstance(v, str) else ""

    inp = d.get("input")
    return {"action": d["action"], "skill": text_of("skill"),
            "input": _js_json(inp) if isinstance(inp, dict) else text_of("input"),
            "note": text_of("note"), "text": text_of("text")}


def _skill_turn(request: str, note: str, inp: Any) -> str:
    T = _ROUTER_TEXT
    parts = [
        f"{T['forSkillRequest']}\n{_kept_in(request)}" if _js_trim(request) else "",
        f"{T['forSkillNote']}\n{_kept_in(note)}" if note else "",
        f"{T['forSkillInput']}\n{_js_json(inp)}",
    ]
    return "\n\n".join(p for p in parts if p)


def _skill_text(value: Any) -> str:
    if isinstance(value, str):
        text = value
    elif value is None:
        text = ""
    else:
        try:
            text = _js_json(value)
        except (TypeError, ValueError, RecursionError):
            text = str(value)
    return text if _js_trim(text) else _ROUTER_TEXT["empty"]


def _cut_chars(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n - 1] + "…"


def _exchange_answer(r: dict) -> str:
    k = _ROUTER_TEXT["kept"]
    if r.get("reason") == "needs_confirmation":
        head = f"[{k['skill']} {r.get('skill')}: {k['waiting']}]"
    elif r.get("action") == "skill":
        if r.get("ok"):
            cut = f", {k['cutOff']}" if r.get("truncated") else ""
            head = f"[{k['skill']} {r.get('skill')}{cut}]"
        else:
            head = f"[{k['skill']} {r.get('skill')}: {k['failed']}, {r.get('reason')}]"
    else:
        head = f"[{r.get('action')}]"
    body = _cut_chars(_js_trim(r["text"]), _EXCHANGE_ANSWER_CHARS) if r.get("ok") else ""
    return f"{head} {body}" if body else head


def _call_bytes(c: ModelCall) -> int:
    schema = 0 if c.schema is None else _byte_length(_js_json(c.schema))
    return _FRAME_TOKENS + _byte_length(c.user) + sum(_byte_length(t) for t in c.system) + schema


def _sum_usage(into: dict, more: Mapping) -> dict:
    for k, v in more.items():
        if _is_number(v):
            have = into.get(k)
            into[k] = (have if _is_number(have) else 0) + v
        elif isinstance(v, Mapping):
            have = into.get(k)
            into[k] = _sum_usage(dict(have) if isinstance(have, Mapping) else {}, v)
        elif k not in into:
            into[k] = v
    return into


def _new_ask_id() -> str:
    return "ask-" + os.urandom(12).hex()


def _ask_turn(turn: Any) -> Optional[str]:
    if turn is None:
        return None
    if _is_number(turn):
        return _js_json(turn)
    if isinstance(turn, str) and _js_trim(turn) and _utf16_len(turn) <= 256:
        return _js_trim(turn)
    raise ValueError(
        "turn must be your id for the answer this ask belongs to: a non-blank string of 256 characters at "
        "most, or a number."
    )


def _ask_speaker(speaker: Any) -> Optional[str]:
    if speaker is None:
        return None
    if not isinstance(speaker, str) or not _js_trim(speaker):
        raise ValueError("speaker must be a non-blank name: leave it out for the store's own exchanges.")
    s = _js_trim(speaker)
    if _byte_length(s) > _SPEAKER_MAX_BYTES:
        raise ValueError(f"speaker is at most {_SPEAKER_MAX_BYTES} bytes.")
    return _one_line_speaker(s, "speaker")


def _ask_history(h: Any) -> bool:
    if h is None or h == "use":
        return True
    if h == "none":
        return False
    raise ValueError(f'history must be "use" or "none", got {_jq(h)}.')


_SCHEMA_TYPES = ("object", "array", "string", "number", "integer", "boolean", "null")
_SCHEMA_CHECKED = (
    "type", "properties", "required", "additionalProperties", "items", "enum", "const", "minLength", "maxLength",
    "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "minItems", "maxItems", "anyOf",
)
_SCHEMA_NOTES = ("description", "title", "default", "examples", "$comment", "format")


def _check_skill(defn: Any, st: _RouterState) -> dict:
    if not isinstance(defn, Mapping):
        raise ValueError("a skill is a dict: {name, description, kind, input, ...}.")
    for k in defn:
        if k not in _SKILL_FIELDS:
            near = next((n for n in _SKILL_FIELDS if isinstance(k, str) and _edit_within(k, n, 2)), None)
            hint = f" (did you mean {_jq(near)}?)" if near else ""
            raise ValueError(f"skill field {_jq(k)} is not one this SDK knows{hint}: {', '.join(_SKILL_FIELDS)}.")
    d = {k: v for k, v in defn.items() if v is not None}
    name = d.get("name")
    if not isinstance(name, str) or not _SKILL_NAME_RE.fullmatch(name):
        raise ValueError(f"skill name must be 1-32 of [a-z0-9_], starting with a letter: got {_jq(name)}.")
    desc = d.get("description")
    if not isinstance(desc, str) or not _js_trim(desc):
        raise ValueError(f"{name}: description is required. It is what the router reads to choose the skill.")
    if d.get("kind") not in ("llm", "code"):
        raise ValueError(f'{name}: kind must be "llm" or "code".')
    _check_schema(d.get("input"), "input")
    if d["input"].get("type") != "object":
        raise ValueError(f'{name}: input must describe an object: its schema needs type: "object".')
    if "confirm" in d and not isinstance(d["confirm"], bool):
        raise ValueError(f"{name}: confirm must be true or false.")
    if "metadata" in d and not isinstance(d["metadata"], Mapping):
        raise ValueError(f"{name}: metadata must be a dict.")
    if d["kind"] == "code":
        for k in ("instructions", "max_tokens", "effort", "model", "caller"):
            if k in d:
                raise ValueError(f"{name}: {k} is for an llm skill; a code skill runs your function.")
        if name not in st.handlers:
            raise ValueError(f"{name}: a code skill needs its function, router.handlers.{name}.")
    else:
        ins = d.get("instructions")
        if not isinstance(ins, str) or not _js_trim(ins):
            raise ValueError(f"{name}: instructions are required: what the skill model is told to do.")
        if "max_tokens" in d:
            _check_whole(d["max_tokens"], f"{name}: max_tokens", 1, _MODEL_MAX_TOKENS)
        if "effort" in d and d["effort"] not in _EFFORTS:
            raise ValueError(f"{name}: effort must be one of {', '.join(_EFFORTS)}.")
        if "model" in d:
            _check_model_name(d["model"], f"{name}: model")
        if "caller" in d and (not isinstance(d["caller"], str) or d["caller"] not in st.callers):
            have = f" ({', '.join(st.callers)})" if st.callers else ""
            raise ValueError(f"{name}: caller {_jq(d['caller'])} is not one of router.callers{have}.")
    if _nests_deeper(d, _SKILL_MAX_DEPTH):
        raise ValueError(f"{name}: the skill nests deeper than {_SKILL_MAX_DEPTH} levels.")
    try:
        kept = _copy_json(d)
    except (TypeError, ValueError, RecursionError) as e:
        raise ValueError(f"{name}: the skill cannot be kept as JSON ({e}).") from None
    if "model" in kept:
        kept["model"] = _js_trim(kept["model"])
    return kept


def _is_array(v: Any) -> bool:
    return isinstance(v, (list, tuple))


_SKILL_MAX_DEPTH = 100


def _check_schema(s: Any, at: str, depth: int = 0) -> None:
    if depth > 32:
        raise ValueError(f"{at}: the schema nests deeper than 32 levels.")
    if not isinstance(s, Mapping):
        raise ValueError(f"{at}: a schema is an object.")
    for k, v in s.items():
        if k in _SCHEMA_NOTES:
            continue
        if k not in _SCHEMA_CHECKED:
            raise ValueError(
                f"{at}: {_jq(k)} is not a keyword this SDK checks, so an input could pass that the skill does not "
                f"expect. It checks {', '.join(_SCHEMA_CHECKED)}."
            )
        if k == "type":
            ts = list(v) if _is_array(v) else [v]
            if not ts or not all(isinstance(t, str) and t in _SCHEMA_TYPES for t in ts):
                raise ValueError(f"{at}: type must be one of {', '.join(_SCHEMA_TYPES)}, or a list of them.")
        elif k == "properties":
            if not isinstance(v, Mapping):
                raise ValueError(f"{at}: properties must be an object of schemas.")
            for p, sub in v.items():
                _check_schema(sub, f"{at}.{p}", depth + 1)
        elif k == "required":
            if not _is_array(v) or not all(isinstance(r, str) for r in v):
                raise ValueError(f"{at}: required must be a list of property names.")
        elif k == "additionalProperties":
            if not isinstance(v, bool):
                _check_schema(v, f"{at}.*", depth + 1)
        elif k == "items":
            _check_schema(v, f"{at}[]", depth + 1)
        elif k == "enum":
            if not _is_array(v) or not v:
                raise ValueError(f"{at}: enum must be a non-empty list.")
        elif k == "anyOf":
            if not _is_array(v) or not v:
                raise ValueError(f"{at}: anyOf must be a non-empty list of schemas.")
            for i, sub in enumerate(v):
                _check_schema(sub, f"{at}.anyOf[{i}]", depth + 1)
        elif k in ("minLength", "maxLength", "minItems", "maxItems"):
            if not _is_whole(v) or v < 0:
                raise ValueError(f"{at}: {k} must be a whole number of 0 or more.")
        elif k in ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"):
            if not _is_number(v):
                raise ValueError(f"{at}: {k} must be a number.")


def _is_type(v: Any, t: str) -> bool:
    return {"object": isinstance(v, Mapping), "array": _is_array(v), "string": isinstance(v, str), "integer": _is_whole(v),
            "number": _is_number(v), "boolean": isinstance(v, bool), "null": v is None}.get(t, False)


def _canonical_json(v: Any) -> str:
    if _is_array(v):
        return "[" + ",".join(_canonical_json(x) for x in v) + "]"
    if isinstance(v, Mapping):
        return "{" + ",".join(f"{_js_string(str(k))}:{_canonical_json(v[k])}" for k in sorted(v, key=str)) + "}"
    try:
        return _js_json(v)
    except TypeError:
        return "null"


def _same_json(a: Any, b: Any) -> bool:
    return _canonical_json(a) == _canonical_json(b)


def _input_error(v: Any, s: Mapping, at: str) -> Optional[str]:
    if "type" in s:
        ts = list(s["type"]) if _is_array(s["type"]) else [s["type"]]
        if not any(_is_type(v, t) for t in ts):
            return f"{at} must be {' or '.join(_js_str(t) for t in ts)}"
    if _is_array(s.get("enum")) and not any(_same_json(e, v) for e in s["enum"]):
        return f"{at} must be one of {_jq(s['enum'])}"
    if "const" in s and not _same_json(s["const"], v):
        return f"{at} must be {_jq(s['const'])}"
    if isinstance(v, str):
        n = len(v)
        if _is_number(s.get("minLength")) and n < s["minLength"]:
            return f"{at} must be at least {_jq(s['minLength'])} characters"
        if _is_number(s.get("maxLength")) and n > s["maxLength"]:
            return f"{at} must be at most {_jq(s['maxLength'])} characters"
    if _is_number(v):
        if _is_number(s.get("minimum")) and v < s["minimum"]:
            return f"{at} must be at least {_jq(s['minimum'])}"
        if _is_number(s.get("maximum")) and v > s["maximum"]:
            return f"{at} must be at most {_jq(s['maximum'])}"
        if _is_number(s.get("exclusiveMinimum")) and v <= s["exclusiveMinimum"]:
            return f"{at} must be over {_jq(s['exclusiveMinimum'])}"
        if _is_number(s.get("exclusiveMaximum")) and v >= s["exclusiveMaximum"]:
            return f"{at} must be under {_jq(s['exclusiveMaximum'])}"
    if _is_array(v):
        if _is_number(s.get("minItems")) and len(v) < s["minItems"]:
            return f"{at} must hold at least {_jq(s['minItems'])} items"
        if _is_number(s.get("maxItems")) and len(v) > s["maxItems"]:
            return f"{at} must hold at most {_jq(s['maxItems'])} items"
        if isinstance(s.get("items"), Mapping):
            e = next((e for i, x in enumerate(v) for e in [_input_error(x, s["items"], f"{at}[{i}]")] if e), None)
            if e:
                return e
    if isinstance(v, Mapping):
        props = s.get("properties") if isinstance(s.get("properties"), Mapping) else {}
        if _is_array(s.get("required")):
            r = next((r for r in s["required"] if isinstance(r, str) and r not in v), None)
            if r is not None:
                return f"{at}.{r} is required"
        extra = s.get("additionalProperties")
        for k, x in v.items():
            sub = props.get(k) if k in props else None
            if not isinstance(sub, Mapping) and extra is False:
                return f"{at}.{k} is not a field this skill takes"
            sub = sub if isinstance(sub, Mapping) else extra if isinstance(extra, Mapping) else None
            e = None if sub is None else _input_error(x, sub, f"{at}.{k}")
            if e:
                return e
    if _is_array(s.get("anyOf")) and not any(
            isinstance(sub, Mapping) and _input_error(v, sub, at) is None for sub in s["anyOf"]):
        return f"{at} fits none of the shapes it may take"
    return None


_SKILL_MD_INPUT = {
    "type": "object",
    "properties": {"task": {"type": "string", "description": _ROUTER_TEXT["skillMdTask"]}},
    "required": ["task"],
}
_SKILL_MD_NAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
_FRONT_KEY = re.compile(r"([A-Za-z0-9_][A-Za-z0-9_-]*)[ \t]*:(?:[ \t]([^\n\r\u2028\u2029]*))?")
_FRONT_LIST_DEPTH = 32
_SPACE_HASH = re.compile(f"[{_JS_SPACE_CLASS}]#")


def _leading_space(line: str) -> int:
    return len(line) - len(line.lstrip(_JS_SPACE))


def _indented(line: str) -> bool:
    return bool(line) and line[0] in _JS_SPACE


def _yaml_comment(line: str) -> bool:
    return line.lstrip(_JS_SPACE).startswith("#")


def _parse_skill_md(text: Any) -> tuple:
    if not isinstance(text, str):
        raise ValueError("SKILL.md must be given as its text.")
    if text.startswith("\ufeff"):
        text = text[1:]
    lines = re.sub(r"\r\n?", "\n", text).split("\n")
    if _js_trim(lines[0]) != "---":
        raise ValueError("SKILL.md must begin with YAML frontmatter: a --- line, name: and description:, and another --- line.")
    close = next((i for i in range(1, len(lines)) if _js_trim(lines[i]) == "---"), -1)
    if close == -1:
        raise ValueError("SKILL.md: the frontmatter has no closing --- line.")
    rest = _parse_frontmatter(lines[1:close], 2)
    name = rest.pop("name", _MISSING)
    description = rest.pop("description", _MISSING)
    if name is _MISSING:
        raise ValueError("SKILL.md: the frontmatter has no name.")
    if not isinstance(name, str) or _utf16_len(name) > 64 or not _SKILL_MD_NAME.fullmatch(name):
        raise ValueError(f"SKILL.md: name must be lowercase letters, digits and hyphens, at most 64 characters, got {_jq(name)}.")
    if description is _MISSING:
        raise ValueError("SKILL.md: the frontmatter has no description. It is what the router reads to choose the skill.")
    if not isinstance(description, str) or not _js_trim(description):
        raise ValueError("SKILL.md: description must be text, and not blank.")
    if _utf16_len(description) > 1024:
        raise ValueError(f"SKILL.md: description is at most 1024 characters, got {_utf16_len(description)}.")
    body = _js_trim("\n".join(lines[close + 1:]))
    if not body:
        raise ValueError("SKILL.md: there are no instructions after the frontmatter.")
    return name, _js_trim(description), rest, body


def _parse_frontmatter(lines: list, first_line: int) -> dict:
    out: dict = {}

    def where(i: int) -> str:
        return f"SKILL.md frontmatter, line {first_line + i}"

    i = 0
    while i < len(lines):
        line = lines[i]
        if not _js_trim(line) or _yaml_comment(line):
            i += 1
            continue
        if _indented(line):
            raise ValueError(f"{where(i)}: an indented line with no key above it.")
        kv = _FRONT_KEY.fullmatch(line)
        if kv is None:
            raise ValueError(f'{where(i)}: expected "key: value", got {_jq(line[:80])}.')
        if kv.group(1) in out:
            raise ValueError(f"{where(i)}: {kv.group(1)} is given twice.")
        j = i + 1
        while j < len(lines) and (not _js_trim(lines[j]) or _indented(lines[j])):
            j += 1
        block = lines[i + 1:j]
        while block and not _js_trim(block[-1]):
            block.pop()
        out[kv.group(1)] = _front_value(kv.group(2) or "", block, where(i))
        i = j
    return out


def _front_value(raw: str, block: list, where: str) -> Any:
    v = _js_trim(raw)
    if re.fullmatch(r"[|>][+-]?", v):
        filled = [line for line in block if _js_trim(line)]
        if not filled:
            return ""
        indent = min(_leading_space(line) for line in filled)
        ls = [line[indent:].rstrip(_JS_SPACE) for line in block]
        if v[0] == "|":
            return _js_trim("\n".join(ls))
        paragraphs = re.split(r"\n{2,}", "\n".join(ls))
        return _js_trim("\n".join(" ".join(p.split("\n")) for p in paragraphs))
    if v == "":
        items = [_js_trim(line) for line in block if _js_trim(line) and not _yaml_comment(line)]
        if not items:
            return ""
        if all(x == "-" or x.startswith("- ") for x in items):
            return [_scalar_of(_js_trim(x[1:]), where) for x in items]
        nested: dict = {}
        for x in items:
            m = _FRONT_KEY.fullmatch(x)
            if m is None:
                raise ValueError(f'{where}: expected "key: value" under it, got {_jq(x[:80])}.')
            nested[m.group(1)] = _scalar_of(_js_trim(m.group(2) or ""), where)
        return nested
    if block:
        if v[:1] in ('"', "'", "[", "{"):
            raise ValueError(f"{where}: a quoted value cannot go on over the indented lines below it.")
        return " ".join(x for x in [v] + [_js_trim(line) for line in block] if x)
    return _scalar_of(v, where)


def _quote_end(v: str) -> Optional[int]:
    i = 1
    while i < len(v):
        c = v[i]
        if v[0] == '"' and c == "\\":
            i += 2
            continue
        if c == v[0]:
            if v[0] == "'" and v[i + 1:i + 2] == "'":
                i += 2
                continue
            return i
        i += 1
    return None


def _quoted(v: str, where: str) -> str:
    kind = "double" if v[0] == '"' else "single"
    end = _quote_end(v)
    if end is None:
        raise ValueError(f"{where}: a {kind}-quoted value has no closing quote.")
    rest = v[end + 1:]
    if rest and not (rest[0] in _JS_SPACE and _js_trim(rest).startswith("#")):
        raise ValueError(f"{where}: only a comment can follow the closing quote.")
    return v[:end + 1]


def _scalar_of(v: str, where: str, depth: int = 0) -> Any:
    if v.startswith('"'):
        v = _quoted(v, where)
        try:
            return _json_loads(v)
        except (ValueError, RecursionError):
            raise ValueError(f"{where}: the double-quoted value is not valid.") from None
    if v.startswith("'"):
        return _quoted(v, where)[1:-1].replace("''", "'")
    if v.startswith("[") and v.endswith("]"):
        if depth >= _FRONT_LIST_DEPTH:
            raise ValueError(f"{where}: a list nests deeper than {_FRONT_LIST_DEPTH} levels.")
        return [_scalar_of(x, where, depth + 1) for x in (_js_trim(p) for p in v[1:-1].split(",")) if x]
    m = _SPACE_HASH.search(v)
    return _js_trim(v if m is None else v[:m.start()])


def _skill_md_definition(text: Any, name: Optional[str], inp: Optional[dict], model: Optional[str],
                         caller: Optional[str], max_tokens: Optional[int], effort: Optional[str],
                         confirm: Optional[bool]) -> dict:
    md_name, description, rest, body = _parse_skill_md(text)
    skill_name = md_name.replace("-", "_") if name is None else name
    if name is None and not _SKILL_NAME_RE.fullmatch(skill_name):
        raise ValueError(
            f"SKILL.md: the name {_jq(md_name)} makes the skill name {_jq(skill_name)}, which is not 1-32 of "
            "[a-z0-9_] starting with a letter: pass another as name=.")
    d: dict = {"name": skill_name, "description": description, "kind": "llm", "instructions": body,
               "input": _copy_json(_SKILL_MD_INPUT) if inp is None else inp}
    for k, v in (("model", model), ("caller", caller), ("max_tokens", max_tokens), ("effort", effort),
                 ("confirm", confirm)):
        if v is not None:
            d[k] = v
    d["metadata"] = {"source": "SKILL.md", "skill_md_name": md_name, **({"frontmatter": rest} if rest else {}),
                     "not_run": _SKILL_MD_NOT_RUN}
    return d


def _skills_add(client: Any, definition: Any, user_id: Optional[str]) -> dict:
    store = client._uid(user_id)
    st = client._router
    d = _check_skill(definition, st)
    with st.lock:
        if st.skills.get(store, d["name"]) is not None:
            raise ValueError(f"store {_jq(store)} already has a skill named {_jq(d['name'])}: remove it first to replace it.")
        st.skills.add(store, [d])
    return _copy_json(d)


def _skills_list(client: Any, user_id: Optional[str]) -> list:
    st = client._router
    with st.lock:
        return [_copy_json(d) for d in st.skills.list(client._uid(user_id))]


def _skills_remove(client: Any, name: str, user_id: Optional[str]) -> bool:
    store = client._uid(user_id)
    st = client._router
    with st.lock:
        return st.skills.remove(store, _required(name, "name"))


def _skills_copy(client: Any, name: str, from_: Any, to: Any) -> list:
    skill = _required(name, "name")
    if not isinstance(from_, str) or not _js_trim(from_):
        raise ValueError("from_ is required: the store to copy from. A copy never falls back to the default store.")
    targets = list(to) if _is_array(to) else [to]
    if not targets:
        raise ValueError("to is required: a store, or a list of stores.")
    for t in targets:
        if not isinstance(t, str) or not _js_trim(t):
            raise ValueError(f"to holds {_jq(t)}, which is not a store id.")
        _warn_if_store_id_collapses(t)
    _warn_if_store_id_collapses(from_)
    st = client._router
    with st.lock:
        src = st.skills.get(from_, skill)
        if src is None:
            raise ValueError(f"store {_jq(from_)} has no skill named {_jq(skill)}.")
        seen = {_normalize_store_id(from_)}
        for t in targets:
            k = _normalize_store_id(t)
            if k in seen:
                why = "the store copied from" if k == _normalize_store_id(from_) else "in the list twice"
                raise ValueError(f"{_jq(t)} is {why}; nothing was copied.")
            seen.add(k)
            if st.skills.get(t, src["name"]) is not None:
                raise ValueError(f"store {_jq(t)} already has a skill named {_jq(src['name'])}; nothing was copied.")
        for t in targets:
            st.skills.add(t, [_copy_json(src)])
    return targets


def _skills_export(client: Any, user_id: Optional[str]) -> str:
    store = client._uid(user_id)
    st = client._router
    with st.lock:
        return _js_json({"wontopos_skills": _SKILLS_EXPORT_VERSION, "store": store, "skills": st.skills.list(store)})


def _skills_import(client: Any, data: Any, user_id: Optional[str]) -> list:
    parsed = data
    if isinstance(data, str):
        try:
            parsed = _json_loads(data)
        except (ValueError, RecursionError) as e:
            raise ValueError(f"the skills are not JSON: {e}") from None
    version = parsed.get("wontopos_skills") if isinstance(parsed, Mapping) else None
    if (not isinstance(parsed, Mapping) or not _is_number(version) or version != _SKILLS_EXPORT_VERSION
            or not isinstance(parsed.get("skills"), list)):
        raise ValueError(f'not an export_skills text: expected {{"wontopos_skills": {_SKILLS_EXPORT_VERSION}, "skills": [...]}}.')
    store = client._uid(user_id)
    st = client._router
    defs = []
    for i, s in enumerate(parsed["skills"]):
        try:
            defs.append(_check_skill(s, st))
        except ValueError as e:
            raise ValueError(f"skills[{i}]: {e} Nothing was imported.") from None
    with st.lock:
        names: set = set()
        for d in defs:
            if d["name"] in names:
                raise ValueError(f"{_jq(d['name'])} is in the skills twice; nothing was imported.")
            names.add(d["name"])
            if st.skills.get(store, d["name"]) is not None:
                raise ValueError(f"store {_jq(store)} already has a skill named {_jq(d['name'])}; nothing was imported.")
        st.skills.add(store, defs)
    return [d["name"] for d in defs]


_NO_CHANNEL = (404, 501)


def _channel_read_body(store: str, speaker: Optional[str]) -> dict:
    return {"user_id": store, "channel": "router", **({} if speaker is None else {"speaker": speaker})}


def _exchanges(r: Any) -> list:
    return [e for e in _as_records(_as_dict(r).get("exchanges"))
            if isinstance(e.get("request"), str) and isinstance(e.get("answer"), str)]


def _precheck(client: Any, store: str) -> Optional[_Fail]:
    st = client._router
    if st.router is None:
        return _Fail("not_configured",
                     'pass router={"key": ...} (the model provider\'s key) or router={"router_caller": ...} to the client')
    if _router_clock(client) is None:
        return _Fail("no_time_zone",
                     'the client has no time zone: set router["time_zone"] to an IANA name such as "Asia/Seoul", '
                     "or tz to the UTC offset in hours, such as 9")
    with st.lock:
        empty = not st.skills.list(store)
    if empty:
        return _Fail("no_skills", f"store {_jq(store)} has no skills: add one with add_skill")
    return None


def _count_flow(caller: Any, c: ModelCall, run: _AskClock) -> Any:
    fn = getattr(caller, "count_tokens", None)
    if fn is None:
        return None
    kind, value = yield ("run", fn, getattr(caller, "_acount_tokens", None), (c,), run)
    if kind != "ok" or not _is_whole(value) or value < 0:
        return None
    return int(value)


def _stable_tokens_flow(st: _RouterState, caller: Any, c: ModelCall, run: _AskClock) -> Any:
    key = st.tokens_key(c)
    hit = st.tokens_get(key)
    if hit is not None:
        return hit, True
    n = yield from _count_flow(caller, c, run)
    if n is None:
        return _call_bytes(c), False
    st.tokens_put(key, n)
    return n, True


def _call_model_flow(caller: Any, c: ModelCall, run: _AskClock, usage: dict, purpose: str) -> Any:
    attempt = 0
    while True:
        if run.why():
            return run.failure()
        kind, value = yield ("run", caller.call, getattr(caller, "_acall", None), (c,), run)
        if kind == "ok":
            reply = _reply_shape(value)
            if reply["usage"] is not None:
                usage[purpose] = _sum_usage(usage.get(purpose, {}), reply["usage"])
            return reply
        if run.why():
            return run.failure()
        f = _model_failure(value)
        if f.reason == "rate_limit" and attempt == 0 and f.retry_after is not None and f.retry_after < run.left():
            yield ("sleep", f.retry_after, run)
            attempt += 1
            continue
        return f


def _router_decide_flow(client: Any, store: str, request: str, exchanges: list, clock: _RouterClock,
                        run: _AskClock, usage: dict) -> Any:
    st = client._router
    caller = st.router
    with st.lock:
        skills = st.skills.list(store)
    base = ModelCall("router", st.router_model, [st.prompt, _skill_list(skills)], "-", st.max_tokens,
                     effort="low", schema=_router_schema([s["name"] for s in skills]), signal=run.signal)
    stable, counted = yield from _stable_tokens_flow(st, caller, base, run)
    if run.why():
        return run.failure()
    if stable > _ROUTER_STABLE_MAX_TOKENS:
        return _Fail("too_large", f"the router prompt and this store's skills come to {stable} tokens, over "
                                  f"{_ROUTER_STABLE_MAX_TOKENS}")
    held = list(exchanges)
    while True:
        turn_text = _router_turn(held, clock, time.time(), st.name, request)
        call = base._replace(user=turn_text, system_tokens=stable if counted else None)
        if stable + _byte_length(turn_text) + _FRAME_TOKENS <= _ROUTER_PROMPT_MAX_TOKENS:
            break
        exact = yield from _count_flow(caller, call, run)
        if run.why():
            return run.failure()
        if exact is not None and exact <= _ROUTER_PROMPT_MAX_TOKENS:
            break
        if not held:
            return _Fail("too_large", f"the request does not fit the router's {_ROUTER_PROMPT_MAX_TOKENS} tokens")
        held = held[1:]
    reply = yield from _call_model_flow(caller, call, run, usage, "router")
    if isinstance(reply, _Fail):
        return reply
    if reply["stop"] == "refusal":
        return _Fail("refusal", "the router declined")
    if reply["stop"] == "max_tokens":
        return _Fail("truncated", "the router's answer was cut off before it was whole")
    d = _parse_decision(reply["text"])
    if d is None:
        return _Fail("bad_choice", "the router's answer holds no decision")
    if d["action"] != "skill":
        text = _js_trim(d["text"])
        if not text:
            names = ", ".join(s["name"] for s in skills)
            text = (f"{_ROUTER_TEXT['explainFallback']} {names}" if d["action"] == "explain"
                    else _ROUTER_TEXT["cannot"])
        return {"action": d["action"], "text": text}
    with st.lock:
        defn = st.skills.get(store, d["skill"])
    if defn is None:
        return _Fail("bad_choice", f"the router chose {_jq(d['skill'])}, which this store has no skill of")
    try:
        inp = _json_loads(d["input"]) if _js_trim(d["input"]) else {}
    except (ValueError, RecursionError):
        return _Fail("bad_choice", f"the router's input for {defn['name']} is not JSON")
    if not isinstance(inp, dict):
        return _Fail("bad_choice", f"the router's input for {defn['name']} is not an object")
    bad = _input_error(inp, defn["input"], "input")
    if bad:
        return _Fail("bad_choice", f"the router's input for {defn['name']}: {bad}")
    return {"action": "skill", "def": defn, "input": inp, "note": _js_trim(d["note"])}


def _error_text(e: Any) -> str:
    m = getattr(e, "message", None)
    return m if isinstance(m, str) else str(e)


def _run_skill_def_flow(client: Any, defn: dict, inp: dict, ctx: tuple, run: _AskClock, usage: dict) -> Any:
    st = client._router
    store, request, note, ask_id, speaker = ctx
    name = defn["name"]
    if defn["kind"] == "code":
        fn = st.handlers.get(name)
        if fn is None:
            return _Fail("tool_error", f"no function is registered for {name} (router.handlers)")
        sctx = SkillContext(store, request, note, ask_id, speaker, run.signal)
        kind, value = yield ("run", fn, None, (_copy_json(inp), sctx), run)
        if kind != "ok":
            if run.why():
                return run.failure()
            return _Fail("tool_error", f"{name} threw: {_clean_capped(_error_text(value), 512)}")
        return {"text": _skill_text(value)}
    named = defn.get("caller")
    caller = st.callers.get(named) if named is not None else st.skill
    if caller is None:
        detail = (f"{name} names the caller {_jq(named)}, which router.callers does not have" if named is not None
                  else "an llm skill needs router.key, router.skill_key or router.skill_caller")
        return _Fail("not_configured", detail)
    own_model = getattr(caller, "model", None) if named is not None else None
    model = _first_set(defn.get("model"), None if own_model is None else _js_trim(own_model), st.skill_model)
    c = ModelCall("skill", model, [defn.get("instructions") or ""], _skill_turn(request, note, inp),
                  _first_set(defn.get("max_tokens"), _SKILL_MAX_TOKENS), effort=defn.get("effort"),
                  signal=run.signal)
    got = yield from _call_model_flow(caller, c, run, usage, "skill")
    if isinstance(got, _Fail):
        return got
    if got["stop"] == "refusal":
        return _Fail("refusal", "the skill model declined")
    if not _js_trim(got["text"]):
        if got["stop"] == "max_tokens":
            return _Fail("truncated", "the skill model ran out of tokens before it wrote anything")
        return _Fail("failed", "the skill model answered with no text")
    return {"text": got["text"], **({"truncated": True} if got["stop"] == "max_tokens" else {})}


def _ask_flow(client: Any, request: Any, user_id: Optional[str], turn: Any, speaker: Any, history: Any) -> Any:
    if not isinstance(request, str) or not _js_trim(request):
        raise ValueError("request is required: what the model asks for, in its own words.")
    turn_id = _ask_turn(turn)
    speaker = _ask_speaker(speaker)
    use_history = _ask_history(history)
    store = client._uid(user_id)
    st = client._router
    ask_id = _new_ask_id()
    hist = {"read": "skipped" if use_history else "off", "pushed": "skipped" if use_history else "off"}
    usage: dict = {}

    def end(r: dict) -> dict:
        return {**r, "store": store, "ask_id": ask_id, "history": hist, **({"usage": usage} if usage else {})}

    def refuse(f: _Fail, **extra: Any) -> dict:
        return end({"ok": False, "text": _ROUTER_TEXT["failed"][f.reason], "reason": f.reason, **extra,
                    **({"detail": f.detail} if f.detail else {})})

    pre = _precheck(client, store)
    if pre is not None:
        return refuse(pre)
    clock = _router_clock(client)
    lim = st.reserve(_normalize_store_id(store), turn_id, _clock_parts(clock, time.time())[0], _time.monotonic())
    if isinstance(lim, str):
        return refuse(_Fail("limit", lim))
    run = _AskClock(lim.budget)
    started = _time.monotonic()
    try:
        exchanges: list = []
        if use_history:
            ok, got = yield ("post", "/api/v1/memory/channel/read", _channel_read_body(store, speaker), run)
            if ok:
                exchanges = _exchanges(got)
                hist["read"] = "ok"
            else:
                hist["read"] = "failed"
        if run.why():
            return refuse(run.failure())
        d = yield from _router_decide_flow(client, store, request, exchanges, clock, run, usage)
        if isinstance(d, _Fail):
            return refuse(d)
        if d["action"] != "skill":
            result = end({"ok": True, "action": d["action"], "text": d["text"]})
        elif d["def"].get("confirm"):
            result = refuse(_Fail("needs_confirmation"), action="skill", skill=d["def"]["name"], input=d["input"])
        else:
            ran = yield from _run_skill_def_flow(client, d["def"], d["input"],
                                                 (store, request, d["note"], ask_id, speaker), run, usage)
            at = {"action": "skill", "skill": d["def"]["name"], "input": d["input"]}
            result = refuse(ran, **at) if isinstance(ran, _Fail) else end({"ok": True, **at, **ran})
        if use_history:
            if run.why() or run.left() <= 0:
                hist["pushed"] = "skipped"
            else:
                body = _channel_read_body(store, None)
                body["request"] = _cut_chars(request, _EXCHANGE_MAX_CHARS)
                body["answer"] = _cut_chars(_exchange_answer(result), _EXCHANGE_MAX_CHARS)
                if speaker is not None:
                    body["speaker"] = speaker
                ok, _ = yield ("post", "/api/v1/memory/channel/push", body, run)
                hist["pushed"] = "ok" if ok else "failed"
        return result
    except GeneratorExit:
        run.cancel()
        raise
    finally:
        run.close()
        lim.done(_time.monotonic() - started)


def _run_skill_flow(client: Any, name: Any, inp: Any, user_id: Optional[str], request: Any, note: Any,
                    speaker: Any, ask_id: Any) -> Any:
    store = client._uid(user_id)
    st = client._router
    with st.lock:
        defn = st.skills.get(store, _required(name, "name"))
    if defn is None:
        raise ValueError(f"store {_jq(store)} has no skill named {_jq(name)}.")
    if not isinstance(inp, Mapping):
        raise ValueError("input must be a dict: the input the skill takes.")
    try:
        inp = _copy_json(inp)
    except (TypeError, ValueError, RecursionError) as e:
        raise ValueError(f"input must be JSON: {e}") from None
    bad = _input_error(inp, defn["input"], "input")
    if bad:
        raise ValueError(f"{defn['name']}: {bad}.")
    speaker = _ask_speaker(speaker)
    ask_id = _new_ask_id() if ask_id is None else _required(ask_id, "ask_id")
    request = "" if request is None else str(request)
    note = "" if note is None else str(note)
    usage: dict = {}
    run = _AskClock(st.limits["time"])
    try:
        ran = yield from _run_skill_def_flow(client, defn, inp, (store, request, note, ask_id, speaker), run, usage)
        at = {"action": "skill", "skill": defn["name"], "input": _copy_json(inp), "store": store, "ask_id": ask_id,
              "history": {"read": "off", "pushed": "off"}}
        if usage.get("skill") is not None:
            at["usage"] = usage
        return {**at, **_readiness(ran)} if isinstance(ran, _Fail) else {**at, "ok": True, **ran}
    except GeneratorExit:
        run.cancel()
        raise
    finally:
        run.close()


def _readiness(f: Optional[_Fail]) -> dict:
    if f is None:
        return {"ok": True}
    return {"ok": False, "reason": f.reason, "text": _ROUTER_TEXT["failed"][f.reason],
            **({"detail": f.detail} if f.detail else {})}


def _drive_sync(client: Any, flow: Any) -> Any:
    value: Any = None
    while True:
        try:
            need = flow.send(value)
        except StopIteration as stop:
            return stop.value
        try:
            value = _do_sync(client, need)
        except BaseException:
            flow.close()
            raise


async def _drive_async(client: Any, flow: Any) -> Any:
    value: Any = None
    while True:
        try:
            need = flow.send(value)
        except StopIteration as stop:
            return stop.value
        try:
            value = await _do_async(client, need)
        except BaseException:
            flow.close()
            raise


def _do_sync(client: Any, need: tuple) -> Any:
    kind = need[0]
    if kind == "post":
        _, path, body, run = need
        try:
            c = client._clone(deadline=max(0.001, run.left()))
            return True, c._post(path, body)
        except Exception as e:
            return False, e
    if kind == "sleep":
        _, seconds, run = need
        _time.sleep(max(0.0, min(seconds, run.left())))
        return None
    _, fn, _afn, args, run = need
    return _run_in_thread(fn, args, run)


def _run_in_thread(fn: Any, args: tuple, run: _AskClock) -> tuple:
    left = run.left()
    if left <= 0 or run.why():
        run.expire()
        return "timeout", None
    box: dict = {}
    done = threading.Event()

    def work() -> None:
        try:
            v = fn(*args)
            if inspect.isawaitable(v):
                v = asyncio.run(_awaited(v))
            box["ok"] = v
        except BaseException as e:
            box["error"] = e
        finally:
            done.set()

    threading.Thread(target=work, name="wontopos-ask", daemon=True).start()
    if not done.wait(left):
        run.expire()
        return "timeout", None
    return ("error", box["error"]) if "error" in box else ("ok", box.get("ok"))


async def _do_async(client: Any, need: tuple) -> Any:
    kind = need[0]
    if kind == "post":
        _, path, body, run = need
        try:
            c = client._clone(deadline=max(0.001, run.left()))
            return True, await c._post(path, body)
        except Exception as e:
            return False, e
    if kind == "sleep":
        _, seconds, run = need
        await _async_sleep(max(0.0, min(seconds, run.left())))
        return None
    _, fn, afn, args, run = need
    target = afn if afn is not None else fn
    if run.left() <= 0 or run.why():
        run.expire()
        return "timeout", None
    if _is_async_callable(target):
        try:
            aw = target(*args)
        except Exception as e:
            return "error", e
        return await _await_within(aw, run)
    got = await _thread_within(target, args, run)
    if got[0] == "ok" and inspect.isawaitable(got[1]):
        return await _await_within(got[1], run)
    return got


def _forget_outcome(task: Any) -> None:
    if not task.cancelled():
        task.exception()


def _settle(fut: Any, outcome: tuple) -> None:
    if not fut.done():
        fut.set_result(outcome)


class _RouterToolsBase:

    def __init__(self, client: Any, store: str, turn: Any, speaker: Any, history: Any):
        self._client = client
        self._store = store
        self._turn = turn
        self._speaker = speaker
        self._history = history

    def definitions(self, format: str = "anthropic") -> list:
        if format not in ("anthropic", "openai"):
            raise ValueError(f'format must be "anthropic" or "openai", got {_jq(format)}.')
        schema = {
            "type": "object",
            "properties": {"request": {"type": "string", "description": _ROUTER_TEXT["toolRequest"]}},
            "required": ["request"],
        }
        if format == "openai":
            return [{"type": "function",
                     "function": {"name": "ask", "description": _ROUTER_TEXT["tool"], "parameters": schema}}]
        return [{"name": "ask", "description": _ROUTER_TEXT["tool"], "input_schema": schema}]

    def _request(self, call: Any) -> tuple:
        name = _call_field(call, "name")
        if name != "ask":
            raise ValueError(f"not the ask tool: {_jq(name)}")
        raw = _call_field(call, "input")
        if raw is None:
            arguments = _call_field(call, "arguments")
            raw = {}
            if isinstance(arguments, str) and arguments.strip(_JS_SPACE):
                try:
                    raw = _json_loads(arguments)
                except (ValueError, RecursionError):
                    return None, _js_json({"ok": False, "text": _ROUTER_TEXT["notJson"]})
        args = raw if isinstance(raw, Mapping) else {}
        request = args.get("request")
        if not isinstance(request, str) or not _js_trim(request):
            return None, _js_json({"ok": False, "text": _ROUTER_TEXT["noRequest"]})
        return request, None

    def _ask_kwargs(self) -> dict:
        return {"user_id": self._store, "turn": self._turn, "speaker": self._speaker, "history": self._history}


class RouterTools(_RouterToolsBase):
    """The ``ask`` tool for one store, from :meth:`Client.router_tools`."""

    def run(self, call: Any) -> str:
        """Answer one call of ``ask`` with the JSON text the model reads."""
        request, answer = self._request(call)
        if answer is not None:
            return answer
        return _js_json(ask_for_model(self._client.ask(request, **self._ask_kwargs())))


class AsyncRouterTools(_RouterToolsBase):
    """The ``ask`` tool for one store, from :meth:`AsyncClient.router_tools`."""

    async def run(self, call: Any) -> str:
        """Answer one call of ``ask`` with the JSON text the model reads."""
        request, answer = self._request(call)
        if answer is not None:
            return answer
        return _js_json(ask_for_model(await self._client.ask(request, **self._ask_kwargs())))


class _RouterCalls:
    def add_skill(self, definition: dict, user_id: Optional[str] = None) -> dict:
        """Add a skill to a store, kept in this process. A name the store already has is refused."""
        return _skills_add(self, definition, user_id)

    def list_skills(self, user_id: Optional[str] = None) -> list[dict]:
        """A store's skills, by name."""
        return _skills_list(self, user_id)

    def remove_skill(self, name: str, user_id: Optional[str] = None) -> bool:
        """Remove a skill from a store; false when it had none of that name."""
        return _skills_remove(self, name, user_id)

    def copy_skill(self, name: str, from_: str, to: Union[str, Sequence[str]]) -> list[str]:
        """Copy a skill from one store to others."""
        return _skills_copy(self, name, from_, to)

    def export_skills(self, user_id: Optional[str] = None) -> str:
        """A store's skills as JSON text, for :meth:`import_skills`."""
        return _skills_export(self, user_id)

    def import_skills(self, data: Union[str, dict], user_id: Optional[str] = None) -> list[str]:
        """Add the skills of an :meth:`export_skills` text to a store, all or none."""
        return _skills_import(self, data, user_id)

    def import_skill_md(self, user_id: Optional[str], text: str, *, name: Optional[str] = None,
                        input: Optional[dict] = None, model: Optional[str] = None, caller: Optional[str] = None,
                        max_tokens: Optional[int] = None, effort: Optional[str] = None,
                        confirm: Optional[bool] = None) -> dict:
        """Add an LLM skill from an Agent Skills ``SKILL.md``."""
        return _skills_add(self, _skill_md_definition(text, name, input, model, caller, max_tokens, effort, confirm),
                           user_id)

    def ask(self, request: str, user_id: Optional[str] = None, *, turn: Optional[Union[str, int, float]] = None,
            speaker: Optional[str] = None, history: Optional[str] = None) -> dict:
        """Ask the router for something outside memory.
        Only a mistake in the arguments raises; any other failure comes back as ``ok: False``."""
        return _drive_sync(self, _ask_flow(self, request, user_id, turn, speaker, history))

    def can_ask(self, user_id: Optional[str] = None) -> dict:
        """Whether :meth:`ask` can run for this store now."""
        return _readiness(_precheck(self, self._uid(user_id)))

    def run_skill(self, name: str, input: dict, user_id: Optional[str] = None, *, request: Optional[str] = None,
                  note: Optional[str] = None, speaker: Optional[str] = None, ask_id: Optional[str] = None) -> dict:
        """Run a skill the router chose, once a person agreed to it; never offer this to a model."""
        return _drive_sync(self, _run_skill_flow(self, name, input, user_id, request, note, speaker, ask_id))

    def router_history(self, user_id: Optional[str] = None, *, speaker: Optional[str] = None) -> list[dict]:
        """The router's last exchanges with a store, oldest first."""
        body = _channel_read_body(self._uid(user_id), _ask_speaker(speaker))
        try:
            return _exchanges(self._post("/api/v1/memory/channel/read", body))
        except WosError as e:
            if e.status in _NO_CHANNEL:
                return []
            raise

    def router_tools(self, user_id: Optional[str] = None, *, turn: Optional[Union[str, int, float]] = None,
                     speaker: Optional[str] = None, history: Optional[str] = None) -> "RouterTools":
        """The ``ask`` tool for a model that calls tools."""
        _ask_turn(turn)
        _ask_speaker(speaker)
        _ask_history(history)
        return RouterTools(self, self._uid(user_id), turn, speaker, history)


class _AsyncRouterCalls:
    async def add_skill(self, definition: dict, user_id: Optional[str] = None) -> dict:
        """Add a skill to a store, kept in this process. A name the store already has is refused."""
        return _skills_add(self, definition, user_id)

    async def list_skills(self, user_id: Optional[str] = None) -> list[dict]:
        """A store's skills, by name."""
        return _skills_list(self, user_id)

    async def remove_skill(self, name: str, user_id: Optional[str] = None) -> bool:
        """Remove a skill from a store; false when it had none of that name."""
        return _skills_remove(self, name, user_id)

    async def copy_skill(self, name: str, from_: str, to: Union[str, Sequence[str]]) -> list[str]:
        """Copy a skill from one store to others."""
        return _skills_copy(self, name, from_, to)

    async def export_skills(self, user_id: Optional[str] = None) -> str:
        """A store's skills as JSON text, for :meth:`import_skills`."""
        return _skills_export(self, user_id)

    async def import_skills(self, data: Union[str, dict], user_id: Optional[str] = None) -> list[str]:
        """Add the skills of an :meth:`export_skills` text to a store, all or none."""
        return _skills_import(self, data, user_id)

    async def import_skill_md(self, user_id: Optional[str], text: str, *, name: Optional[str] = None,
                              input: Optional[dict] = None, model: Optional[str] = None,
                              caller: Optional[str] = None, max_tokens: Optional[int] = None,
                              effort: Optional[str] = None, confirm: Optional[bool] = None) -> dict:
        """Add an LLM skill from an Agent Skills ``SKILL.md``."""
        return _skills_add(self, _skill_md_definition(text, name, input, model, caller, max_tokens, effort, confirm),
                           user_id)

    async def ask(self, request: str, user_id: Optional[str] = None, *,
                  turn: Optional[Union[str, int, float]] = None, speaker: Optional[str] = None,
                  history: Optional[str] = None) -> dict:
        """Ask the router for something outside memory.
        Only a mistake in the arguments raises; any other failure comes back as ``ok: False``."""
        return await _drive_async(self, _ask_flow(self, request, user_id, turn, speaker, history))

    async def can_ask(self, user_id: Optional[str] = None) -> dict:
        """Whether :meth:`ask` can run for this store now."""
        return _readiness(_precheck(self, self._uid(user_id)))

    async def run_skill(self, name: str, input: dict, user_id: Optional[str] = None, *,
                        request: Optional[str] = None, note: Optional[str] = None, speaker: Optional[str] = None,
                        ask_id: Optional[str] = None) -> dict:
        """Run a skill the router chose, once a person agreed to it; never offer this to a model."""
        return await _drive_async(self, _run_skill_flow(self, name, input, user_id, request, note, speaker, ask_id))

    async def router_history(self, user_id: Optional[str] = None, *, speaker: Optional[str] = None) -> list[dict]:
        """The router's last exchanges with a store, oldest first."""
        body = _channel_read_body(self._uid(user_id), _ask_speaker(speaker))
        try:
            return _exchanges(await self._post("/api/v1/memory/channel/read", body))
        except WosError as e:
            if e.status in _NO_CHANNEL:
                return []
            raise

    def router_tools(self, user_id: Optional[str] = None, *, turn: Optional[Union[str, int, float]] = None,
                     speaker: Optional[str] = None, history: Optional[str] = None) -> "AsyncRouterTools":
        """The ``ask`` tool for a model that calls tools."""
        _ask_turn(turn)
        _ask_speaker(speaker)
        _ask_history(history)
        return AsyncRouterTools(self, self._uid(user_id), turn, speaker, history)
