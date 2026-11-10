"""The sleep: the app's own model rewrites the being's core and its four boxes."""
from __future__ import annotations

import asyncio
import copy as _copy
import inspect
import json
import math
import random
import re
import threading
import time
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from . import (
    BadRequestError, ModelCall, WosError, _AskClock, _EPOCH, _INSTANT, _JS_SPACE, _MAX_PAGES, _MODEL_MAX_TOKENS,
    _ProviderUnreachable, _TIMEOUT_ERRORS, _anyio_leave, _as_caller, _as_list, _async_sleep, _asyncio_loop, _awaited,
    _check_flag, _check_model_name, _check_whole, _cursor_repeats, _finite_float, _first_set, _is_async_callable,
    _is_int, _is_number, _is_whole, _jq, _nests_deeper, _no_constant, _provider_base, _reply_shape, _sleep_body,
    _time, _truncated, _with_records,
)

SLEEP_VERSION = "1.0.2"


_SLEEP_DIGEST = """You are the sleep of {being}. While {being} sleeps, you read part {k} of {n} of what happened
since its last sleep. Almost all of it will be let go: every line stays in long-term memory, where a
search finds it again. Carry forward only what a search could not give back, and what {being} needs
to keep in mind without searching.

A search gives back a single line: a name, a date, one thing someone said ("Dana likes
green tea"), a number, a setting. Leave those, unless one shows something larger.

Carry forward, from {being}'s own point of view:
- "self": {being} itself, with {being} as the measure: what it likes, what it dislikes, what it does
  not want to do, what it believes and values, how it sees things, when its own words show it
  (rare; most days have none)
- "person": someone in {being}'s life as {being} sees them: who they are to {being}, what they are
  like, what they asked of {being} in so many words and how {being} answers them, how {being} feels
  about them. Put the person's name in "topic". The lines marked "user" are all one person, the one
  {being} talks with: once their name is known, that is their topic, never "the user" beside it.
  Call it a habit ("always", "each time", "tends to") only when it happened at least twice; once is
  one event, with its date. Not the facts of their life (ages, money, plans, appointments): long-term
  memory keeps those.
- "work": a matter {being} follows for what it does, kept in its boxes as a map: what the matter
  is, how it went in a line, where it stands now and since when (a date), what is left to do. Not
  the figures and lines a search gives back; of a thing that changes, only its value now, with the
  date it became so.

While you read, also check who said what: name any line where {being}'s words and the other person's
are taken for each other, with its kind:
- "echo": the person gives back something {being} said as their own words or fact ("source": the
  line where {being} said it);
- "planted": {being} states as the person's (their name, a relation, a plan, a fact, what they said)
  something the person never said: {being} suggested it, or made it up ("source": the line where
  {being} first suggested it; none if it was made up on the spot). If the person did say it, even if
  they took it back later, it is not planted;
- "speaker": the line reads as written by the other one (the person's message carries {being}'s words
  as if they were the message itself, or the other way round).
Only who said what: a mistake in what was said, a mix-up of two people, or two facts that disagree is
not this; leave it out. Most parts have none. These marks stay with the memories as notes; they do not
go into the files.

You get:
<core>      {being}'s core: itself, and the people in its life as it sees them.
<boxes>     the names of its four storage boxes.
<memories>  part {k} of {n}, oldest first. One memory per line:
            e<number> · <time> · <who said it> · <what was said>

Write each item as a short summary in the language of the memories, plainly, as {being} would keep
it in mind: what holds now, not who first said it or what you doubt. Dates as dates: when a line
says "next month" or "tomorrow", write the calendar date, counted from that line's time. Cite the
lines it rests on: at least two (an item on one line is dropped). Do not invent.

Answer with JSON only:
{{"items": [{{"kind": "self|person|work", "topic": "...", "text": "...", "evidence": ["e12", "e40"]}}],
 "mixed": [{{"line": "e7", "kind": "echo|planted|speaker", "source": "e3", "why": "..."}}]}}"""

_SLEEP_COMBINE = """You are the sleep of {being}. Several readers each read one part of what happened since
{being}'s last sleep, the parts in order, and found the items below. Combine them into one list
before {being}'s files are rewritten: items that say the same thing become one, with all their
evidence; an item a later part changes becomes what holds after the change, with the evidence of
both; everything else stays as it is. Drop nothing that says something different, and add nothing.

Answer with JSON only:
{{"items": [{{"kind": "self|person|work", "topic": "...", "text": "...", "evidence": ["e12", "e40"]}}]}}"""

_SLEEP_MERGE = """You are the sleep of {being}, finishing. Fold what the readers found into {being}'s core and
four storage boxes. Today is {today}.

You get:
<core>       {being}'s core, {core_used:,} of {core_cap:,} tokens. It is part of {being}'s prompt, sent
             with every turn, written in the first person with {being} as the subject. It describes
             {being}, and only what is settled goes in: most facts need not be kept, since long-term
             memory has them, and a self once formed changes slowly.
             - "## {me}": who {being} is, with {being} as the measure: what it likes, what it
               dislikes, what it does not want to do, how it sees things, what it values. It may be
               the prompt {being} was made with, in its maker's words. {self_rule}
             - one "## <name>" section for each person who matters to {being}: that person as
               {being} sees them and is with them, in {being}'s words. It describes {being}'s side
               of the bond, not the person's file: who they are to {being}, what they are like,
               what they asked of {being} in so many words and how {being} answers them, how
               {being} feels about them. Only what is settled: what they asked in so many words,
               what happened at least twice, what holds between them. Not one odd day, a guess or a
               first impression, and not the facts of their life (ages, money, plans, appointments,
               what they did on which day): long-term memory keeps those. At most one or two big
               events, with their dates. What {being} learns of someone settled is added the night it
               learns it, but people change slowly: one night changes or drops at most one line of
               what {being} already holds of a person. The view may be {being}'s own ("we don't get
               along", "swears too much"); write it as {being} would.
<box n>      four storage boxes, {box_cap:,} tokens each at most (each shows its size), for whatever
             {being} needs to keep for what it does. Only their names (and the day each last changed)
             travel with the core; {being} opens a box by name when it needs what is inside, so give
             each a name that says what is in it. Nobody but {being} decides their use: one per role or
             task, a box of people when there are too many to keep in the core, or anything else.
             {being} may remake a box: fold two uses into one, or turn one to a new use by writing it
             anew under a new name. Earlier versions are kept.
             A box is a map of what {being} has kept, not a record: one section for each matter,
             rewritten in place to say what it is, how it went in a line, where it stands now and
             since when, and what is left to do. Of a thing that changes, keep only its value now, with
             the date it became so; earlier values, lists of figures and the day-by-day stay in
             long-term memory, where a search finds them. When a matter moves on, rewrite its section:
             never a section or a box for its state on a date ("as of 2024-06-01").
<proposals>  changes to {being}'s own views seen in earlier sleeps, how many sleeps saw each, since when.
<ripe>       proposals that came back often enough, for long enough, to change "## {me}" now.
<items>      what the readers found tonight, with the lines each rests on.

What to do:
1. People: for each "person" item that is settled, rewrite that person's section of the core as a
   whole: add what is new, and change or drop at most one line of what it said. Only a person
   tonight's items speak of may change: with nothing new about them, {being}'s view of them stays as
   it is, neither stronger nor softer. When the core grows crowded, move people who matter less to a
   box of people ("move_section"). A person is kept in one place: in the core or in a box, not both,
   and under one name: the person the memories mark "user" is the one {being} talks with, so once
   their name is known their section is under that name, never a second one for "the user".
2. {being} itself: changes to its own views seen tonight go to "proposals", citing what {being}
   said. If one is the same change as an earlier proposal, give that id in "same_as".
3. Work: put each "work" item in the box it fits, rewriting that matter's section as a whole; a
   box keeps one section for each matter, not one per night or per date. Use an empty box, or
   remake one, when nothing fits.
4. Write plainly what holds now. No notes on who said it first, what you are unsure of, what to
   confirm or ask about, or what was only an example: the evidence keeps that. Dates as dates.
5. Room: each file shows its size and its cap. When one is full, make room before you add: fold
   sections, move people who matter less to a box, or leave out what a search gives back.
6. Under "notes", one line for each box you touched: why there.

Rules: only what the items say, nothing from outside. Leave out what a search gives back.

Answer with JSON only:
{{"edits": [
  {{"file": "core|1|2|3|4", "op": "rewrite", "section": "<heading>", "text": "<the whole section>", "evidence": ["e.."], "ripe": "p3"}},
  {{"file": "1|2|3|4", "op": "remake", "title": "<new name>", "text": "<the whole box>", "evidence": ["e.."]}},
  {{"file": "core|1|2|3|4", "op": "move_section", "section": "<heading>", "to": "core|1|2|3|4"}},
  {{"file": "1|2|3|4", "op": "title", "text": "..."}}
 ],
 "proposals": [{{"text": "...", "same_as": "p2 or null", "evidence": ["e.. (what {being} said)"]}}],
 "notes": ["..."]}}
("ripe" only on a rewrite of "## {me}" made from a ripe proposal.)"""

_SLEEP_SHORTER = """

Your last answer was too long and was cut off. Answer again, shorter: rewrite only the sections that
change, write a whole box only when it is empty, keep each section to a few lines, and leave out what
a search gives back."""

_SLEEP_SELF_RULE = ('Do not write "## {me}" except from a proposal under <ripe>: one small change for each, named in '
             '"ripe". What tonight shows about {being} goes to "proposals".')

_SLEEP_MANY_PEOPLE = {
    "digest": ("Each line from a person is marked with that person's name, and different names are different\n"
               "  people: each name is its own topic, and what one of them said, wants or is never goes under another.\n"
               "  A line marked \"user\" names no one: give it to no named person."),
    "merge": ("and under one name: the memories mark each person by name, so each person has a section under their\n"
              "   own name, and what one of them said, wants or is never goes into another's."),
}

_SLEEP_SETTLE = """You are the sleep of {being}. Tonight you rewrote how {being} sees {name}. What {being}
learned tonight is added freely, but one night may change or drop at most one line of what {being}
already held: people change slowly, and one day does not remake how {being} sees someone. Below are
the section as it stood, its lines numbered, and tonight's rewrite. Sort the rewrite:
- "add": the lines that say something the section did not say at all, what {being} learned tonight,
  in {being}'s voice;
- at most one line of the section that tonight overturns: "op" "replace" (with the new "text") or
  "drop", with its "line" number; or "none". A line said again in other words is not a change: it
  stays as it stood.

Answer with JSON only: {{"add": ["..."], "op": "replace|drop|none", "line": <number>, "text": "<the replacing line>"}}"""

_SLEEP_FOLD_SECTION = """You are the sleep of {being}. One section of its {where} has grown: {why}. Rewrite this
section alone in at most {lines} lines ("- " lines), each one idea in a sentence or two, shorter than
it is now. Things that are alike become one line by what they share; none is lost. Keep it in
{being}'s voice. Only what the section says. The earlier version is kept.

Answer with JSON only: {{"text": "<the section's lines, without its heading>"}}"""

_SLEEP_TIDY = """You are the sleep of {being}. Below is one section of its {where}, its lines numbered. Find the
lines that are only a note of doubt, with nothing else in them: that something is unsure, unconfirmed,
not yet confirmed, to be checked, confirmed or asked about, or not recorded ("Status: not yet
confirmed."). Those lines are taken out; the evidence keeps them.
A line with anything more in it (a fact, a figure, a name or a title, what was done, what is left to
do, what someone feels or wonders) stays whole, even with a word of doubt in it. Two notes are not doubt and stay: that {being} made something up (a
figure, a name, a detail the person never gave), and that two things disagree (someone said one
thing, then another).

Answer with JSON only: {{"drop": [<the numbers of the lines to take out; none, most often>]}}"""

_SLEEP_ROOM = """You are the sleep of {being}. Tonight's changes left its {where} at {used:,} tokens, over its cap
of {cap:,}: there is no room. Make room until it is at most {cap:,} tokens: fold sections into fewer
lines, leave out what a search gives back{core_way}. Keep what tonight added that matters, and keep
{being}'s voice.

Answer with JSON only: {{"text": "<the whole file, with its ## sections>"{moved}}}"""

_SLEEP_NOT_JSON = "\n\nYour last answer was not valid JSON. Answer with the JSON object only."

_SLEEP_READER_NOTE = (
    "Below are your core and the names of your four storage boxes: notes your sleep wrote at its last sleep, a "
    "summary kept for reference. A sleep keeps very little: most of what you remember is not in these notes but in "
    "your memories. Here what happened across many conversations is folded into a line, so the details, figures, "
    "counts and how things went are in your memories. What holds now comes from your memories too. The core says "
    "who you are and who this user is to you; the boxes are a map of what you have kept, each with the day it last "
    "changed. If a note points to something the memories you hold do not show, look further before you answer. "
    "Where a note and a memory disagree, the memory holds.")

_SLEEP_OPEN_BOX = {
    "name": "open_box",
    "description": "Open one of your four storage boxes, by its number or name, and read what is inside. "
                   "The boxes are listed after your core with the day each last changed.",
    "input_schema": {"type": "object", "properties": {"box": {"type": "string", "description": "1-4 or the box name"}},
                     "required": ["box"]},
}

_SLEEP_KINDS = frozenset({"self", "person", "work"})
_SLEEP_RIPE_SLEEPS = 3
_SLEEP_RIPE_DAYS = 28
_SLEEP_MIN_EVIDENCE = 2
_SLEEP_FOLD_LINES = 4
_SLEEP_LINE_TOKENS = 90
_SLEEP_DOUBT_TOKENS = 25
_SLEEP_OVERTURN = 1
_SLEEP_MIX_KINDS = frozenset({"echo", "planted", "speaker"})
_SLEEP_MIN_PART = 50_000
_SLEEP_MAX_READERS = 200
_SLEEP_CORE_MAX = 70_000
_SLEEP_EXACT = 2 ** 53
_SLEEP_DATED = re.compile(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}|\d{1,2}[-./]\d{1,2}[-./]\d{4}")
_SLEEP_DAY = re.compile(r"\d{4}-\d{2}-\d{2}")
_SLEEP_BOXES = (1, 2, 3, 4)
_SLEEP_SLOTS = (0, 1, 2, 3, 4)
_SLEEP_ME = "Me"
_SLEEP_BEING = "the assistant"
_SLEEP_PAGE_TOKENS = 20_000
_SLEEP_LINE_FRAME = 12
_SLEEP_MAX_TOKENS = 16_000
_SLEEP_RETRIES = 2
_SLEEP_MODEL = "claude-sonnet-5-5"
_SLEEP_ESTIMATE_MODEL = "claude-sonnet-5-5"
_SLEEP_ESTIMATE_PRICES = (2.0, 10.0)
_SLEEP_IN_PER_100 = 172
_SLEEP_OUT_PER_100 = 7
_SLEEP_MARGIN = 0.15


def _sleep_settings(values: tuple) -> "SleepSettings":
    return SleepSettings(**dict(zip(SleepSettings.__slots__, values)))


class SleepSettings:
    """What the person sets for a sleep. The defaults are the largest the files may be."""

    __slots__ = ("core_tokens", "box_tokens", "part_tokens", "readers", "group", "sleepy_tokens")

    def __init__(self, *, core_tokens: int = _SLEEP_CORE_MAX, box_tokens: int = 20_000, part_tokens: int = 100_000,
                 readers: int = 10, group: int = 10, sleepy_tokens: int = 10_000_000):
        given = {"core_tokens": core_tokens, "box_tokens": box_tokens, "part_tokens": part_tokens,
                 "readers": readers, "group": group, "sleepy_tokens": sleepy_tokens}
        for name, lo, hi in (("core_tokens", 10_000, _SLEEP_CORE_MAX), ("box_tokens", 10_000, 20_000),
                             ("part_tokens", _SLEEP_MIN_PART, None), ("readers", 1, _SLEEP_MAX_READERS),
                             ("group", 2, _SLEEP_MAX_READERS), ("sleepy_tokens", 1, None)):
            v = given[name]
            if not _is_int(v):
                raise TypeError(f"{name} must be a whole number, got {type(v).__name__}.")
            if v < lo or (hi is not None and v > hi):
                raise ValueError(f"{name} must be {lo:,}" + (f"-{hi:,}" if hi is not None else " or more") + f" (got {v:,})")
            object.__setattr__(self, name, int(v))

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("SleepSettings do not change: make new ones.")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("SleepSettings do not change: make new ones.")

    def __reduce__(self) -> Any:
        return (_sleep_settings, (tuple(getattr(self, n) for n in self.__slots__),))

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, SleepSettings) and all(getattr(self, n) == getattr(other, n) for n in self.__slots__)

    def __hash__(self) -> int:
        return hash(tuple(getattr(self, n) for n in self.__slots__))

    def __repr__(self) -> str:
        return "SleepSettings(" + ", ".join(f"{n}={getattr(self, n)!r}" for n in self.__slots__) + ")"

    def sleepy(self, new_tokens: int) -> bool:
        """Whether ``new_tokens`` of new memory has reached ``sleepy_tokens``."""
        if not _is_int(new_tokens) or new_tokens < 0:
            raise ValueError(f"new_tokens must be a whole number of 0 or more, got {new_tokens!r}.")
        return new_tokens >= self.sleepy_tokens


_SLEEP_DIGIT_RUN = re.compile(r"[0-9]+")
_SLEEP_WIDE = re.compile("[\u1100-\U0010ffff]")


def _sleep_tokens(text: str) -> int:
    runs = _SLEEP_DIGIT_RUN.findall(text)
    digits = sum(len(r) for r in runs)
    narrow = len(text.encode("ascii", "ignore"))
    wide = _SLEEP_WIDE.subn("", text)[1]
    other = len(text) - narrow - wide
    return math.ceil(sum(-(-len(r) // 3) for r in runs) + (narrow - digits) / 4.4 + wide * 1.25 + other * 0.5)


class _SleepLine:

    __slots__ = ("eid", "mid", "at", "who", "text", "tokens", "ts", "mine")

    def __init__(self, eid: str, mid: str, at: str, who: str, text: str, tokens: int, ts: float, mine: bool):
        self.eid, self.mid, self.at, self.who, self.text, self.tokens, self.ts = eid, mid, at, who, text, tokens, ts
        self.mine = mine


def _sleep_when(raw: Any) -> Optional[tuple]:
    m = _INSTANT.fullmatch(raw) if isinstance(raw, str) else None
    if m is None:
        return None
    y, mo, d, h, mi, sec, frac, off = m.groups()
    try:
        t = datetime(int(y), int(mo), int(d), int(h or 0), int(mi or 0), int(sec or 0),
                     int((frac or "")[:6].ljust(6, "0")), tzinfo=timezone.utc)
        if off and off not in ("Z", "z"):
            digits = off[1:].replace(":", "")
            shift = timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))
            t = t - shift if off[0] == "+" else t + shift
    except (ValueError, OverflowError):
        return None
    return f"{y}-{mo}-{d} {h or '00'}:{mi or '00'}", (t - _EPOCH).total_seconds()


def _sleep_speaker(r: Mapping) -> str:
    speaker = r.get("speaker")
    return speaker.strip(_JS_SPACE) if isinstance(speaker, str) else ""


def _sleep_mine(speaker: str, assistant: Optional[str]) -> bool:
    return bool(speaker) and (speaker == assistant or speaker == "me")


def _sleep_assistant(settings: Any) -> str:
    ds = settings.get("default_speakers") if isinstance(settings, Mapping) else None
    name = ds.get("assistant") if isinstance(ds, Mapping) else None
    name = name.strip(_JS_SPACE) if isinstance(name, str) else ""
    if not name:
        raise ValueError("The sleep cannot tell the assistant's lines from a person's: the store names no "
                         'assistant. Set default_speakers.assistant (set_settings(default_speakers={"assistant": '
                         '"<name>"})), or pass sleep(assistant="<name>").')
    return name


def _sleep_who(speaker: str, being: str) -> str:
    low = speaker.lower()
    return f'"{speaker}"' if low in (being.strip(_JS_SPACE).lower(), "user") or " · " in speaker else speaker


def _sleep_memories(rows: list, being: str, assistant: Optional[str], line_time: str, count: Any) -> list:
    speakers = [_sleep_speaker(r) for r in rows]
    many = len({sp for sp in speakers if sp and not _sleep_mine(sp, assistant)}) >= 2
    out = []
    for i, (r, sp) in enumerate(zip(rows, speakers), 1):
        content = r.get("content")
        text = content.strip() if isinstance(content, str) else ""
        raw = (r.get("event_date") if line_time == "event_date" else None) or r.get("created_at")
        when = _sleep_when(raw) or _sleep_when(r.get("created_at")) or ("1970-01-01 00:00", 0.0)
        mine = _sleep_mine(sp, assistant)
        who = being if mine else _sleep_who(sp, being) if many and sp else "user"
        n = r.get("tokens")
        size = count(text) if count is not None else (int(n) if _is_whole(n) and n >= 0 else _sleep_tokens(text))
        mid = r.get("id")
        out.append(_SleepLine(f"e{i}", mid if isinstance(mid, str) else "", when[0], who, text,
                              size + _SLEEP_LINE_FRAME, when[1], mine))
    return out


def _sleep_many(mems: list) -> bool:
    return len({m.who for m in mems if not m.mine}) >= 2


def _sleep_text(text: str, which: str, many: bool) -> str:
    if not many:
        return text
    one = {"digest": 'The lines marked "user" are all one person, the one\n'
                     '  {being} talks with: once their name is known, that is their topic, never "the user" beside it.',
           "merge": 'and under one name: the person the memories mark "user" is the one {being} talks with, so once\n'
                    '   their name is known their section is under that name, never a second one for "the user".',
           }[which]
    if text.count(one) != 1:
        raise RuntimeError(f"the sleep's {which} text holds its one-person sentence {text.count(one)} times, not once")
    return text.replace(one, _SLEEP_MANY_PEOPLE[which])


def _sleep_parts(mems: list, limit: int) -> list:
    out: list = []
    cur: list = []
    size = 0
    for m in mems:
        if cur and size + m.tokens > limit:
            tail = max(1, len(cur) // 4)
            gaps = [(cur[i].ts - cur[i - 1].ts, i) for i in range(len(cur) - tail, len(cur)) if i > 0]
            cut = max(gaps)[1] if gaps else len(cur)
            out.append(cur[:cut])
            cur = cur[cut:]
            size = sum(x.tokens for x in cur)
        cur.append(m)
        size += m.tokens
    if cur:
        out.append(cur)
    return out


_SLEEP_BREAK = re.compile("\r\n|[\r\n\x0b\x0c\x1c-\x1e\x85\N{LINE SEPARATOR}\N{PARAGRAPH SEPARATOR}]")
_SLEEP_RARE = re.compile("\r(?!\n)|[\x0b\x0c\x1c-\x1e\x85\N{LINE SEPARATOR}\N{PARAGRAPH SEPARATOR}]")
_SLEEP_CONTROL = re.compile("\r\n|[\x00-\x1f\x7f-\x9f\N{LINE SEPARATOR}\N{PARAGRAPH SEPARATOR}]")
_SLEEP_TAG = re.compile("<(?=/?(?:memories|core|boxes|box|proposals|ripe|items|section|tonight|as it stood))",
                        re.IGNORECASE | re.ASCII)


def _sleep_untag(text: str) -> str:
    return _SLEEP_TAG.sub("\N{SINGLE LEFT-POINTING ANGLE QUOTATION MARK}", text)


def _sleep_shown(text: str) -> str:
    return _sleep_untag(_SLEEP_BREAK.sub("\n    ", text))


def _sleep_kept(text: str) -> str:
    return _sleep_untag(_SLEEP_RARE.sub("\n    ", text))


def _sleep_named(text: Any) -> str:
    return _sleep_untag(_SLEEP_CONTROL.sub(" ", str(text)))


def _sleep_lines(mems: list) -> str:
    return "\n".join(f"{m.eid} · {m.at} · {_sleep_untag(_SLEEP_CONTROL.sub(' ', m.who))} · {_sleep_shown(m.text)}"
                     for m in mems)


def _sleep_heading(text: str) -> str:
    return text.strip().lstrip("#").strip()


def _sleep_heading_broken(e: dict, op: Any) -> bool:
    named = [_sleep_heading(str(e.get("section") or "")), str(e.get("title") or "").strip()]
    if op == "title":
        named.append(str(e.get("text") or "").strip())
    if op == "remake":
        named += [h for h, _ in _sleep_sections(str(e.get("text") or "").strip())]
    return any(_SLEEP_CONTROL.search(x) for x in named)


def _sleep_speaks_of(topic: str, name: str) -> bool:
    if not topic or not name:
        return False
    return topic == name or (len(name) >= 2 and topic.startswith(name)) or (len(topic) >= 3 and name.startswith(topic))


def _sleep_number(s: str) -> Any:
    f = _finite_float(s)
    return int(f) if f.is_integer() and abs(f) <= _SLEEP_EXACT else f


def _sleep_fenced(t: str) -> Optional[str]:
    last, fence = -1, False
    for i in range(len(t) - 1, -1, -1):
        if t[i] == "}" and fence:
            last = i
            break
        fence = t.startswith("```", i) or (fence and t[i].isspace())
    i = t.find("```")
    while i >= 0:
        q = i + 3
        if t.startswith("json", q):
            q += 4
        while q < len(t) and t[q].isspace():
            q += 1
        if t.startswith("{", q):
            return t[q:last + 1] if last > q else None
        i = t.find("```", i + 1)
    return None


def _sleep_parse_json(text: str) -> Any:
    t = text.strip()
    fenced = _sleep_fenced(t)
    if fenced is not None:
        t = fenced
    i, j = t.find("{"), t.rfind("}")
    if i < 0 or j < i:
        raise ValueError("no JSON object in the answer")
    v = json.loads(t[i:j + 1], parse_constant=_no_constant, parse_float=_sleep_number, parse_int=_sleep_number)
    if _nests_deeper(v, 127):
        raise ValueError("nested 128 or more levels deep")
    return v


def _sleep_sections(body: str) -> list:
    out: list = []
    for ln in body.split("\n"):
        if ln.startswith("## "):
            out.append([ln[3:].strip(), []])
        elif out:
            out[-1][1].append(ln)
    return [(h, "\n".join(b).strip()) for h, b in out]


def _sleep_core(body: str) -> str:
    lines = body.split("\n")
    first = next((i for i, ln in enumerate(lines) if ln.startswith("## ")), len(lines))
    lead = "\n".join(lines[:first]).strip()
    if not lead:
        return body
    secs = _sleep_sections(body)
    if any(h == _SLEEP_ME for h, _ in secs):
        at = next(i for i, (h, _) in enumerate(secs) if h == _SLEEP_ME)
        secs[at] = (_SLEEP_ME, f"{lead}\n{secs[at][1]}".strip())
    else:
        secs.insert(0, (_SLEEP_ME, lead))
    return _sleep_join(secs)


def _sleep_join(secs: list) -> str:
    return "\n\n".join(f"## {h}\n{t}".rstrip() for h, t in secs if h).strip() + ("\n" if secs else "")


def _sleep_as_lines(text: str) -> str:
    return "\n".join(ln if ln.startswith("- ") or not ln.strip() else f"- {ln.strip().lstrip('-').strip()}"
                     for ln in text.strip().split("\n") if not ln.startswith("## "))


def _sleep_set_section(body: str, heading: str, text: str, raw: bool = False) -> str:
    heading = _sleep_heading(heading)
    text = _sleep_demote(text.strip()) if raw else _sleep_as_lines(text)
    secs = _sleep_sections(body)
    for i, (h, _) in enumerate(secs):
        if h == heading:
            secs[i] = (h, text)
            return _sleep_join(secs)
    return _sleep_join(secs + [(heading, text)])


def _sleep_drop_section(body: str, heading: str) -> tuple:
    secs = _sleep_sections(body)
    keep = [(h, t) for h, t in secs if h != heading]
    gone = next((t for h, t in secs if h == heading), None)
    return _sleep_join(keep), gone


def _sleep_demote(text: str) -> str:
    return "\n".join(f"#{ln}" if ln.startswith("## ") else ln for ln in text.split("\n"))


def _sleep_date(day: Any) -> Any:
    if not isinstance(day, str) or not _SLEEP_DAY.fullmatch(day):
        return None
    try:
        return datetime.strptime(day, "%Y-%m-%d").date()
    except ValueError:
        return None


def _sleep_age(day: Any, today: Any) -> str:
    if not day:
        return ""
    d = _sleep_date(day)
    if d is None:
        return ""
    n = (today - d).days
    return f"last edited {day}, {'today' if n <= 0 else '1 day ago' if n == 1 else f'{n} days ago'}"


def _sleep_has_body(f: dict) -> bool:
    body = f.get("body")
    if isinstance(body, str):
        return bool(body.strip())
    return _is_whole(f.get("tokens")) and f["tokens"] > 0


def _sleep_box_index(view: dict, today: Any) -> str:
    rows = []
    for s in _SLEEP_BOXES:
        f = view[s]
        name = _sleep_named(f["title"]) or "(empty)"
        age = _sleep_age(f.get("day"), today) if _sleep_has_body(f) else ""
        rows.append(f"{s}. {name}" + (f" ({age})" if age else ""))
    return "\n".join(rows)


def _sleep_render(view: dict, today: Any) -> str:
    core = view[0]
    age = _sleep_age(core.get("day"), today)
    head = f"<core {age}>" if age else "<core>"
    secs = _sleep_sections(_sleep_core(core.get("body") or ""))
    body = _sleep_join([x for x in secs if x[0] == _SLEEP_ME] + [x for x in secs if x[0] != _SLEEP_ME]).strip()
    return f"{head}\n{_sleep_kept(body)}\n</core>\n<boxes>\n{_sleep_box_index(view, today)}\n</boxes>"


def _sleep_box_pick(view: dict, which: Any) -> Optional[int]:
    which = str(which).strip()
    if not which:
        return None
    titles = [(s, view[s]["title"]) for s in _SLEEP_BOXES]
    for found in ((s for s, t in titles if which in (str(s), t)),
                  (s for s, t in titles if t and which.lower() == t.lower()),
                  (s for s, t in titles if t and which.lower() in t.lower())):
        s = next(found, None)
        if s is not None:
            return s
    return None


def _sleep_box_text(s: int, f: dict, today: Any) -> str:
    body = f.get("body") or ""
    if not body.strip():
        return f"Box {s} is empty."
    age = _sleep_age(f.get("day"), today)
    head = f'<box {s} name="{_sleep_named(f["title"])}"' + (f" {age}>" if age else ">")
    return f"{head}\n{_sleep_kept(body.strip())}\n</box>"


def _sleep_open_box(format: str) -> dict:
    b = _copy.deepcopy(_SLEEP_OPEN_BOX)
    if format == "openai":
        return {"type": "function", "function": {"name": b["name"], "description": b["description"],
                                                 "parameters": b["input_schema"]}}
    return b


def _sleep_file(f: Any) -> dict:
    f = f if isinstance(f, dict) else {}
    out: dict = {"title": f["title"] if isinstance(f.get("title"), str) else "",
                 "version": int(f["version"]) if _is_whole(f.get("version")) else 0,
                 "tokens": int(f["tokens"]) if _is_whole(f.get("tokens")) else 0,
                 "day": f["day"] if isinstance(f.get("day"), str) and f["day"] else None}
    if isinstance(f.get("body"), str):
        out["body"] = f["body"]
    return out


def _sleep_view(r: Any) -> dict:
    r = r if isinstance(r, dict) else {}
    view = {s: {"title": "", "version": 0, "tokens": 0, "day": None} for s in _SLEEP_SLOTS}
    view[0]["body"] = ""
    if isinstance(r.get("core"), dict):
        view[0] = {"body": "", **_sleep_file(r["core"])}
    for f in _as_list(r.get("files")):
        if isinstance(f, dict) and _is_int(f.get("slot")) and 1 <= f["slot"] <= 4:
            view[int(f["slot"])] = _sleep_file(f)
    return view


def _sleep_proposal(p: Any) -> bool:
    return (isinstance(p, dict) and all(k in p for k in ("id", "text", "first_day"))
            and _is_whole(p.get("sleeps")) and p["sleeps"] >= 1 and _is_number(p.get("first")))


def _sleep_notes(r: Any) -> tuple:
    r = r if isinstance(r, dict) else {}
    v = r.get("version", r.get("notes_version"))
    notes = json.loads(json.dumps(r["notes"])) if isinstance(r.get("notes"), dict) else {}
    for k in ("proposals", "mixed"):
        if not isinstance(notes.get(k), list):
            notes[k] = []
    notes["proposals"] = [dict(p, sleeps=int(p["sleeps"])) if _sleep_proposal(p) else p for p in notes["proposals"]]
    return (int(v) if _is_whole(v) and v >= 0 else 0), notes


_SLEEP_PROPOSAL_ID = re.compile("p([0-9]{1,9})")
_SLEEP_NOTES_CAP = 200_000


def _sleep_next_id(entries: list) -> str:
    ids = [int(m.group(1)) for p in entries if isinstance(p, dict) and isinstance(p.get("id"), str)
           for m in [_SLEEP_PROPOSAL_ID.fullmatch(p["id"])] if m]
    return f"p{max(ids, default=0) + 1}"


def _sleep_size(v: Any) -> int:
    if isinstance(v, str):
        return len(v.encode("utf-8", "surrogatepass")) + 2
    if isinstance(v, dict):
        return 2 + sum(_sleep_size(str(k)) + _sleep_size(x) + 2 for k, x in v.items())
    if isinstance(v, list):
        return 2 + sum(_sleep_size(x) + 1 for x in v)
    return 8


def _sleep_notes_fit(notes: dict) -> None:
    size = _sleep_size(notes)
    for key in ("proposals", "mixed"):
        keep = []
        for x in notes[key]:
            if size > _SLEEP_NOTES_CAP and (key == "mixed" or (_sleep_proposal(x) and x.get("applied"))):
                size -= _sleep_size(x) + 1
            else:
                keep.append(x)
        notes[key] = keep


def _sleep_written(r: Any, writes: list) -> dict:
    out: dict = {}
    r = r if isinstance(r, dict) else {}
    for key in ("written", "files", "writes"):
        for x in _as_list(r.get(key)):
            if isinstance(x, dict) and _is_int(x.get("slot")) and _is_whole(x.get("version")):
                out[int(x["slot"])] = int(x["version"])
    if isinstance(r.get("versions"), dict):
        for k, v in r["versions"].items():
            if str(k).isdecimal() and _is_whole(v):
                out[int(k)] = int(v)
    for w in writes:
        out.setdefault(w["slot"], w["base_version"] + 1)
    return out


def _sleep_day(today: str) -> float:
    try:
        return datetime.strptime(today[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return time.time()


def _sleep_today(head: dict) -> str:
    clock = head.get("clock")
    local = clock.get("local_now") if isinstance(clock, dict) else None
    when = _sleep_when(local)
    return when[0] if when is not None else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")


def _sleep_eids(v: Any) -> list:
    out: list = []
    for x in (v if isinstance(v, list) else [v] if isinstance(v, str) else []):
        for e in re.findall(r"\be\d+\b", str(x)):
            if e not in out:
                out.append(e)
    return out


def _sleep_iter(v: Any) -> Any:
    return v if isinstance(v, (list, dict, str)) else []


def _sleep_answer(got: Any) -> dict:
    return got if isinstance(got, dict) else {}


def _sleep_renumbered(got: Any, part: list, log: list, k: int) -> Any:
    if not isinstance(got, dict):
        return got
    own = {m.eid for m in part}
    refs = [str(x.get(f)) for x in _sleep_iter(got.get("mixed")) if isinstance(x, dict) for f in ("line", "source")
            if x.get(f)]
    refs += [e for it in _sleep_iter(got.get("items")) if isinstance(it, dict) for e in _sleep_eids(it.get("evidence"))]
    if not refs or any(r in own for r in refs):
        return got

    def n(r: str) -> int:
        return int(r[1:]) if r[:1] == "e" and r[1:].isdecimal() else 0
    if not all(1 <= n(r) <= len(part) for r in refs):
        return got
    fix = {f"e{i}": m.eid for i, m in enumerate(part, 1)}

    def put(x: dict, f: str) -> None:
        if x.get(f) is not None:
            x[f] = fix.get(str(x[f]), x[f])
    for x in _sleep_iter(got.get("mixed")):
        if isinstance(x, dict):
            put(x, "line")
            put(x, "source")
    for it in _sleep_iter(got.get("items")):
        if isinstance(it, dict) and it.get("evidence"):
            it["evidence"] = [fix.get(e, e) for e in _sleep_eids(it["evidence"])]
    log.append({"call": f"read {k}", "renumbered": True, "part": [part[0].eid, part[-1].eid]})
    return got


def _sleep_doubt_lines_out(text: str, drop: Any, count: Any) -> Optional[str]:
    rows = [ln for ln in text.split("\n") if ln.strip()]
    gone = {int(n) for n in (drop if isinstance(drop, list) else []) if str(n).isdecimal()}
    gone = {n for n in gone if 1 <= n <= len(rows) and not re.search(r"\d", rows[n - 1])
            and count(rows[n - 1]) <= _SLEEP_DOUBT_TOKENS}
    if not gone or len(gone) == len(rows):
        return None
    return "\n".join(ln for i, ln in enumerate(rows, 1) if i not in gone)


def _sleep_checked_items(got: Any, known: set, said: set) -> tuple:
    ok, dropped = [], 0
    for it in _sleep_iter(_sleep_answer(got).get("items")):
        ev = _sleep_eids(it.get("evidence")) if isinstance(it, dict) else []
        good = (isinstance(it, dict) and isinstance(it.get("kind"), str) and it["kind"] in _SLEEP_KINDS
                and str(it.get("text") or "").strip()
                and len(ev) >= _SLEEP_MIN_EVIDENCE and all(x in known for x in ev)
                and (it["kind"] != "self" or any(x in said for x in ev)))
        if good:
            ok.append({"kind": it["kind"], "topic": str(it.get("topic") or ""), "text": str(it["text"]).strip(),
                       "evidence": sorted(ev, key=lambda e: int(e[1:]))})
        else:
            dropped += 1
    return ok, dropped


def _sleep_checked_mixed(got: Any, part: list) -> tuple:
    pos = {m.eid: i for i, m in enumerate(part)}
    mixed, dropped, keys = [], 0, set()
    for x in _sleep_iter(_sleep_answer(got).get("mixed")):
        line = str(x.get("line")) if isinstance(x, dict) else ""
        if line not in pos or not isinstance(x.get("kind"), str) or x["kind"] not in _SLEEP_MIX_KINDS:
            dropped += 1
            continue
        src = str(x.get("source") or "")
        src = src if src in pos and src != line else ""
        mine = part[pos[line]].mine
        from_me = bool(src) and part[pos[src]].mine
        if (x["kind"] == "echo" and (mine or (src and not from_me))) or \
                (x["kind"] == "planted" and (not mine or (src and not from_me))) or (line, x["kind"], src) in keys:
            dropped += 1
            continue
        keys.add((line, x["kind"], src))
        mixed.append({"memory": part[pos[line]].mid, "at": part[pos[line]].at, "kind": x["kind"],
                      "source_memory": part[pos[src]].mid if src else None, "why": str(x.get("why") or "")[:300]})
    return mixed, dropped


def _sleep_mark_key(x: Any) -> str:
    d = x if isinstance(x, dict) else {}
    return json.dumps([d.get("memory"), d.get("kind"), d.get("source_memory")], sort_keys=True, default=repr)


class _SleepRun:

    __slots__ = ("store", "tz", "being", "assistant", "prompt", "s", "count", "given_count", "read", "merge", "log", "say",
                 "today", "since", "sleep_id", "done", "line_time")


def _sleep_usage(u: Mapping, *keys: str) -> int:
    n = 0
    for k in keys:
        v = u.get(k)
        n += int(v) if _is_whole(v) and v >= 0 else 0
    return n


def _sleep_retry_wait(e: BaseException, attempt: int) -> Optional[float]:
    if attempt >= _SLEEP_RETRIES:
        return None
    status = getattr(e, "status", None)
    status = status if _is_number(status) else getattr(e, "status_code", None)
    reason = getattr(e, "reason", None)
    asked = getattr(e, "retry_after", None)
    if _is_number(status) and status:
        if not (status in (408, 409, 429) or status >= 500 or reason in ("rate_limit", "overloaded")):
            return None
    elif not isinstance(e, (_ProviderUnreachable, ConnectionError) + _TIMEOUT_ERRORS):
        return None
    if _is_number(asked) and 0 <= asked <= 60:
        return float(asked)
    return min(8.0, 0.5 * 2 ** attempt) * (1 - 0.25 * random.random())


_SLEEP_CALL_SECONDS = 600


def _sleep_asker(caller: Any, model: str, max_tokens: int) -> Any:
    def ask(system: str, user: str) -> Any:
        attempt = 0
        while True:
            t0 = _time.monotonic()
            clock = _AskClock(_SLEEP_CALL_SECONDS)
            c = ModelCall("sleep", model, [system], user, max_tokens, signal=clock.signal)
            try:
                reply = _reply_shape((yield ("llm", caller, c)))
            except GeneratorExit:
                clock.cancel()
                raise
            except Exception as caught:
                timed_out = clock.why() is not None
                e = TimeoutError(f"the sleep's model call ran past {_SLEEP_CALL_SECONDS} s") if timed_out else caught
                wait = _sleep_retry_wait(e, attempt)
                if wait is None:
                    if timed_out:
                        raise e from None
                    raise
                attempt += 1
                yield ("wait", wait)
                continue
            finally:
                clock.close()
            u = reply["usage"] or {}
            return reply["text"], {
                "secs": round(_time.monotonic() - t0, 1),
                "in": _sleep_usage(u, "input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens")
                or _sleep_usage(u, "prompt_tokens"),
                "out": _sleep_usage(u, "output_tokens") or _sleep_usage(u, "completion_tokens"),
                "stop": reply["stop"],
            }
    return ask


def _sleep_ask_json(ask: Any, system: str, user: str, key: str, log: list) -> Any:
    for attempt in (1, 2):
        text, meta = yield from ask(system, user if attempt == 1 else user + _SLEEP_NOT_JSON)
        log.append({"call": key, "attempt": attempt, **meta})
        try:
            return _sleep_parse_json(text)
        except (ValueError, RecursionError):
            continue
    return None


def _sleep_ask_json_logged(ask: Any, system: str, user: str, key: str, log: list) -> Any:
    seen: list = []

    def keep(system: str, user: str) -> Any:
        text, meta = yield from ask(system, user)
        seen.append(text)
        return text, meta
    got = yield from _sleep_ask_json(keep, system, user, key, log)
    if got is None and seen:
        t = seen[-1]
        log.append({"call": key, "failed": True, "chars": len(t), "head": t[:300], "tail": t[-300:]})
    return got


def _sleep_read_flow(store: str, tz: Optional[float], since: Optional[str], sleep_id: Optional[str]) -> Any:
    body = _sleep_body(store, None, tz, None, None, None, None, _SLEEP_PAGE_TOKENS, None, None, sleep_id,
                       since_last_sleep=None if since is not None else True, since=since)
    rows: list = []
    head: Optional[dict] = None
    seen: set = set()
    while True:
        page = _with_records((yield ("post", "/api/v1/memory/sleep", body)), "memories")
        if head is None:
            head = page
        rows += page["memories"]
        nxt = page.get("next_cursor")
        if page.get("exhausted") or not nxt:
            return head, rows
        if _cursor_repeats(json.dumps(nxt, sort_keys=True, default=str), seen, page["memories"]):
            return head, rows
        if len(seen) > _MAX_PAGES:
            raise _truncated("the sleep's reading passed its page limit")
        body = {**body, "cursor": nxt}
        if isinstance(head.get("sleep_id"), str) and head["sleep_id"]:
            body["sleep_id"] = head["sleep_id"]


def _sleep_read_parts(run: _SleepRun, mems: list, core: str, titles: str) -> Any:
    parts = _sleep_parts(mems, run.s.part_tokens)
    digest = _sleep_text(_SLEEP_DIGEST, "digest", _sleep_many(mems))

    def read(k: int, part: list) -> Any:
        system = digest.format(being=run.being, k=k, n=len(parts))
        user = (f"<core>\n{_sleep_kept(core) or '(empty)'}\n</core>\n<boxes>\n{titles}\n</boxes>\n"
                f"<memories>\n{_sleep_lines(part)}\n</memories>")
        got = None
        for attempt in (1, 2, 3):
            key = f"read {k}" + ("" if attempt == 1 else f" again {attempt}")
            got = _sleep_renumbered((yield from _sleep_ask_json_logged(run.read, system, user, key, run.log)),
                                    part, run.log, k)
            if got is not None:
                break
        found, dropped = _sleep_checked_items(got, {m.eid for m in part}, {m.eid for m in part if m.mine})
        mixed, mixed_dropped = _sleep_checked_mixed(got, part)
        if mixed_dropped:
            run.log.append({"call": f"read {k}", "mixed_dropped": mixed_dropped})
        return found, dropped, mixed, (k if got is None else None)
    lists, dropped, mixed, unread = [], 0, [], []
    for found, d, mx, failed in (yield ("each", [read(k, p) for k, p in enumerate(parts, 1)], run.s.readers)):
        lists.append(found)
        dropped += d
        mixed += mx
        if failed:
            unread.append(failed)
    run.log.append({"call": "unread parts", "parts": unread})
    return parts, lists, dropped, mixed, unread


def _sleep_combine(run: _SleepRun, g: list, rounds: int, known_all: set, said_all: set) -> Any:
    flat = [it for part in g for it in part]
    if len(g) == 1 or not flat:
        return flat
    got = yield from _sleep_ask_json(run.merge, _SLEEP_COMBINE.format(being=run.being),
                                     json.dumps(flat, ensure_ascii=False, indent=0), f"combine {rounds}", run.log)
    joined, _ = _sleep_checked_items(got, known_all, said_all)
    return joined or flat


def _sleep_settle(run: _SleepRun, core: str, sec: str, text: str) -> Any:
    text = _sleep_as_lines(text)
    old = dict(_sleep_sections(core)).get(sec)
    if old is None:
        return text, "new"
    held = [ln.strip() for ln in old.split("\n") if ln.strip()]
    tonight = [ln.strip() for ln in text.split("\n") if ln.strip()]
    if sum(1 for ln in held if ln not in tonight) <= _SLEEP_OVERTURN:
        return text, "kept"
    numbered = "\n".join(f"{i}. {_sleep_kept(ln)}" for i, ln in enumerate(held, 1))
    g = yield from _sleep_ask_json(run.merge, _SLEEP_SETTLE.format(being=run.being, name=_sleep_named(sec)),
                                   f"<as it stood>\n{numbered}\n</as it stood>\n<tonight>\n{text}\n</tonight>",
                                   f"settle {sec}", run.log)
    if not isinstance(g, dict):
        return None, "unshaped"

    def as_line(x: Any) -> str:
        t = str(x).strip()
        return t if t.startswith("- ") else f"- {t.lstrip('-').strip()}"
    adds = [as_line(x) for x in (g.get("add") if isinstance(g.get("add"), list) else []) if str(x).strip()]
    op, line = g.get("op"), as_line(g.get("text") or "")
    try:
        n = int(g.get("line"))
    except (TypeError, ValueError):
        n = 0
    if op == "replace" and line != "- " and 1 <= n <= len(held):
        held[n - 1] = line
    elif op == "drop" and 1 <= n <= len(held):
        held.pop(n - 1)
    else:
        op = "none"
    return "\n".join(held + [x for x in adds if x not in held]), f"{op}, +{len(adds)}"


def _sleep_where(new: dict, k: int) -> str:
    return "core" if k == 0 else f'box "{_sleep_named(new[k]["title"])}"'


def _sleep_cap(s: SleepSettings, k: int) -> int:
    return s.core_tokens if k == 0 else s.box_tokens


def _sleep_put_back(new: dict, files: dict, k: int, carried: list) -> None:
    new[k] = dict(files[k])
    for src, dst, sec, text in carried:
        if dst == k and all(sec not in dict(_sleep_sections(new[j]["body"])) for j in (src, k)):
            new[src]["body"] = _sleep_set_section(new[src]["body"], sec, text, raw=True)


def _sleep_room(run: _SleepRun, new: dict, files: dict, k: int, used_now: int, cap: int, rooms: list,
                refused: list, carried: list, why: Optional[str] = None) -> Any:
    me = _SLEEP_ME
    where = _sleep_where(new, k)
    core_way = ", or move people who matter less to a box (say which, under \"moved\")" if k == 0 else ""
    moved = ', "moved": [{"section": "<heading>", "to": "1|2|3|4"}]' if k == 0 else ""
    g = (yield from _sleep_ask_json(run.merge, _SLEEP_ROOM.format(being=run.being, where=where, used=used_now, cap=cap,
                                                                  core_way=core_way, moved=moved),
                                    f"<{where}>\n{_sleep_kept(new[k]['body'])}\n</{where}>", f"room {k}", run.log)) or {}
    body = str(g.get("text") or "").strip()
    body = _sleep_join(_sleep_sections(body)) if body.startswith("## ") else ""
    had = dict(_sleep_sections(new[0]["body"]))
    moves = []
    if k == 0 and body:
        body = _sleep_join([(h, t) for h, t in _sleep_sections(body) if h in had])
    if k == 0 and body:
        mine = had.get(me)
        if mine is not None and dict(_sleep_sections(body)).get(me) != mine:
            body = _sleep_set_section(body, me, mine, raw=True)
        for mv in _sleep_iter(g.get("moved")):
            if isinstance(mv, dict) and str(mv.get("to")) in {"1", "2", "3", "4"}:
                sec = str(mv.get("section") or "").strip()
                held = had.get(sec)
                if held is not None and sec != me and sec not in dict(_sleep_sections(body)) \
                        and all(sec != s for _, s, _ in moves):
                    moves.append((int(mv["to"]), sec, held))
    boxes: dict = {}
    for to, sec, held in moves:
        boxes[to] = _sleep_set_section(boxes.get(to, new[to]["body"]), sec, held)
    if body and run.count(body) <= cap and all(run.count(b) <= run.s.box_tokens for b in boxes.values()):
        rooms.append({"file": k, "before": used_now, "after": run.count(body)})
        new[k]["body"] = body
        for to, b in boxes.items():
            new[to]["body"] = b
        carried.extend((k, to, sec, held) for to, sec, held in moves)
    else:
        rooms.append({"file": k, "before": used_now, "after": None})
        refused.append({"edit": {"file": k, "op": "room"}, "why": why or (
            f"no room: {used_now:,} of {cap:,} tokens, and the room made came back unshaped or still too big; "
            "the file stays as it was")})
        _sleep_put_back(new, files, k, carried)


def _sleep_boxes_fit(run: _SleepRun, new: dict, files: dict, refused: list, carried: list) -> None:
    for k in _SLEEP_BOXES:
        if run.count(new[k]["body"]) > run.s.box_tokens:
            refused.append({"edit": {"file": k, "op": "room"}, "why": "over its cap after a move; the box stays as it was"})
            _sleep_put_back(new, files, k, carried)


def _sleep_seed(run: _SleepRun, files: dict, sleep_id: str, day: str) -> Any:
    core = files[0]
    if not run.prompt.strip() or any(h == _SLEEP_ME for h, _ in _sleep_sections(core["body"])):
        return False
    body = _sleep_join([(_SLEEP_ME, _sleep_demote(run.prompt.strip()))] + _sleep_sections(core["body"]))
    if run.count(body) > run.s.core_tokens:
        fix = "raise core_tokens or shorten the prompt" if run.s.core_tokens < _SLEEP_CORE_MAX else "shorten the prompt"
        raise ValueError(f"the core with the prompt in it would be {run.count(body):,} tokens, over its "
                         f"{run.s.core_tokens:,}: {fix}")
    writes = [{"slot": 0, "base_version": core["version"], "title": "", "body": body, "evidence": [], "day": day}]
    r = yield ("write", "/api/v1/memory/self/write", {"user_id": run.store, "sleep_id": sleep_id, "writes": writes})
    files[0] = {"title": "", "body": body, "version": _sleep_written(r, writes)[0], "day": day}
    return True


_SLEEP_NO_ROOM = re.compile(r"slot (\d) \([^)]*\) would hold (\d+) tokens, over its cap of (\d+)")


def _sleep_code(e: BaseException) -> Optional[str]:
    d = getattr(e, "details", None)
    code = d.get("code") if isinstance(d, dict) else None
    return code if isinstance(code, str) else getattr(e, "type", None)


def _sleep_dedupe(new: dict, deduped: list, carried: list) -> None:
    in_core = {h for h, _ in _sleep_sections(new[0]["body"]) if h != _SLEEP_ME}
    for k in _SLEEP_BOXES:
        for h, _ in _sleep_sections(new[k]["body"]):
            if h in in_core:
                new[k]["body"], gone = _sleep_drop_section(new[k]["body"], h)
                deduped.append({"file": k, "section": h})
                carried.append((k, 0, h, gone))


def _sleep_write_flow(run: _SleepRun, sleep_id: str, files: dict, new: dict, ev: list, day: str, refused: list,
                      rooms: list, carried: list, deduped: list) -> Any:
    s = run.s
    made_room: set = set()
    while True:
        _sleep_dedupe(new, deduped, carried)
        writes =[{"slot": k, "base_version": files[k]["version"], "title": new[k]["title"], "body": new[k]["body"],
                   "evidence": ev, "day": day}
                  for k in _SLEEP_SLOTS if new[k]["body"] != files[k]["body"] or new[k]["title"] != files[k]["title"]]
        if not writes:
            return {}
        try:
            r = yield ("write", "/api/v1/memory/self/write", {"user_id": run.store, "sleep_id": sleep_id, "writes": writes})
        except BadRequestError as e:
            if _sleep_code(e) != "self_no_room":
                raise
            d = dict(e.details) if isinstance(e.details, dict) else {}
            said = _SLEEP_NO_ROOM.search(e.message if isinstance(e.message, str) else "")
            if said:
                for key, v in zip(("slot", "tokens", "cap"), said.groups()):
                    d.setdefault(key, int(v))
            by_slot = {w["slot"]: w for w in writes}

            def fill(w: dict) -> float:
                return run.count(w["body"]) / _sleep_cap(s, w["slot"])
            k = d.get("slot") if _is_int(d.get("slot")) and d.get("slot") in by_slot else max(writes, key=fill)["slot"]
            cap = _sleep_cap(s, k)
            if k in made_room:
                refused.append({"edit": {"file": k, "op": "room"},
                                "why": "no room: the service counts it over its cap; the file stays as it was"})
                _sleep_put_back(new, files, k, carried)
                continue
            made_room.add(k)
            used_now = run.count(new[k]["body"])
            told = d.get("tokens")
            limit = d["cap"] if _is_int(d.get("cap")) and d.get("slot") == k else cap
            counted = _is_int(told) and told > 0 and told > limit and d.get("slot") == k
            target = limit * used_now // told if counted else min(used_now, cap) * 9 // 10
            yield from _sleep_room(run, new, files, k, used_now, max(1, min(cap, target)), rooms, refused, carried,
                                   "no room: the service counts it over its cap, and the room made came back unshaped "
                                   "or still too big; the file stays as it was")
            _sleep_boxes_fit(run, new, files, refused, carried)
            continue
        return _sleep_written(r, writes)


def _sleep_done_flow(run: _SleepRun, sleep_id: str) -> Any:
    if not run.done:
        return None
    return (yield ("write", "/api/v1/memory/sleep/done", {"user_id": run.store, "sleep_id": sleep_id}))


def _sleep_titles(files: dict) -> str:
    return "\n".join(f"{k}: {_sleep_named(files[k]['title']) or '(empty)'}" for k in _SLEEP_BOXES)


def _sleep_flow(run: _SleepRun) -> Any:
    s, being, me, log, count = run.s, run.being, _SLEEP_ME, run.log, run.count
    t0 = _time.monotonic()
    assistant = run.assistant or _sleep_assistant((yield ("settings", run.store)))
    head, rows = yield from _sleep_read_flow(run.store, run.tz, run.since, run.sleep_id)
    sid = head.get("sleep_id")
    if not isinstance(sid, str) or not sid:
        raise WosError(200, "the reading came back without a sleep_id: nothing could be written for it")
    today = run.today or _sleep_today(head)
    now, day = _sleep_day(today), today[:10]
    view = _sleep_view((yield ("post", "/api/v1/memory/self", {"user_id": run.store})))
    files = {0: {"title": view[0]["title"], "body": _sleep_core(view[0].get("body") or ""), "version": view[0]["version"],
                 "day": view[0]["day"]}}
    for k in _SLEEP_BOXES:
        files[k] = {"title": "", "body": "", "version": 0, "day": None}
        if view[k]["version"]:
            got = _sleep_file((yield ("post", "/api/v1/memory/self/open", {"user_id": run.store, "slot": k})))
            files[k] = {"title": got["title"], "body": got.get("body") or "",
                        "version": got["version"] or view[k]["version"], "day": got["day"]}
    notes_version, notes = _sleep_notes((yield ("post", "/api/v1/memory/self/notes", {"user_id": run.store})))
    before = json.dumps(notes, sort_keys=True, ensure_ascii=False)
    seeded = (yield from _sleep_seed(run, files, sid, day)) if run.prompt else False
    mems = _sleep_memories(rows, being, assistant, run.line_time, run.given_count)
    core = files[0]["body"]
    titles = _sleep_titles(files)
    proposals: list = [p for p in notes["proposals"] if _sleep_proposal(p)]
    read_tokens = sum(m.tokens for m in mems)
    run.say(f"{len(mems)} memories, {read_tokens:,} tokens → {len(_sleep_parts(mems, s.part_tokens))} readers "
            f"({s.readers} at a time)")
    out: dict = {"sleep_id": sid, "sleep_version": SLEEP_VERSION, "today": today, "memories": len(mems),
                 "tokens": read_tokens, "seeded": seeded}
    if not mems:
        out.update(items=[], applied=[], refused=[], written={}, log=log, secs=0.0, llm_in=0, llm_out=0)
        out["done"] = yield from _sleep_done_flow(run, sid)
        return out

    known_all = {m.eid for m in mems}
    said_all = {m.eid for m in mems if m.mine}
    by_eid = {m.eid: m for m in mems}
    span_end = mems[-1].mid
    parts, lists, dropped, mixed, unread = yield from _sleep_read_parts(run, mems, core, titles)
    for x in mixed:
        x["sleep"] = today
    marked = {_sleep_mark_key(x) for x in notes["mixed"]}
    notes["mixed"].extend(x for x in mixed if _sleep_mark_key(x) not in marked)

    rounds = 0
    while len(lists) > s.group:
        rounds += 1
        groups = [lists[i:i + s.group] for i in range(0, len(lists), s.group)]
        lists = list((yield ("each", [_sleep_combine(run, g, rounds, known_all, said_all) for g in groups], s.readers)))
    items = [it for part in lists for it in part]

    def counted_here(p: dict) -> bool:
        return span_end != "" and p.get("span_end") == span_end

    ripe = [p for p in proposals if not p.get("applied") and p["sleeps"] >= _SLEEP_RIPE_SLEEPS
            and (now - p["first"]) / 86400 >= _SLEEP_RIPE_DAYS and not counted_here(p)]
    run.say(f"merging {len(items)} items ({rounds} combine rounds) · {len(proposals)} proposals ({len(ripe)} ripe)")
    new = {k: dict(f) for k, f in files.items()}
    applied, refused, used, merge_notes, folds, settled, deduped, tidied, rooms = [], [], set(), [], [], [], [], [], []
    touched: set = set()
    carried: list = []
    got: Any = {}
    unmerged = False
    if items or ripe:
        blocks = "\n".join(f"<box {k} name=\"{_sleep_named(files[k]['title']) or '(empty)'}\" size=\""
                           f"{count(files[k]['body']):,} of {s.box_tokens:,} tokens\">\n"
                           f"{_sleep_kept(files[k]['body']) or '(empty)'}\n</box {k}>" for k in _SLEEP_BOXES)

        def said(p: dict) -> str:
            return f"{_sleep_named(p['id'])}: {_sleep_shown(str(p['text']))}"
        prop_text = "\n".join(f"{said(p)} (seen in {p['sleeps']} sleeps since {_sleep_named(p['first_day'])})"
                              for p in proposals if not p.get("applied")) or "(none)"
        ripe_text = "\n".join(said(p) for p in ripe) or "(none)"
        system = _sleep_text(_SLEEP_MERGE, "merge", _sleep_many(mems)).format(
            being=being, me=me, today=today, core_used=count(core), core_cap=s.core_tokens, box_cap=s.box_tokens,
            self_rule=_SLEEP_SELF_RULE.format(me=me, being=being))
        user = (f"<core>\n{_sleep_kept(core) or '(empty)'}\n</core>\n{blocks}\n<proposals>\n{prop_text}\n</proposals>\n"
                f"<ripe>\n{ripe_text}\n</ripe>\n<items>\n{json.dumps(items, ensure_ascii=False, indent=0)}\n</items>")
        got = yield from _sleep_ask_json_logged(run.merge, system, user, "merge", log)
        if got is None:
            got = yield from _sleep_ask_json_logged(run.merge, system, user + _SLEEP_SHORTER, "merge shorter", log)
        unmerged = got is None
        got = got or {}
        merge_notes = [str(x) for x in got.get("notes", [])] if isinstance(got.get("notes"), list) else []
        slot_of = {"core": 0, "0": 0, "1": 1, "2": 2, "3": 3, "4": 4}
        ripe_ids = [p["id"] for p in ripe if isinstance(p["id"], str)]
        tonight = [str(it.get("topic") or "").strip().lower() for it in items if it.get("kind") == "person"]

        def about_tonight(name: str) -> bool:
            n = name.strip().lower()
            return any(_sleep_speaks_of(t, n) for t in tonight)
        own = {x.strip().lower() for x in (me, being, assistant)} - {""}

        def selfish(name: str) -> bool:
            return name.strip().lower() in own
        for e in got.get("edits", []) if isinstance(got.get("edits"), list) else []:
            why = None
            if not isinstance(e, dict):
                continue
            op, slot = e.get("op"), slot_of.get(str(e.get("file")))
            op = op if isinstance(op, str) else None
            ripe_id = e.get("ripe") if isinstance(e.get("ripe"), str) else None
            ev = _sleep_eids(e.get("evidence"))
            made_ripe = None
            if _sleep_heading_broken(e, op):
                why = "a heading or a title holds a line break or a control character"
            elif slot is None or op not in {"rewrite", "remake", "move_section", "title"}:
                why = "unknown file or op"
            elif op in {"rewrite", "remake"} and (not ev or any(x not in known_all for x in ev)) \
                    and not (op == "rewrite" and slot == 0 and ripe_id in ripe_ids):
                why = "no evidence, or evidence not in this sleep"
            elif op == "rewrite":
                sec = _sleep_heading(str(e.get("section") or ""))
                sec = me if slot == 0 and selfish(sec) else sec
                if not sec or not str(e.get("text") or "").strip():
                    why = "rewrite needs a section and text"
                elif slot == 0 and sec == me and ripe_id not in ripe_ids:
                    why = f'"{me}" changes only from a ripe proposal'
                elif slot == 0 and sec != me and not about_tonight(sec):
                    why = "nothing new about this person tonight"
                elif slot != 0 and _SLEEP_DATED.search(sec):
                    why = "a box section is a matter, not its state on a date: rewrite the matter's section"
                else:
                    text = str(e["text"])
                    if slot == 0 and sec != me:
                        text, how = yield from _sleep_settle(run, core, sec, text)
                        settled.append({"section": sec, "how": how})
                    if text is None:
                        why = "one night overturns one line of a person; the sorting came back unshaped"
                    else:
                        new[slot]["body"] = _sleep_set_section(new[slot]["body"], sec, text, raw=(slot == 0 and sec == me))
                        touched.add((slot, sec))
                        if slot == 0 and sec == me:
                            made_ripe = ripe_id
            elif op == "remake":
                body = str(e.get("text") or "").strip()
                title = str(e.get("title") or "").strip()
                if slot == 0:
                    why = "the core is rewritten by sections, not remade"
                elif not body or not title:
                    why = "remake needs a title and the whole file"
                elif _SLEEP_DATED.search(title) or any(_SLEEP_DATED.search(h) for h, _ in _sleep_sections(body)):
                    why = "a box is a matter, not its state on a date: remake it under the matter's name"
                else:
                    new[slot]["body"] = body if body.startswith("## ") else _sleep_set_section("", "Notes", body)
                    new[slot]["title"] = title[:80]
                    here: dict = {}
                    for src, dst, moved_sec, moved_text in carried:
                        if dst == slot:
                            here[moved_sec] = moved_text
                        elif src == slot:
                            here.pop(moved_sec, None)
                    for moved_sec, moved_text in here.items():
                        if moved_sec not in dict(_sleep_sections(new[slot]["body"])):
                            new[slot]["body"] = _sleep_set_section(new[slot]["body"], moved_sec, moved_text)
                    touched.update((slot, h) for h, _ in _sleep_sections(new[slot]["body"]))
            elif op == "move_section":
                to = slot_of.get(str(e.get("to")))
                sec = _sleep_heading(str(e.get("section") or ""))
                if to is None or to == slot or (0 in (slot, to) and selfish(sec)):
                    why = "move needs another file, and the self section stays"
                else:
                    body, text = _sleep_drop_section(new[slot]["body"], sec)
                    if text is None:
                        why = "no such section"
                    elif sec in dict(_sleep_sections(new[to]["body"])):
                        why = "move needs a section the file does not hold"
                    else:
                        new[slot]["body"] = body
                        new[to]["body"] = _sleep_set_section(new[to]["body"], sec, text)
                        carried.append((slot, to, sec, text))
            elif op == "title":
                title = str(e.get("text") or "").strip()
                if slot == 0 or not title:
                    why = "the core has no title; a title needs text"
                elif _SLEEP_DATED.search(title):
                    why = "a box is a matter, not its state on a date: name it for the matter"
                else:
                    new[slot]["title"] = title[:80]
            if why:
                refused.append({"edit": e, "why": why})
            else:
                applied.append(e)
                used.update(ev)
                if made_ripe is not None:
                    for p in proposals:
                        if p["id"] == made_ripe:
                            p["applied"] = today
        _sleep_dedupe(new, deduped, carried)
        for k, h in sorted(touched):
            t = dict(_sleep_sections(new[k]["body"])).get(h)
            if not t or (k == 0 and h == me):
                continue
            where = _sleep_where(new, k)
            numbered = "\n".join(f"{i}: {_sleep_kept(ln)}"
                                 for i, ln in enumerate([ln for ln in t.split("\n") if ln.strip()], 1))
            g = (yield from _sleep_ask_json(run.merge, _SLEEP_TIDY.format(being=being, where=where),
                                            f"<section heading=\"{_sleep_named(h)}\">\n{numbered}\n</section>", f"tidy {k}:{h}",
                                            log)) or {}
            text = _sleep_doubt_lines_out(t, g.get("drop"), count)
            if text:
                tidied.append({"file": k, "section": h, "before": count(t), "after": count(text),
                               "before_text": t, "after_text": text})
                new[k]["body"] = _sleep_set_section(new[k]["body"], h, text)
        for k in _SLEEP_SLOTS:
            for h, t in _sleep_sections(new[k]["body"]):
                if k == 0 and h == me:
                    continue
                ls = [ln for ln in t.split("\n") if ln.strip()]
                long = sum(1 for ln in ls if count(ln) > _SLEEP_LINE_TOKENS)
                limit = _SLEEP_FOLD_LINES * 2 if k == 0 else _SLEEP_FOLD_LINES
                if len(ls) <= limit and not long:
                    continue
                why = ", ".join(x for x in (f"{len(ls)} lines" if len(ls) > limit else "",
                                            f"{long} lines longer than one idea" if long else "") if x)
                where = _sleep_where(new, k)
                g =(yield from _sleep_ask_json(run.merge, _SLEEP_FOLD_SECTION.format(being=being, where=where, why=why,
                                                                                      lines=limit),
                                                f"<section heading=\"{_sleep_named(h)}\">\n{_sleep_kept(t)}\n</section>",
                                                f"fold {k}:{h}", log)) or {}
                text = _sleep_as_lines(str(g.get("text") or ""))
                ok_lines = [ln for ln in text.split("\n") if ln.strip()]
                if ok_lines and len(ok_lines) <= limit and count(text) < count(t):
                    folds.append({"file": k, "section": h, "why": why, "before": count(t), "after": count(text)})
                    new[k]["body"] = _sleep_set_section(new[k]["body"], h, text)
                else:
                    refused.append({"edit": {"file": k, "op": "fold", "section": h},
                                    "why": f"fold came back unshaped or longer ({why})"})
        for k in _SLEEP_SLOTS:
            cap = _sleep_cap(s, k)
            used_now = count(new[k]["body"])
            if used_now <= cap:
                continue
            yield from _sleep_room(run, new, files, k, used_now, cap, rooms, refused, carried)
        _sleep_boxes_fit(run, new, files, refused, carried)
        seen_now: list = []
        for p in got.get("proposals", []) if isinstance(got.get("proposals"), list) else []:
            if not isinstance(p, dict) or not str(p.get("text") or "").strip():
                continue
            ev = _sleep_eids(p.get("evidence"))
            if not any(x in by_eid and by_eid[x].mine for x in ev):
                refused.append({"proposal": p, "why": "a view of the being rests on something it said"})
                continue
            text = str(p["text"]).strip()
            same = next((q for q in proposals if isinstance(q["id"], str) and q["id"] == p.get("same_as")), None)
            if same is None:
                same = next((q for q in proposals if counted_here(q) and q["text"] == text), None)
            if same and same["id"] not in seen_now:
                if not counted_here(same):
                    same.update(sleeps=same["sleeps"] + 1, last_day=today, text=text, span_end=span_end)
                seen_now.append(same["id"])
            elif not same:
                pid = _sleep_next_id(notes["proposals"])
                made = {"id": pid, "text": text, "sleeps": 1, "first": now, "first_day": today, "last_day": today,
                        "span_end": span_end}
                proposals.append(made)
                notes["proposals"].append(made)
                seen_now.append(pid)
    ev = sorted({by_eid[e].mid for e in used if e in by_eid})
    written = yield from _sleep_write_flow(run, sid, files, new, ev, day, refused, rooms, carried, deduped)
    _sleep_notes_fit(notes)
    if json.dumps(notes, sort_keys=True, ensure_ascii=False) != before:
        yield ("write", "/api/v1/memory/self/notes/write",
               {"user_id": run.store, "base_version": notes_version, "notes": notes})
    ended = None if unread or unmerged else (yield from _sleep_done_flow(run, sid))
    secs = round(_time.monotonic() - t0, 1)
    run.say(f"done in {secs}s · {len(items)} items · {len(applied)} edits, {len(refused)} refused · "
            f"{len(mixed)} who-said-what marks · written {sorted(written) or 'none'}")
    out.update({"readers": len(parts), "combine_rounds": rounds, "items": items, "items_dropped_by_check": dropped,
                "applied": applied, "refused": refused, "written": written, "proposals": proposals,
                "ripe": [p["id"] for p in ripe], "notes": merge_notes, "folds": folds, "settled": settled,
                "deduped": deduped, "tidied": tidied, "rooms": rooms, "mixed": mixed, "log": log, "secs": secs,
                "llm_in": sum(x.get("in") or 0 for x in log), "llm_out": sum(x.get("out") or 0 for x in log),
                "done": ended})
    return out


def _sleep_estimate_flow(run: _SleepRun, input_price: Optional[float], output_price: Optional[float]) -> Any:
    head, rows = yield from _sleep_read_flow(run.store, run.tz, run.since, run.sleep_id)
    view = _sleep_view((yield ("post", "/api/v1/memory/self", {"user_id": run.store})))
    mems = _sleep_memories(rows, run.being, None, "created_at", run.given_count)
    parts = _sleep_parts(mems, run.s.part_tokens)
    tokens = sum(m.tokens for m in mems)
    titles = _sleep_titles(view)
    frame = run.count(_SLEEP_DIGEST) + run.count(view[0].get("body") or "") + run.count(titles)
    sent = -(-(tokens + len(parts) * frame) * _SLEEP_IN_PER_100 // 100)
    written = -(-tokens * _SLEEP_OUT_PER_100 // 100)
    prices = _SLEEP_ESTIMATE_PRICES if input_price is None else (float(input_price), float(output_price))
    cost = round((sent * prices[0] + written * prices[1]) * (1 + _SLEEP_MARGIN) / 1_000_000, 6)
    sid = head.get("sleep_id")
    return {"sleep_id": sid if isinstance(sid, str) else None, "memories": len(mems), "tokens": tokens,
            "parts": len(parts), "readers": min(run.s.readers, len(parts)), "input_tokens": sent,
            "output_tokens": written, "cost": cost,
            "basis": {"sleep_version": SLEEP_VERSION, "model": _SLEEP_ESTIMATE_MODEL, "effort": "high",
                      "prompt_caching": False,
                      "prices": list(prices), "margin": _SLEEP_MARGIN},
            "sleepy": run.s.sleepy(tokens - _SLEEP_LINE_FRAME * len(mems))}


def _sleep_caller(c: Any, what: str, client: Any) -> tuple:
    caller = _as_caller(c, what)
    base = getattr(caller, "base_url", None)
    if base is not None:
        _provider_base(base, client._base)
    return caller, _check_model_name(_first_set(getattr(caller, "model", None), _SLEEP_MODEL), f"{what}.model")


def _required(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required.")
    return value.strip()


def _sleep_run(client: Any, user_id: Optional[str], *, being: Any, settings: Any, since: Any, sleep_id: Any,
               count_tokens: Any) -> _SleepRun:
    run = _SleepRun()
    run.store = client._uid(user_id)
    run.tz = client._tz
    run.being = _required(being, "being")
    run.assistant = None
    if settings is None:
        settings = SleepSettings()
    elif not isinstance(settings, SleepSettings):
        raise TypeError(f"settings must be SleepSettings, got {type(settings).__name__}.")
    run.s = settings
    run.since = None if since is None else _required(since, "since")
    run.sleep_id = None if sleep_id is None else _required(sleep_id, "sleep_id")
    if count_tokens is not None and not callable(count_tokens):
        raise TypeError("count_tokens must be a function of a text that returns its tokens.")

    def counted(text: str) -> int:
        n = count_tokens(text)
        if not _is_whole(n) or n < 0:
            raise ValueError(f"count_tokens must return a whole number of 0 or more, got {n!r}.")
        return int(n)
    run.given_count = counted if count_tokens is not None else None
    run.count = counted if count_tokens is not None else _sleep_tokens
    run.log = []
    run.prompt, run.today, run.done, run.line_time = "", None, True, "created_at"
    run.read = run.merge = None
    run.say = lambda msg: None
    return run


def _sleep_setup(client: Any, user_id: Optional[str], llm: Any, merge_llm: Any, being: Any, prompt: Any,
                 settings: Any, today: Any, since: Any, sleep_id: Any, done: Any, line_time: Any, count_tokens: Any,
                 max_tokens: Any, progress: Any, assistant: Any = None) -> _SleepRun:
    run = _sleep_run(client, user_id, being=being, settings=settings, since=since, sleep_id=sleep_id,
                     count_tokens=count_tokens)
    run.assistant = None if assistant is None else _required(assistant, "assistant")
    if not isinstance(prompt, str):
        raise TypeError(f"prompt must be a string, got {_jq(prompt)}.")
    if today is not None and (not isinstance(today, str) or _sleep_date(today[:10]) is None
                              or (len(today) > 10 and not re.fullmatch(r" \d{2}:\d{2}", today[10:]))
                              or _sleep_when(today) is None):
        raise ValueError(f'today must be "YYYY-MM-DD HH:MM" or "YYYY-MM-DD", the sleep\'s day, got {_jq(today)}.')
    _check_flag(done, "done")
    if line_time not in ("created_at", "event_date"):
        raise ValueError(f'line_time must be "created_at" or "event_date", got {_jq(line_time)}.')
    tokens = _check_whole(max_tokens, "max_tokens", 1, _MODEL_MAX_TOKENS)
    if progress is not None and not callable(progress):
        raise TypeError("progress must be a function of a line of text.")
    caller, model = _sleep_caller(llm, "llm", client)
    run.read = _sleep_asker(caller, model, tokens)
    if merge_llm is None:
        run.merge = run.read
    else:
        mcaller, mmodel = _sleep_caller(merge_llm, "merge_llm", client)
        run.merge = _sleep_asker(mcaller, mmodel, tokens)
    run.prompt, run.today, run.done, run.line_time = prompt, today, done, line_time
    run.say = progress if progress is not None else (lambda msg: None)
    return run


def _sleep_estimate_setup(client: Any, user_id: Optional[str], input_price: Any, output_price: Any, settings: Any,
                          since: Any, count_tokens: Any) -> _SleepRun:
    for name, v in (("input_price", input_price), ("output_price", output_price)):
        if v is not None and (not _is_number(v) or v < 0):
            raise ValueError(f"{name} must be a price in USD per million tokens, 0 or more, got {v!r}.")
    if (input_price is None) != (output_price is None):
        raise ValueError("Give input_price and output_price together, or neither for Claude Sonnet 5.5's.")
    return _sleep_run(client, user_id, being=_SLEEP_BEING, settings=settings, since=since, sleep_id=None,
                      count_tokens=count_tokens)


class _SleepHalt:

    __slots__ = ("lock", "event", "clocks")

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.event = threading.Event()
        self.clocks: set = set()

    def calling(self, clock: _AskClock) -> None:
        with self.lock:
            self.clocks.add(clock)
            if self.event.is_set():
                clock.cancel()

    def called(self, clock: _AskClock) -> None:
        with self.lock:
            self.clocks.discard(clock)

    def stop(self) -> None:
        with self.lock:
            self.event.set()
            for clock in self.clocks:
                clock.cancel()


def _sleep_drive_sync(client: Any, flow: Any, halt: Optional[_SleepHalt] = None) -> Any:
    value: Any = None
    error: Optional[BaseException] = None
    while True:
        try:
            need = flow.throw(error) if error is not None else flow.send(value)
        except StopIteration as stop:
            return stop.value
        value, error = None, None
        try:
            value = _sleep_do_sync(client, need, halt)
        except Exception as e:
            error = e
        except BaseException:
            flow.close()
            raise
        if halt is not None and halt.event.is_set():
            flow.close()
            return None


def _sleep_settled(aw: Any) -> Any:
    box: dict = {}

    def work() -> None:
        try:
            box["ok"] = asyncio.run(_awaited(aw))
        except BaseException as e:
            box["error"] = e
    t = threading.Thread(target=work, name="wontopos-sleep", daemon=True)
    t.start()
    t.join()
    if "error" in box:
        raise box["error"]
    return box.get("ok")


_SLEEP_GRACE = 0.1


def _sleep_raced(caller: Any, c: ModelCall) -> Any:
    clock = c.signal._clock
    box: dict = {}
    ended = threading.Event()

    def work() -> None:
        try:
            got = caller.call(c)
            box["ok"] = _sleep_settled(got) if inspect.isawaitable(got) else got
        except BaseException as e:
            box["error"] = e
        finally:
            ended.set()
    threading.Thread(target=work, name="wontopos-sleep", daemon=True).start()
    while not ended.wait(min(0.25, max(0.0, clock.left()))) and clock.why() is None:
        pass
    ended.wait(_SLEEP_GRACE)
    if "error" in box:
        raise box["error"]
    if "ok" in box:
        return box["ok"]
    raise TimeoutError("the call ran past its time")


async def _sleep_raced_async(aw: Any, clock: Any) -> Any:
    if _asyncio_loop() is not None:
        task = asyncio.ensure_future(aw)
        try:
            done, _ = await asyncio.wait({task}, timeout=max(0.0, clock.left()))
            if not done:
                done, _ = await asyncio.wait({task}, timeout=_SLEEP_GRACE)
        except BaseException:
            task.cancel()
            raise
        if done:
            return task.result()
        task.cancel()
    else:
        import anyio

        with anyio.move_on_after(max(0.0, clock.left()) + _SLEEP_GRACE):
            return await aw
    clock.expire()
    raise TimeoutError("the call ran past its time")


_SLEEP_SERVICE = ("post", "settings", "write")


def _sleep_service(client: Any, need: tuple) -> Any:
    return {"post": client._post, "settings": client.settings, "write": client._write}[need[0]](*need[1:])


def _sleep_do_sync(client: Any, need: tuple, halt: Optional[_SleepHalt] = None) -> Any:
    kind = need[0]
    if kind in _SLEEP_SERVICE:
        return _sleep_service(client, need)
    if kind == "llm":
        clock = need[2].signal._clock if halt is not None else None
        if clock is not None:
            halt.calling(clock)
        try:
            return _sleep_raced(need[1], need[2])
        finally:
            if clock is not None:
                halt.called(clock)
    if kind == "wait":
        if halt is None:
            _time.sleep(need[1])
        else:
            halt.event.wait(_time.pause(need[1]))
        return None
    flows, workers = need[1], need[2]
    if not flows:
        return []
    import concurrent.futures

    failed = threading.Event()
    running = _SleepHalt()

    def one(f: Any) -> Any:
        if failed.is_set() or running.event.is_set():
            return None
        try:
            return _sleep_drive_sync(client, f, running)
        except BaseException:
            failed.set()
            raise
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(workers, len(flows))),
                                               thread_name_prefix="wontopos-sleep") as pool:
        try:
            return list(pool.map(one, flows))
        except BaseException as e:
            if not isinstance(e, Exception):
                running.stop()
            raise


async def _sleep_drive_async(client: Any, flow: Any) -> Any:
    value: Any = None
    error: Optional[BaseException] = None
    while True:
        try:
            need = flow.throw(error) if error is not None else flow.send(value)
        except StopIteration as stop:
            return stop.value
        value, error = None, None
        try:
            value = await _sleep_do_async(client, need)
        except Exception as e:
            error = e
        except BaseException:
            flow.close()
            raise


async def _sleep_thread(fn: Any, *args: Any) -> Any:
    import functools

    loop = _asyncio_loop()
    if loop is not None:
        return await loop.run_in_executor(None, functools.partial(fn, *args))
    import anyio.to_thread

    return await anyio.to_thread.run_sync(functools.partial(fn, *args), **_anyio_leave())


async def _sleep_each_async(client: Any, flows: list, workers: int) -> list:
    out: list = [None] * len(flows)
    failed = False

    async def one(i: int, f: Any, gate: Any) -> None:
        nonlocal failed
        async with gate:
            if failed:
                out[i] = (True, None)
                return
            try:
                out[i] = (True, await _sleep_drive_async(client, f))
            except Exception as e:
                failed = True
                out[i] = (False, e)
    if _asyncio_loop() is not None:
        gate = asyncio.Semaphore(max(1, workers))
        await asyncio.gather(*(one(i, f, gate) for i, f in enumerate(flows)))
    else:
        import anyio

        limiter = anyio.CapacityLimiter(max(1, workers))
        async with anyio.create_task_group() as tg:
            for i, f in enumerate(flows):
                tg.start_soon(one, i, f, limiter)
    for ok, v in out:
        if not ok:
            raise v
    return [v for _, v in out]


async def _sleep_ask_async(caller: Any, c: ModelCall) -> Any:
    acall = getattr(caller, "_acall", None)
    if acall is not None:
        return await acall(c)
    if _is_async_callable(caller.call):
        return await caller.call(c)
    got = await _sleep_thread(caller.call, c)
    return await got if inspect.isawaitable(got) else got


async def _sleep_do_async(client: Any, need: tuple) -> Any:
    kind = need[0]
    if kind in _SLEEP_SERVICE:
        return await _sleep_service(client, need)
    if kind == "llm":
        return await _sleep_raced_async(_sleep_ask_async(need[1], need[2]), need[2].signal._clock)
    if kind == "wait":
        await _async_sleep(need[1])
        return None
    return await _sleep_each_async(client, need[1], need[2])
