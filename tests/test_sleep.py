import contextlib
import copy
import hashlib
import inspect
import json
import os
import pickle
import re
import signal
import sys
import threading
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos  # noqa: E402
from wontopos import ConflictError, ModelCallError, SleepSettings  # noqa: E402
from test_client import CLOCK, HAVE_HTTPX, KEY  # noqa: E402
from test_rooms_work import BEFORE_PHOTOS, tools_digest  # noqa: E402


async def done(x):
    return await x if inspect.isawaitable(x) else x


tok = wontopos.sleep._sleep_tokens


def digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


BEING = "the assistant"
ASSISTANT, PERSON = "Kai", "Dana"
PROMPT = "I am the assistant in these conversations with this user.\n## Style\nShort answers."
SETTINGS = dict(box_tokens=10_000, part_tokens=50_000, readers=2, group=2)
DAYS = ("2026-09-01", "2026-09-15", "2026-10-01", "2026-10-05")
FILLER = "the garden path and the morning light " * 46
BIG = "- " + "a long line of plant names " * 1700


def night_rows(n, day):
    rows = []
    for i in range(300 if n == 0 else 20):
        secs = 9 * 3600 + i * 20 + (i // 50) * 3600
        text = f"Night {n + 1} line {i}: " + (FILLER if n == 0 else "short words about the day")
        rows.append({"id": f"m{n + 1}-{i:04d}", "content": text,
                     "created_at": f"{day}T{secs // 3600:02d}:{secs % 3600 // 60:02d}:{secs % 60:02d}Z",
                     "speaker": PERSON if i % 2 == 0 else ASSISTANT, "tokens": tok(text)})
    return rows


def nights():
    return [(f"{day} 23:00", night_rows(n, day)) for n, day in enumerate(DAYS)]


SUMMARY_KEYS = ("seeded", "readers", "combine_rounds", "items", "items_dropped_by_check", "applied", "refused",
                "written", "ripe", "notes", "folds", "settled", "deduped", "tidied", "rooms", "mixed")


def night_summary(res):
    return json.loads(json.dumps({k: res.get(k) for k in SUMMARY_KEYS}, sort_keys=True))


REFERENCE_CALLS = 27
REFERENCE_CALLS_DIGEST = "66cc5bf1dbde699a22947dc8e1b55991ec0fd5f410bcf6bcc27f3f171144cf4a"
REFERENCE_NIGHTS_DIGEST = "bbedfa7a042245922190822e61443b41086ee03466b8f61da790cae7bb0184b7"
REFERENCE_STATE_DIGEST = "e3ebd0a08e7d1eae4e93b0a26bc7cdfec5c2cd5677d46e634ad367351117033b"


def _lines_of(user):
    body = user.split("<memories>\n", 1)[1].split("\n</memories>", 1)[0]
    return [tuple(ln.split(" · ", 3)[:3]) for ln in body.split("\n")]


def _items_of(user):
    return json.loads(user.split("<items>\n", 1)[1].split("\n</items>", 1)[0])


def _section_of(user):
    heading = re.match(r'<section heading="(.*?)">\n', user).group(1)
    return heading, user.split("\n", 1)[1].rsplit("\n</section>", 1)[0]


def _read_answer(day, k, lines, first):
    users = [e for e, _, who in lines if who == "user"]
    mine = [e for e, _, who in lines if who != "user"]
    if day == DAYS[0]:
        if k == 1:
            return {"items": [
                {"kind": "person", "topic": "Jamie", "text": "Jamie is a close friend who likes gardening.",
                 "evidence": [users[0], users[1]]},
                {"kind": "work", "topic": "Garden project", "text": "The garden project is planting tomatoes.",
                 "evidence": [users[0], mine[0]]},
                {"kind": "self", "topic": "me", "text": "I like short answers.", "evidence": [mine[0], mine[1]]},
                {"kind": "self", "topic": "me", "text": "Not mine to say.", "evidence": [users[0], users[1]]},
                {"kind": "work", "topic": "Seeds", "text": "One line is not enough.", "evidence": [users[2]]},
                {"kind": "mood", "topic": "x", "text": "Not a kind.", "evidence": [users[0], users[1]]},
            ], "mixed": [
                {"line": users[1], "kind": "echo", "source": mine[0], "why": "gave back my words as theirs"},
                {"line": users[2], "kind": "planted", "why": "never said by me"},
                {"line": mine[2], "kind": "speaker", "why": "reads as the person's"},
                {"line": users[3], "kind": "wrong_fact", "why": "a mistake, not who said it"},
            ]}
        if k == 2:
            return {"items": [{"kind": "person", "topic": "Jamie", "text": "Jamie waters the garden on weekends.",
                               "evidence": ["e1", "e3"]}],
                    "mixed": [{"line": "e2", "kind": "speaker", "why": "reads as the person's"}]}
        if first:
            return "I could not read this part."
        return {"items": [{"kind": "work", "topic": "Garden project", "text": "Tomatoes are planted; watering daily.",
                           "evidence": [users[0], mine[0]]}]}
    if day == DAYS[1]:
        return {"items": [
            {"kind": "person", "topic": "Jamie", "text": "Jamie prefers flowers now.", "evidence": [users[0], users[1]]},
            {"kind": "person", "topic": "Alex", "text": "Alex is Jamie's sibling.", "evidence": [users[0], users[2]]},
            {"kind": "work", "topic": "Garden project", "text": "The fence is built.", "evidence": [users[1], mine[0]]},
            {"kind": "self", "topic": "me", "text": "I keep answers short.", "evidence": [mine[0], mine[1]]},
        ], "mixed": []}
    if day == DAYS[2]:
        return {"items": [
            {"kind": "person", "topic": "Alex", "text": "Alex visits every month.", "evidence": [users[0], users[1]]},
            {"kind": "work", "topic": "Big list", "text": "A long list of plants.", "evidence": [users[1], users[2]]},
            {"kind": "self", "topic": "me", "text": "I still keep answers short.", "evidence": [mine[0], mine[1]]},
        ], "mixed": []}
    return {"items": [], "mixed": []}


def _combine_answer(flat):
    merged = {}
    for it in flat:
        key = (it["kind"], it["topic"])
        if key in merged:
            merged[key]["text"] = it["text"]
            merged[key]["evidence"] = sorted(set(merged[key]["evidence"]) | set(it["evidence"]), key=lambda e: int(e[1:]))
        else:
            merged[key] = {"kind": it["kind"], "topic": it["topic"], "text": it["text"], "evidence": list(it["evidence"])}
    return {"items": list(merged.values())}


def _merge_answer(day, user):
    items = _items_of(user)

    def ev(kind, topic):
        return next((it["evidence"] for it in items if it["kind"] == kind and it["topic"] == topic), [])
    if day == DAYS[0]:
        person, work, me = ev("person", "Jamie"), ev("work", "Garden project"), ev("self", "me")
        return {"edits": [
            {"file": "core", "op": "rewrite", "section": "Jamie", "evidence": person,
             "text": "- Jamie is a close friend.\n- Jamie likes gardening.\n- Jamie waters the garden on weekends."},
            {"file": "core", "op": "rewrite", "section": "Robin", "text": "Robin is new.", "evidence": person},
            {"file": "core", "op": "rewrite", "section": "Me", "text": "I am different now.", "evidence": person},
            {"file": "1", "op": "title", "text": "Garden"},
            {"file": "1", "op": "rewrite", "section": "Garden project", "evidence": work,
             "text": "- Planting tomatoes.\n- Watering daily.\n- Status: not yet confirmed.\n- Next: build a fence.\n"
                     "- Soil is clay.\n- Bought seeds."},
            {"file": "2", "op": "remake", "title": "Trip 2026-09-01", "text": "## Trip\n- Plan the route.", "evidence": work},
            {"file": "3", "op": "remake", "title": "Reading list", "text": "Books to read next.", "evidence": work},
            {"file": "core", "op": "move_section", "section": "Nobody", "to": "2"},
            {"file": "core", "op": "delete"},
            {"file": "1", "op": "rewrite", "section": "Other", "text": "x", "evidence": ["e99999"]},
        ], "proposals": [
            {"text": "I prefer short answers.", "same_as": None, "evidence": me},
            {"text": "From the person only.", "same_as": None, "evidence": person},
        ], "notes": ["1: what the garden needs"]}
    if day == DAYS[1]:
        return {"edits": [
            {"file": "core", "op": "rewrite", "section": "Jamie", "evidence": ev("person", "Jamie"),
             "text": "- Jamie is my oldest friend.\n- Jamie prefers flowers now.\n- Jamie waters on Sundays."},
            {"file": "core", "op": "rewrite", "section": "Alex", "text": "- Alex is Jamie's sibling.",
             "evidence": ev("person", "Alex")},
            {"file": "1", "op": "rewrite", "section": "Garden project", "text": "- Tomatoes planted.\n- Fence built.",
             "evidence": ev("work", "Garden project")},
        ], "proposals": [{"text": "I prefer short answers, always.", "same_as": "p1", "evidence": ev("self", "me")}],
            "notes": []}
    if day == DAYS[2]:
        return {"edits": [
            {"file": "core", "op": "move_section", "section": "Alex", "to": "3"},
            {"file": "4", "op": "rewrite", "section": "Big list", "text": BIG, "evidence": ev("work", "Big list")},
            {"file": "4", "op": "title", "text": "Big list"},
            {"file": "2", "op": "remake", "title": "People notes", "text": "## Jamie\n- Dup.\n## Neighbors\n- Kind people.",
             "evidence": ev("work", "Big list")},
            {"file": "core", "op": "rewrite", "section": "Jamie", "text": "- Changed.", "evidence": ev("person", "Alex")},
        ], "proposals": [{"text": "I prefer short answers, still.", "same_as": "p1", "evidence": ev("self", "me")}],
            "notes": []}
    if "Your last answer was too long" not in user:
        return '{"edits": [{"file": "core", "op": "rewrite", "text": "cut off'
    return {"edits": [{"file": "core", "op": "rewrite", "section": "Me", "ripe": "p1",
                       "text": "I am the assistant in these conversations with this user.\nI keep answers short."}],
            "proposals": [], "notes": []}


def scripted(system, user, first):
    if " you read part " in system:
        k = int(re.search(r"you read part (\d+) of", system).group(1))
        lines = _lines_of(user)
        return _read_answer(lines[0][1][:10], k, lines, first)
    if "Several readers each read one part" in system:
        return _combine_answer(json.loads(user))
    if ", finishing." in system:
        return _merge_answer(re.search(r"Today is (\d{4}-\d{2}-\d{2})", system).group(1), user)
    if "Tonight you rewrote how" in system:
        return {"add": ["Jamie prefers flowers now."], "op": "replace", "line": 2,
                "text": "Jamie likes flowers more than vegetables."}
    if "lines that are only a note of doubt" in system:
        rows = [ln.split(": ", 1) for ln in _section_of(user)[1].split("\n")]
        return {"drop": [int(n) for n, t in rows if "not yet confirmed" in t]}
    if "has grown:" in system:
        heading, text = _section_of(user)
        return {"text": text + " and more"} if heading == "Big list" else {"text": "\n".join(text.split("\n")[:3])}
    if "there is no room" in system:
        return {"text": "## Big list\n- Trimmed to what matters."}
    raise AssertionError("a call the script does not know: " + system[:80])


class Model:

    def __init__(self, fail=None):
        self.lock = threading.Lock()
        self.seen = {}
        self.calls = []
        self.texts = []
        self.fail = list(fail or [])

    def answer(self, system, user):
        with self.lock:
            key = hashlib.sha256((system + "\x00" + user).encode()).hexdigest()
            self.seen[key] = self.seen.get(key, 0) + 1
            first = self.seen[key] == 1
            self.calls.append(key)
            self.texts.append((system, user))
            if self.fail:
                raise self.fail.pop(0)
        out = scripted(system, user, first)
        return out if isinstance(out, str) else json.dumps(out)

    def __call__(self, c):
        assert c.purpose == "sleep" and len(c.system) == 1 and c.max_tokens == self.max_tokens, c
        text = self.answer(c.system[0], c.user)
        return {"text": text, "stop": "end", "usage": {"input_tokens": tok(c.user), "output_tokens": tok(text)}}

    max_tokens = 16_000

    def digest(self):
        return hashlib.sha256("\n".join(sorted(self.calls)).encode()).hexdigest()


class SettleModel(Model):

    def __init__(self, settle):
        super().__init__()
        self.settle = settle

    def answer(self, system, user):
        if "Tonight you rewrote how" in system:
            return self.settle
        return super().answer(system, user)


SETTLE_LINES = {"Infinity": "unshaped", "-Infinity": "unshaped", "NaN": "unshaped", "1e999": "unshaped",
                "-1e999": "unshaped", "1e300": "none, +0", "2.0": "drop, +0"}


class Service:
    CAPS = {0: 70_000, 1: 20_000, 2: 20_000, 3: 20_000, 4: 20_000}
    TAKES = {"sleep/done": {"user_id", "sleep_id"}, "self": {"user_id"}, "self/open": {"user_id", "slot", "version"},
             "self/history": {"user_id", "slot"}, "self/write": {"user_id", "sleep_id", "writes"},
             "self/notes": {"user_id"}, "self/notes/write": {"user_id", "base_version", "notes"}}
    WRITE_TAKES = {"slot", "base_version", "title", "body", "evidence", "day"}

    def __init__(self):
        self.rows, self.last, self.sleeps = [], 0, {}
        self.slots = {k: [] for k in range(5)}
        self.notes, self.notes_version = None, 0
        self.caps, self.scale, self.room_message = dict(self.CAPS), 1, True
        self.fail = []
        self.seen = []
        self.local_now = "2026-10-20T09:30:00+09:00"
        self.assistant = ASSISTANT

    def give(self, rows):
        self.rows.extend(rows)

    def count(self, text):
        return tok(text) * self.scale

    def latest(self, k):
        return self.slots[k][-1] if self.slots[k] else None

    def state(self):
        return json.loads(json.dumps({
            "slots": {str(k): [{x: v[x] for x in ("version", "title", "body", "evidence", "day")} for v in vs]
                      for k, vs in self.slots.items() if vs},
            "proposals": (self.notes or {}).get("proposals", []), "mixed": (self.notes or {}).get("mixed", [])}))

    @staticmethod
    def coded(status, code, message):
        return status, {"error": {"code": code, "message": message}}

    def handle(self, path, body, headers):
        self.seen.append({"path": path, "body": body, "headers": headers})
        for i, (fpath, status, payload) in enumerate(self.fail):
            if fpath == path:
                del self.fail[i]
                return status, payload
        route = path.split("?", 1)[0].replace("/api/v1/memory/", "")
        extra = set(body) - self.TAKES.get(route, set(body))
        if route == "self/write":
            extra |= {k for w in body.get("writes", []) for k in w if k not in self.WRITE_TAKES}
        if route == "self/notes/write":
            extra |= set(body.get("notes", {})) - {"proposals", "mixed"}
        if extra:
            return self.coded(400, "invalid_request", f"unknown fields {sorted(extra)}")
        return getattr(self, "on_" + route.replace("/", "_").replace("-", "_"))(body)

    def on_settings(self, b):
        return 200, {"user_id": b["user_id"], "time_zone": None, "utc_offset_minutes": None,
                     "default_speakers": {"user": PERSON, "assistant": self.assistant}, "speakers": [], "history": []}

    def on_sleep(self, b):
        sid = b.get("sleep_id")
        if sid is None:
            sid = f"s-{len(self.sleeps) + 1}"
            self.sleeps[sid] = {"since": self.last, "until": len(self.rows), "handed": set()}
        r = self.sleeps[sid]
        i = (b.get("cursor") or {}).get("after", r["since"])
        page, used = [], 0
        while i < r["until"]:
            t = self.rows[i]["tokens"]
            if page and used + t > b.get("budget_tokens", 5000):
                break
            page.append(self.rows[i])
            used += t
            i += 1
        r["handed"].update(x["id"] for x in page)
        exhausted = i >= r["until"]
        return 200, {"user_id": b["user_id"], "sleep_id": sid, "memories": page, "count": len(page), "tokens": used,
                     "exhausted": exhausted, "next_cursor": None if exhausted else {"after": i},
                     "clock": {"local_now": self.local_now}}

    def on_sleep_done(self, b):
        r = self.sleeps[b["sleep_id"]]
        changed = r["until"] > self.last
        self.last = max(self.last, r["until"])
        return 200, {"user_id": b["user_id"], "sleep_id": b["sleep_id"], "changed": changed,
                     "last_sleep": {"until": self.last}}

    def entry(self, k, v):
        if v is None:
            return {"slot": k, "title": "", "version": 0, "tokens": 0, "written_at": None, "day": None}
        return {"slot": k, "title": v["title"], "version": v["version"], "tokens": v["tokens"],
                "written_at": "2026-10-20T00:00:00Z", "day": v["day"]}

    def on_self(self, b):
        core = self.entry(0, self.latest(0))
        del core["slot"]
        core["body"] = self.latest(0)["body"] if self.latest(0) else ""
        return 200, {"user_id": b["user_id"], "core": core,
                     "files": [self.entry(k, self.latest(k)) for k in (1, 2, 3, 4)],
                     "notes_version": self.notes_version}

    def on_self_open(self, b):
        vs = self.slots[b["slot"]]
        if "version" in b:
            if not 1 <= b["version"] <= len(vs):
                return self.coded(404, "self_version_not_found", "no such version")
            v = vs[b["version"] - 1]
        else:
            v = vs[-1] if vs else None
        out = {"user_id": b["user_id"], **self.entry(b["slot"], v), "latest_version": len(vs),
               "body": v["body"] if v else "", "evidence": v["evidence"] if v else [],
               "sleep_id": v.get("sleep_id") if v else None}
        return 200, out

    def on_self_history(self, b):
        vs = self.slots[b["slot"]]
        return 200, {"user_id": b["user_id"], "slot": b["slot"], "latest_version": len(vs), "count": len(vs),
                     "versions": [{**self.entry(b["slot"], v), "evidence_count": len(v["evidence"]),
                                   "sleep_id": v.get("sleep_id")} for v in vs]}

    def on_self_write(self, b):
        r = self.sleeps.get(b["sleep_id"])
        if r is None:
            return self.coded(404, "sleep_not_found", "no sleep of this store has that sleep_id")
        for w in b["writes"]:
            if not set(w.get("evidence", [])) <= r["handed"]:
                return self.coded(400, "self_evidence_unknown", "evidence the sleep was not handed")
            have = self.latest(w["slot"])
            if (have["version"] if have else 0) != w["base_version"]:
                return self.coded(409, "self_version_conflict", "the file changed since it was read")
            n = self.count(w["body"])
            if n > self.caps[w["slot"]]:
                named = "slot 0 (the core)" if w["slot"] == 0 else f"slot {w['slot']} (file {w['slot']})"
                return self.coded(400, "self_no_room", (
                    f"{named} would hold {n} tokens, over its cap of {self.caps[w['slot']]}: there is no room. Make "
                    "room (fold, or move from the core to a file) and write again. Nothing was written."
                    if self.room_message else "there is no room"))
        out = []
        for w in b["writes"]:
            v = {"version": w["base_version"] + 1, "title": w.get("title", ""), "body": w["body"],
                 "tokens": self.count(w["body"]), "evidence": w.get("evidence", []), "day": w.get("day"),
                 "sleep_id": b["sleep_id"]}
            self.slots[w["slot"]].append(v)
            out.append({**self.entry(w["slot"], v), "evidence_count": len(v["evidence"]), "sleep_id": b["sleep_id"]})
        return 200, {"user_id": b["user_id"], "sleep_id": b["sleep_id"], "written": out}

    def on_self_notes(self, b):
        notes = self.notes or {"proposals": [], "mixed": []}
        return 200, {"user_id": b["user_id"], "version": self.notes_version, "written_at": None,
                     "bytes": len(json.dumps(notes)), "notes": notes}

    def on_self_notes_write(self, b):
        if b["base_version"] != self.notes_version:
            return self.coded(409, "self_notes_conflict", "the notes changed since they were read")
        self.notes = {"proposals": b["notes"].get("proposals", []), "mixed": b["notes"].get("mixed", [])}
        self.notes_version += 1
        return 200, {"user_id": b["user_id"], "version": self.notes_version, "written_at": "2026-10-20T00:00:00Z",
                     "bytes": len(json.dumps(self.notes))}

    def on_work_open(self, b):
        return 200, {"work_id": "w-1", "steps_left": 5, "deeper_left": 3,
                     "memories": [{"n": 1, "id": "id-1", "content": "Jamie likes flowers."}]}


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.answer(dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(self.path).query)))

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        self.answer(json.loads(self.rfile.read(n) or b"{}"))

    def answer(self, body):
        with self.server.lock:
            status, payload = self.server.service.handle(self.path, body, dict(self.headers))
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_):
        pass


def serve(service):
    srv = HTTPServer(("127.0.0.1", 0), _Handler)
    srv.service, srv.lock = service, threading.Lock()
    threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.002}, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_port}"


def paths(svc):
    return [x["path"].split("?", 1)[0].replace("/api/v1/memory/", "") for x in svc.seen]


PROMPT_DIGESTS = {
    "_SLEEP_DIGEST": "8cff7e06101f3cf1f69256323a5d40563e576d9ecd963016b35a57980ce62541",
    "_SLEEP_COMBINE": "dc3132527b20cdef6ce3af6bf7da1bc1d885aa74914230566c6212765ab4942b",
    "_SLEEP_MERGE": "7ccc820aa16ec2f16f36bf0b4afa57010ece0f36fc03fe6303d28a02a750ac2f",
    "_SLEEP_SHORTER": "1697835120c7f56cc71b2dc229987c4cf45d682eb3084480886f49bc23832730",
    "_SLEEP_SELF_RULE": "47b04387ca56c24adf5d4e31b6072b8a61bdf36d9880704428e7c509441b8ea1",
    "_SLEEP_SETTLE": "c20ba6effc265bd5612530f0f657e660a30eff85f28fd846b79ba5bed20163ca",
    "_SLEEP_FOLD_SECTION": "294ec09cd08d75e1778d567c7a3b1eb2dd4dd221f662334f51d8449260fe3881",
    "_SLEEP_TIDY": "dff0600c9f98c1cc5928f75dd0101e5928e31f06a0d6aadc1a2f553706e3c16c",
    "_SLEEP_ROOM": "41aff5a9ccb596e41031bd269a78dc4bc71aa5a9a47ec08f67ac8361c4926168",
    "_SLEEP_READER_NOTE": "0fa78c4e82cdb880ad70871895a61d7dcdc6505d5b1d1a0498c6c3c75669f028",
}
OPEN_BOX_DIGEST = "4fa83367c475682278e48a00a6b24fdd86a35240c36965001512e4219c1d64d8"


class SleepTextTest(unittest.TestCase):
    def test_the_prompts_are_the_reference_bytes(self):
        for name, want in PROMPT_DIGESTS.items():
            with self.subTest(name=name):
                self.assertEqual(hashlib.sha256(getattr(wontopos.sleep, name).encode()).hexdigest(), want)
        self.assertEqual(hashlib.sha256(json.dumps(wontopos.sleep._SLEEP_OPEN_BOX, sort_keys=True).encode()).hexdigest(),
                         OPEN_BOX_DIGEST)
        self.assertEqual(wontopos.sleep._SLEEP_NOT_JSON,
                         "\n\nYour last answer was not valid JSON. Answer with the JSON object only.")

    def test_the_settings_are_the_reference_ones(self):
        w = wontopos.sleep
        self.assertEqual((w._SLEEP_RIPE_SLEEPS, w._SLEEP_RIPE_DAYS, w._SLEEP_MIN_EVIDENCE, w._SLEEP_FOLD_LINES,
                          w._SLEEP_LINE_TOKENS, w._SLEEP_DOUBT_TOKENS, w._SLEEP_OVERTURN, w._SLEEP_MIN_PART,
                          w._SLEEP_MAX_READERS), (3, 28, 2, 4, 90, 25, 1, 50_000, 200))
        self.assertEqual(w._SLEEP_KINDS, {"self", "person", "work"})
        self.assertEqual(w._SLEEP_MIX_KINDS, {"echo", "planted", "speaker"})
        s = SleepSettings()
        self.assertEqual((s.core_tokens, s.box_tokens, s.part_tokens, s.readers, s.group, s.sleepy_tokens),
                         (70_000, 20_000, 100_000, 10, 10, 10_000_000))
        self.assertEqual((w._SLEEP_PAGE_TOKENS, w._SLEEP_LINE_FRAME, w._SLEEP_MAX_TOKENS), (20_000, 12, 16_000))

    def test_settings_are_checked_and_do_not_change(self):
        for kwargs, message in (
            (dict(core_tokens=9_999), "core_tokens must be 10,000-70,000 (got 9,999)"),
            (dict(core_tokens=70_001), "core_tokens must be 10,000-70,000 (got 70,001)"),
            (dict(box_tokens=20_001), "box_tokens must be 10,000-20,000 (got 20,001)"),
            (dict(part_tokens=49_999), "part_tokens must be 50,000 or more (got 49,999)"),
            (dict(readers=0), "readers must be 1-200 (got 0)"),
            (dict(readers=201), "readers must be 1-200 (got 201)"),
            (dict(group=1), "group must be 2-200 (got 1)"),
            (dict(sleepy_tokens=0), "sleepy_tokens must be 1 or more (got 0)"),
        ):
            with self.subTest(**kwargs):
                with self.assertRaises(ValueError) as cm:
                    SleepSettings(**kwargs)
                self.assertEqual(str(cm.exception), message)
        for bad in (True, 1.5, "50000", None):
            with self.assertRaises(TypeError):
                SleepSettings(part_tokens=bad)
        with self.assertRaises(TypeError):
            SleepSettings(50_000)
        s = SleepSettings(part_tokens=50_000, readers=200)
        with self.assertRaises(AttributeError):
            s.readers = 3
        self.assertEqual(pickle.loads(pickle.dumps(s)), s)
        self.assertEqual(copy.deepcopy(s), s)
        self.assertEqual(hash(s), hash(SleepSettings(part_tokens=50_000, readers=200)))
        self.assertIn("part_tokens=50000", repr(s))
        self.assertFalse(s.sleepy(9_999_999))
        self.assertTrue(s.sleepy(10_000_000))
        self.assertTrue(SleepSettings(sleepy_tokens=1).sleepy(1))
        with self.assertRaises(ValueError):
            s.sleepy(-1)

    def test_the_token_estimate_counts_digits_ascii_and_wide_characters_apart(self):
        self.assertEqual(wontopos.sleep._sleep_tokens(""), 0)
        self.assertEqual(wontopos.sleep._sleep_tokens("abcd"), 1)
        self.assertEqual(wontopos.sleep._sleep_tokens("abcde"), 2)
        self.assertEqual(wontopos.sleep._sleep_tokens("2024"), 2)
        self.assertEqual(wontopos.sleep._sleep_tokens("\u4eca\u65e5"), 3)
        self.assertEqual(wontopos.sleep._sleep_tokens("é"), 1)


class SleepCases:

    def sleep(self, mem, model=None, **kw):
        kw.setdefault("llm", model or Model())
        kw.setdefault("settings", SleepSettings(**SETTINGS))
        return done(mem.sleep("u", **kw))

    async def test_four_nights_are_the_reference_sleep_call_for_call(self):
        svc, mem = self.new()
        model = Model()
        got = []
        for today, rows in nights():
            svc.give(rows)
            res = await self.sleep(mem, model, today=today, prompt=PROMPT)
            got.append(res)
        self.assertEqual(len(model.calls), REFERENCE_CALLS)
        self.assertEqual(model.digest(), REFERENCE_CALLS_DIGEST, "the calls the model got")
        self.assertEqual(digest([night_summary(r) for r in got]), REFERENCE_NIGHTS_DIGEST, "what each night did")
        self.assertEqual(digest(svc.state()), REFERENCE_STATE_DIGEST, "the files and the notes")

        n1, n2, n3, n4 = got
        self.assertTrue(n1["seeded"])
        self.assertEqual((n1["readers"], n1["combine_rounds"]), (3, 1))
        self.assertTrue(svc.slots[0][0]["body"].startswith("## Me\nI am the assistant in these conversations"))
        self.assertIn("### Style", svc.slots[0][0]["body"])
        calls = [x["call"] for x in n1["log"] if "attempt" in x]
        self.assertIn("read 3 again 2", calls, "a part with no JSON is read again")
        self.assertTrue(any(x.get("renumbered") for x in n1["log"]), "a part numbered from e1 is put back")
        self.assertEqual(n1["items_dropped_by_check"], 3)
        self.assertEqual([m["kind"] for m in n1["mixed"]], ["echo", "speaker", "speaker"])
        self.assertTrue(all(m["sleep"] == "2026-09-01 23:00" for m in n1["mixed"]))
        whys = [r["why"] for r in n1["refused"]]
        for why in ("nothing new about this person tonight", '"Me" changes only from a ripe proposal',
                    "a box is a matter, not its state on a date: remake it under the matter's name", "no such section",
                    "unknown file or op", "no evidence, or evidence not in this sleep",
                    "a view of the being rests on something it said"):
            self.assertIn(why, whys)
        self.assertEqual([t["section"] for t in n1["tidied"]], ["Garden project"])
        self.assertEqual([f["section"] for f in n1["folds"]], ["Garden project"])
        self.assertEqual(svc.latest(1)["title"], "Garden")
        self.assertEqual(n2["settled"], [{"section": "Jamie", "how": "replace, +1"}, {"section": "Alex", "how": "new"}])
        self.assertEqual(n3["rooms"], [{"file": 4, "before": n3["rooms"][0]["before"], "after": 9}])
        self.assertEqual(n3["deduped"], [{"file": 2, "section": "Jamie"}])
        self.assertIn("## Alex", svc.latest(3)["body"])
        self.assertEqual([x["call"] for x in n4["log"] if x.get("failed")], ["merge"])
        self.assertIn("merge shorter", [x["call"] for x in n4["log"]])
        self.assertEqual(n4["ripe"], ["p1"])
        self.assertIn("I keep answers short.", svc.latest(0)["body"])
        self.assertEqual(svc.notes["proposals"][0]["applied"], "2026-10-05 23:00")
        self.assertEqual(svc.last, len(svc.rows))
        self.assertEqual(len(svc.sleeps), 4)

    async def test_a_settle_answer_with_a_number_past_what_every_client_reads_is_not_json(self):
        svc, mem = self.new()
        (day1, rows1), (day2, rows2) = nights()[:2]
        svc.give(rows1)
        await self.sleep(mem, Model(), today=day1, prompt=PROMPT)
        for line, how in SETTLE_LINES.items():
            with self.subTest(line=line):
                night2 = copy.deepcopy(svc)
                night2.give(rows2)
                _, mem2 = self.new(night2)
                res = await self.sleep(mem2, SettleModel('{"add": [], "op": "drop", "line": ' + line + '}'),
                                       today=day2, prompt=PROMPT)
                self.assertEqual(res["settled"][0], {"section": "Jamie", "how": how})
                tries = [x["attempt"] for x in res["log"] if x.get("call") == "settle Jamie"]
                self.assertEqual(tries, [1, 2] if how == "unshaped" else [1])

    async def test_a_span_run_again_after_its_sleep_done_failed_counts_each_proposal_and_each_mark_once(self):
        svc, mem = self.new()
        model = Model()
        expired = ("/api/v1/memory/sleep/done",
                   *Service.coded(410, "sleep_expired", "This sleep was read too long ago to finish: read again."))

        def counts():
            return [(p["id"], p["sleeps"], p["span_end"]) for p in svc.notes["proposals"]]

        (day1, rows1), (day2, rows2) = nights()[:2]
        svc.give(rows1)
        svc.fail = [expired]
        with self.assertRaises(wontopos.WosError) as cm:
            await self.sleep(mem, model, today=day1, prompt=PROMPT)
        self.assertEqual(cm.exception.status, 410)
        marks = len(svc.notes["mixed"])
        self.assertGreater(marks, 0)
        self.assertEqual(counts(), [("p1", 1, rows1[-1]["id"])])
        await self.sleep(mem, model, today=day1, prompt=PROMPT)
        self.assertEqual(counts(), [("p1", 1, rows1[-1]["id"])], "a proposal made by this span is not made again")
        self.assertEqual(len(svc.notes["mixed"]), marks, "nor a mark")
        svc.give(rows2)
        svc.fail = [expired]
        with self.assertRaises(wontopos.WosError) as cm:
            await self.sleep(mem, model, today=day2, prompt=PROMPT)
        self.assertEqual(cm.exception.status, 410)
        self.assertEqual(counts(), [("p1", 2, rows2[-1]["id"])])
        again = await self.sleep(mem, model, today=day2, prompt=PROMPT)
        self.assertEqual(counts(), [("p1", 2, rows2[-1]["id"])], "the span counted it once")
        self.assertTrue(again["done"]["changed"])
        self.assertEqual(svc.last, len(svc.rows))

    async def test_notes_from_before_span_end_still_read_and_their_proposals_count(self):
        svc, mem = self.new()
        svc.notes = {"proposals": [{"id": "p1", "text": "I prefer short answers.", "sleeps": 1, "first": 0,
                                    "first_day": "2026-09-01 23:00", "last_day": "2026-09-01 23:00"}], "mixed": []}
        day2, rows2 = nights()[1]
        svc.give(rows2)
        await self.sleep(mem, Model(), today=day2)
        self.assertEqual([(p["id"], p["sleeps"], p["span_end"]) for p in svc.notes["proposals"]],
                         [("p1", 2, rows2[-1]["id"])])

    async def test_one_night_asks_the_service_in_this_order(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        res = await self.sleep(mem, today="2026-09-15 23:00")
        self.assertEqual(paths(svc), ["settings", "sleep", "self", "self/notes", "self/write", "self/notes/write",
                                      "sleep/done"])
        self.assertEqual(svc.seen[0]["path"], "/api/v1/memory/settings?user_id=u")
        first = svc.seen[1]["body"]
        self.assertEqual(first, {"user_id": "u", "budget_tokens": 20_000, "since_last_sleep": True})
        write = svc.seen[4]["body"]
        self.assertEqual(write["sleep_id"], "s-1")
        self.assertEqual([w["slot"] for w in write["writes"]], [0, 1])
        self.assertEqual({w["base_version"] for w in write["writes"]}, {0})
        self.assertEqual({w["day"] for w in write["writes"]}, {"2026-09-15"})
        self.assertTrue(set(write["writes"][0]["evidence"]) <= {r["id"] for r in svc.rows})
        for x in svc.seen[4:]:
            self.assertIn("Idempotency-Key", x["headers"], x["path"])
        self.assertEqual(res["written"], {0: 1, 1: 1})
        self.assertEqual(res["done"]["changed"], True)
        self.assertEqual((res["sleep_id"], res["memories"], res["today"]), ("s-1", 20, "2026-09-15 23:00"))
        self.assertEqual(res["llm_in"], sum(x["in"] for x in res["log"] if "in" in x))
        self.assertEqual((res["sleep_version"], wontopos.SLEEP_VERSION, wontopos.sleep.SLEEP_VERSION), ("1.0.1",) * 3)
        self.assertGreater(res["llm_out"], 0)

    async def test_pages_are_read_to_the_end_and_today_comes_from_the_reading(self):
        svc, mem = self.new()
        svc.give(night_rows(0, DAYS[0])[:120])
        res = await self.sleep(mem, model=Model(), done=False)
        pages = [x["body"] for x in svc.seen if x["path"].endswith("/sleep")]
        self.assertGreater(len(pages), 2)
        self.assertNotIn("sleep_id", pages[0])
        for p in pages[1:]:
            self.assertEqual((p["sleep_id"], p["since_last_sleep"]), ("s-1", True))
        self.assertEqual(res["memories"], 120)
        self.assertEqual(res["today"], "2026-10-20 09:30", "the person's now from the reading's clock")
        self.assertIsNone(res["done"])
        self.assertNotIn("sleep/done", paths(svc))
        self.assertEqual(svc.last, 0, "not done: the next sleep reads this again")

    async def test_an_empty_span_seeds_the_self_and_ends_the_sleep_without_the_model(self):
        svc, mem = self.new()
        model = Model()
        res = await self.sleep(mem, model, prompt="I am Mira.", today="2026-10-20 08:00")
        self.assertEqual(model.calls, [])
        self.assertTrue(res["seeded"])
        self.assertEqual(res["memories"], 0)
        self.assertEqual(svc.latest(0)["body"], "## Me\nI am Mira.\n")
        self.assertEqual(svc.latest(0)["evidence"], [])
        self.assertEqual(paths(svc), ["settings", "sleep", "self", "self/notes", "self/write", "sleep/done"])
        res = await self.sleep(mem, model, prompt="I am Mira.", today="2026-10-21 08:00")
        self.assertFalse(res["seeded"], "the self is there: not seeded again")

    async def test_a_prompt_over_the_core_is_refused_before_the_model_is_called(self):
        svc, mem = self.new()
        model = Model()
        said = r"^the core with the prompt in it would be {} tokens, over its {}: {}$"
        with self.assertRaisesRegex(ValueError, said.format("10,229", "10,000", "raise core_tokens or shorten the prompt")):
            await self.sleep(mem, model, prompt="word " * 9_000, settings=SleepSettings(core_tokens=10_000,
                                                                                            part_tokens=50_000))
        with self.assertRaisesRegex(ValueError, said.format(r"7\d,\d{3}", "70,000", "shorten the prompt")):
            await self.sleep(mem, model, prompt="word " * 65_000, settings=SleepSettings(part_tokens=50_000))
        self.assertEqual(model.calls, [])
        self.assertNotIn("self/write", paths(svc))

    async def test_a_file_changed_meanwhile_raises_conflict_and_none_of_the_night_is_kept(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        svc.fail = [("/api/v1/memory/self/write", 409,
                     {"error": {"code": "self_version_conflict", "message": "the file changed since it was read"}})]
        with self.assertRaises(ConflictError) as cm:
            await self.sleep(mem, today="2026-09-15 23:00")
        self.assertEqual(cm.exception.details, {"code": "self_version_conflict"})
        self.assertEqual(paths(svc).count("self/write"), 1, "not tried again")
        self.assertNotIn("self/notes/write", paths(svc))
        self.assertNotIn("sleep/done", paths(svc))
        self.assertEqual(svc.state()["slots"], {})
        self.assertEqual(svc.last, 0, "the next sleep reads this span again")

    async def test_a_file_the_service_counts_over_its_cap_is_made_room_for_once_then_kept_as_it_was(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        svc.scale, svc.caps[1], svc.room_message = 2, 8, False
        model = Model()
        res = await self.sleep(mem, model, today="2026-09-15 23:00")
        self.assertEqual(paths(svc).count("self/write"), 3)
        writes = [x["body"]["writes"] for x in svc.seen if x["path"].endswith("/self/write")]
        self.assertEqual([[w["slot"] for w in ws] for ws in writes], [[0, 1], [0, 1], [0]])
        self.assertEqual(writes[1][1]["body"], "## Big list\n- Trimmed to what matters.\n")
        room = [s for s, u in model.texts if "there is no room" in s]
        self.assertEqual(len(room), 1)
        self.assertIn("left its box \"\" at 13 tokens, over its cap\nof 11: there is no room", room[0])
        self.assertEqual(res["rooms"], [{"file": 1, "before": 13, "after": 9}])
        self.assertEqual(res["refused"][-1], {"edit": {"file": 1, "op": "room"},
                                              "why": "no room: the service counts it over its cap; the file stays as it was"})
        self.assertEqual(res["written"], {0: 1})
        self.assertEqual(svc.slots[1], [])
        self.assertIn("sleep/done", paths(svc))

    async def test_when_the_service_names_the_file_and_its_count_the_room_is_made_against_them(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        svc.scale, svc.caps[1] = 2, 20
        model = Model()
        res = await self.sleep(mem, model, today="2026-09-15 23:00")
        room = [s for s, u in model.texts if "there is no room" in s]
        self.assertIn("at 13 tokens, over its cap\nof 10: there is no room", room[0])
        self.assertEqual(res["rooms"], [{"file": 1, "before": 13, "after": 9}])
        self.assertEqual(res["written"], {0: 1, 1: 1})
        self.assertEqual(svc.latest(1)["body"], "## Big list\n- Trimmed to what matters.\n")

    async def test_a_no_room_answer_that_counts_0_tokens_makes_room_against_the_clients_own_cap(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        svc.fail = [("/api/v1/memory/self/write", 400, {"error": {"code": "self_no_room", "message": "there is no room",
                                                                    "slot": 1, "tokens": 0, "cap": -1}})]
        model = Model()
        res = await self.sleep(mem, model, today="2026-09-15 23:00")
        room = [s for s, u in model.texts if "there is no room" in s]
        self.assertIn("at 13 tokens, over its cap\nof 11: there is no room", room[0])
        self.assertEqual(res["rooms"], [{"file": 1, "before": 13, "after": 9}])
        self.assertEqual(res["written"], {0: 1, 1: 1})

    async def test_another_refusal_ends_the_sleep_and_is_not_made_room_for(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        svc.fail = [("/api/v1/memory/self/write", 400, {"error": {"code": "self_evidence_unknown", "message": "no"}})]
        with self.assertRaises(wontopos.BadRequestError):
            await self.sleep(mem, today="2026-09-15 23:00")
        self.assertNotIn("sleep/done", paths(svc))

    async def test_a_rate_limited_call_is_tried_again_and_another_failure_ends_the_sleep(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        model = Model(fail=[ModelCallError(429, "slow down", retry_after=0), ModelCallError(529, "busy", retry_after=0)])
        res = await self.sleep(mem, model, today="2026-09-15 23:00")
        self.assertEqual(res["written"], {0: 1, 1: 1})
        self.assertEqual(model.calls[0], model.calls[1])
        self.assertEqual(model.calls[1], model.calls[2])

        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        model = Model(fail=[ModelCallError(400, "bad request")])
        with self.assertRaises(ModelCallError):
            await self.sleep(mem, model, today="2026-09-15 23:00")
        self.assertEqual(len(model.calls), 1)
        self.assertEqual(paths(svc), ["settings", "sleep", "self", "self/notes"])

        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        model = Model(fail=[ModelCallError(429, "slow down", retry_after=0)] * 3)
        with self.assertRaises(ModelCallError):
            await self.sleep(mem, model, today="2026-09-15 23:00")
        self.assertEqual(len(model.calls), 3, "tried twice more, then the sleep ends")

    async def test_a_reader_that_fails_ends_the_night_before_another_part_is_sent(self):
        svc, mem = self.new()
        svc.give(night_rows(0, DAYS[0]))
        model = Model(fail=[ModelCallError(400, "bad request")])
        with self.assertRaises(ModelCallError):
            await self.sleep(mem, model, today="2026-09-01 23:00", settings=SleepSettings(**{**SETTINGS, "readers": 1}))
        self.assertEqual(len(model.calls), 1, "the parts behind the failed one are not sent")
        self.assertEqual(paths(svc), ["settings", "sleep", "sleep", "sleep", "sleep", "sleep", "sleep", "sleep", "self",
                                      "self/notes"])

    async def test_each_model_call_carries_a_signal_that_stops_it_at_600_seconds(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        model = Model()
        signals = []

        def timed(c):
            signals.append((c.signal.aborted, c.signal.time_left()))
            return model(c)
        await self.sleep(mem, timed, today="2026-09-15 23:00")
        self.assertTrue(signals)
        for aborted, left in signals:
            self.assertFalse(aborted)
            self.assertTrue(590 < left <= 600, left)

        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        stopped = []

        def hangs(c):
            stopped.append(c.signal.wait(5))
            raise RuntimeError("stopped")
        with mock.patch.object(wontopos.sleep, "_SLEEP_CALL_SECONDS", 0.05):
            with self.assertRaisesRegex(TimeoutError, "the sleep's model call ran past 0.05 s"):
                await self.sleep(mem, hangs, today="2026-09-15 23:00")
        self.assertEqual(stopped, [True] * 3, "told to stop, and tried twice more")

    async def test_a_call_that_ignores_its_signal_is_raced_against_its_deadline(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        release = threading.Event()
        self.addCleanup(release.set)
        model = Model()
        calls = []

        def deaf(c):
            calls.append(c)
            release.wait(5)
            return model(c)
        t0 = CLOCK.monotonic()
        with mock.patch.object(wontopos.sleep, "_SLEEP_CALL_SECONDS", 0.05):
            with self.assertRaisesRegex(TimeoutError, "the sleep's model call ran past 0.05 s"):
                await self.sleep(mem, deaf, today="2026-09-15 23:00")
        self.assertLess(CLOCK.monotonic() - t0, 4)
        self.assertEqual(len(calls), 3, "raced at its deadline, and tried twice more")

    async def test_merge_llm_writes_and_an_async_caller_is_awaited(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        reader, writer = Model(), Model()

        async def write(c):
            return writer(c)
        res = await self.sleep(mem, reader, merge_llm=write, today="2026-09-15 23:00")
        self.assertEqual(len(reader.calls), 1)
        self.assertTrue(all(", finishing." in s or "Tonight" in s or "note of doubt" in s or "has grown" in s
                            for s, _ in writer.texts))
        self.assertEqual(res["written"], {0: 1, 1: 1})

    async def test_the_model_is_the_callers_own_else_sonnet(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        seen = []

        class Named:
            model = "some-model"

            def call(self, c):
                seen.append(c.model)
                return Model()(c)
        await self.sleep(mem, Named(), today="2026-09-15 23:00")
        self.assertEqual(set(seen), {"some-model"})
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        model = Model()

        def plain(c):
            seen.append(c.model)
            return model(c)
        await self.sleep(mem, plain, today="2026-09-15 23:00")
        self.assertEqual(seen[-1], "claude-sonnet-5-5")

    async def test_lines_are_dated_by_when_they_happened_when_asked(self):
        svc, mem = self.new()
        rows = night_rows(1, DAYS[1])
        for r in rows:
            r["event_date"] = "2023-05-21T00:00:00Z"
        svc.give(rows)
        model = Model()
        await self.sleep(mem, model, today="2026-09-15 23:00", line_time="event_date", done=False)
        read = next(u for s, u in model.texts if " you read part " in s)
        self.assertIn("e1 · 2023-05-21 00:00 · user · Night 2 line 0", read)
        await self.sleep(mem, model, today="2026-09-15 23:00")
        read = [u for s, u in model.texts if " you read part " in s][-1]
        self.assertIn("e2 · 2026-09-15 09:00 · the assistant · Night 2 line 1", read)

    async def test_who_said_a_line_is_told_by_its_speaker_against_the_assistants_name(self):
        svc, mem = self.new()
        said = [("Kai", "I booked the table."), (PERSON, "Thanks."), ("Milo", "Count me in."), (None, "A line."),
                ("me", "Mine too."), ("kai", "Not the same name.")]
        svc.give([{"id": f"w-{i}", "content": text, "created_at": f"2026-10-12T09:0{i}:00Z", "tokens": tok(text),
                   **({"speaker": who} if who else {})} for i, (who, text) in enumerate(said)])

        async def whos(**kw):
            model = Model()
            await self.sleep(mem, model, today="2026-10-12 23:00", done=False, **kw)
            return [w for _, _, w in _lines_of(next(u for s, u in model.texts if " you read part " in s))]
        self.assertEqual(await whos(), [BEING, PERSON, "Milo", "user", BEING, "kai"])
        self.assertEqual(paths(svc)[:2], ["settings", "sleep"])
        n = len(svc.seen)
        self.assertEqual(await whos(assistant=" Milo "), ["Kai", PERSON, BEING, "user", BEING, "kai"])
        self.assertEqual(paths(svc)[n], "sleep")
        self.assertNotIn("settings", paths(svc)[n:])
        svc, mem = self.new()
        svc.give([{"id": f"w-{i}", "content": text, "created_at": f"2026-10-12T09:0{i}:00Z", "tokens": tok(text),
                   **({"speaker": who} if who else {})} for i, (who, text) in enumerate(said) if who not in ("Milo", "kai")])
        self.assertEqual(await whos(), [BEING, "user", "user", BEING])

    async def test_a_night_of_one_person_reads_the_texts_as_written(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        model = Model()
        await self.sleep(mem, model, today="2026-09-15 23:00")
        read = [s for s, _ in model.texts if " you read part " in s]
        merge = [s for s, _ in model.texts if ", finishing." in s]
        self.assertEqual(read, [wontopos.sleep._SLEEP_DIGEST.format(being=BEING, k=1, n=1)])
        self.assertEqual(len(merge), 1)
        self.assertIn('The lines marked "user" are all one person', read[0])
        self.assertIn('the person the memories mark "user" is the one the assistant talks with', merge[0])
        for text in wontopos.sleep._SLEEP_MANY_PEOPLE.values():
            self.assertNotIn(text, read[0] + merge[0])
        self.assertEqual({w for _, _, w in _lines_of(next(u for s, u in model.texts if " you read part " in s))},
                         {"user", BEING})

    async def test_a_night_of_two_people_names_each_and_tells_them_apart(self):
        svc, mem = self.new()
        said = [(ASSISTANT, "I booked the table for Friday."), ("Dana", "I don't drink coffee."),
                ("Mina", "I work the night shift."), (None, "Noted, both of you."), ("Dana", "No coffee, ever."),
                ("Mina", "Nights are my workdays."), (ASSISTANT, "I keep the list short.")]
        svc.give([{"id": f"t-{i}", "content": text, "created_at": f"2026-10-12T09:0{i}:00Z", "tokens": tok(text),
                   **({"speaker": who} if who else {})} for i, (who, text) in enumerate(said)])
        texts = []

        def model(c):
            system, user = c.system[0], c.user
            texts.append((system, user))
            if " you read part " in system:
                out = {"items": [
                    {"kind": "person", "topic": "Dana", "text": "Dana drinks no coffee.", "evidence": ["e2", "e5"]},
                    {"kind": "person", "topic": "Mina", "text": "Mina works the night shift.", "evidence": ["e3", "e6"]},
                    {"kind": "self", "topic": "me", "text": "I keep lists short.", "evidence": ["e4", "e7"]},
                    {"kind": "self", "topic": "me", "text": "Not mine to say.", "evidence": ["e2", "e3"]},
                ], "mixed": [
                    {"line": "e5", "kind": "echo", "source": "e1", "why": "gave back my words"},
                    {"line": "e6", "kind": "planted", "why": "a person's own line is not planted"},
                ]}
            elif ", finishing." in system:
                items = _items_of(user)

                def ev(topic):
                    return next(it["evidence"] for it in items if it["topic"] == topic)
                out = {"edits": [
                    {"file": "core", "op": "rewrite", "section": "Dana", "text": "- Dana drinks no coffee.",
                     "evidence": ev("Dana")},
                    {"file": "core", "op": "rewrite", "section": "Mina", "text": "- Mina works the night shift.",
                     "evidence": ev("Mina")},
                ], "proposals": [{"text": "I keep lists short.", "same_as": None, "evidence": ["e4"]},
                                 {"text": "Said by people.", "same_as": None, "evidence": ["e2", "e3"]}],
                    "notes": []}
            elif "lines that are only a note of doubt" in system:
                out = {"drop": []}
            else:
                raise AssertionError("a call this night does not make: " + system[:80])
            text = json.dumps(out)
            return {"text": text, "stop": "end", "usage": {"input_tokens": tok(user), "output_tokens": tok(text)}}
        res = await self.sleep(mem, model, today="2026-10-12 23:00")

        read = [(s, u) for s, u in texts if " you read part " in s]
        merge = [s for s, _ in texts if ", finishing." in s]
        self.assertEqual((len(read), len(merge)), (1, 1))
        self.assertEqual([w for _, _, w in _lines_of(read[0][1])], [BEING, "Dana", "Mina", "user", "Dana", "Mina", BEING])
        many_digest = ("Each line from a person is marked with that person's name, and different names are different\n"
                       "  people: each name is its own topic, and what one of them said, wants or is never goes under "
                       "another.\n  A line marked \"user\" names no one: give it to no named person.")
        many_merge = ("and under one name: the memories mark each person by name, so each person has a section under "
                      "their\n   own name, and what one of them said, wants or is never goes into another's.")
        self.assertEqual(wontopos.sleep._SLEEP_MANY_PEOPLE, {"digest": many_digest, "merge": many_merge})
        one_digest = ('The lines marked "user" are all one person, the one\n  the assistant talks with: once their '
                      'name is known, that is their topic, never "the user" beside it.')
        self.assertEqual(read[0][0], wontopos.sleep._SLEEP_DIGEST.format(being=BEING, k=1, n=1)
                         .replace(one_digest, many_digest))
        self.assertIn('A line marked "user" names no one', read[0][0])
        self.assertIn(many_merge, merge[0])
        self.assertNotIn('the memories mark "user"', merge[0])
        self.assertEqual([(it["topic"], it["evidence"]) for it in res["items"]],
                         [("Dana", ["e2", "e5"]), ("Mina", ["e3", "e6"]), ("me", ["e4", "e7"])])
        self.assertEqual(res["items_dropped_by_check"], 1)
        self.assertEqual([(m["kind"], m["memory"], m["source_memory"]) for m in res["mixed"]], [("echo", "t-4", "t-0")])
        body = svc.latest(0)["body"]
        self.assertIn("## Dana\n- Dana drinks no coffee.", body)
        self.assertIn("## Mina\n- Mina works the night shift.", body)
        self.assertEqual(sorted(svc.latest(0)["evidence"]), ["t-1", "t-2", "t-4", "t-5"])
        self.assertEqual([p["text"] for p in res["proposals"]], [])
        for p in ({"text": "I keep lists short.", "same_as": None, "evidence": ["e4"]},
                  {"text": "Said by people.", "same_as": None, "evidence": ["e2", "e3"]}):
            self.assertIn({"proposal": p, "why": "a view of the being rests on something it said"}, res["refused"])

    def test_the_one_person_sentence_is_found_once_or_the_night_stops(self):
        w = wontopos.sleep
        self.assertIs(w._sleep_text(w._SLEEP_DIGEST, "digest", False), w._SLEEP_DIGEST)
        self.assertEqual(w._sleep_text(w._SLEEP_MERGE, "merge", True).count(w._SLEEP_MANY_PEOPLE["merge"]), 1)
        for which, text in (("digest", w._SLEEP_MERGE), ("merge", w._SLEEP_DIGEST),
                            ("digest", w._SLEEP_DIGEST + w._SLEEP_DIGEST)):
            with self.subTest(which=which), self.assertRaisesRegex(RuntimeError, "one-person sentence"):
                w._sleep_text(text, which, True)

    async def test_a_store_that_names_no_assistant_is_refused_before_a_line_is_read(self):
        svc, mem = self.new()
        svc.assistant = None
        svc.give(night_rows(1, DAYS[1]))
        model = Model()
        with self.assertRaisesRegex(ValueError, r"names no assistant\. Set default_speakers\.assistant .*"
                                                r"or pass sleep\(assistant="):
            await self.sleep(mem, model, today="2026-09-15 23:00")
        self.assertEqual(paths(svc), ["settings"])
        self.assertEqual((model.calls, svc.sleeps), ([], {}))
        res = await self.sleep(mem, model, today="2026-09-15 23:00", assistant=ASSISTANT)
        self.assertEqual(res["written"], {0: 1, 1: 1})
        self.assertEqual(paths(svc)[1], "sleep", "with the option, the store is not asked")

    async def test_the_estimate_reads_counts_and_prices_and_the_sleep_reads_on_under_its_sleep(self):
        svc, mem = self.new()
        rows = night_rows(1, DAYS[1])
        svc.give(rows)
        est = await done(mem.sleep_estimate("u", input_price=1.0, output_price=5.0,
                                            settings=SleepSettings(part_tokens=50_000)))
        tokens = sum(r["tokens"] + 12 for r in rows)
        frame = tok(wontopos.sleep._SLEEP_DIGEST) + tok("") + tok("1: (empty)\n2: (empty)\n3: (empty)\n4: (empty)")
        sent, written = -(-(tokens + frame) * 172 // 100), -(-tokens * 7 // 100)
        basis = {"sleep_version": "1.0.1", "model": "claude-sonnet-5-5", "effort": "high", "prompt_caching": False,
                 "margin": 0.15}
        self.assertEqual(est, {"sleep_id": "s-1", "memories": 20, "tokens": tokens, "parts": 1, "readers": 1,
                               "input_tokens": sent, "output_tokens": written,
                               "cost": round((sent * 1.0 + written * 5.0) * 1.15 / 1_000_000, 6),
                               "basis": {**basis, "prices": [1.0, 5.0]}, "sleepy": False})
        self.assertEqual(paths(svc), ["sleep", "self"])
        nightly = await self.sleep(mem, today="2026-09-15 23:00", sleep_id=est["sleep_id"])
        self.assertEqual(nightly["sleep_id"], "s-1")
        self.assertEqual(paths(svc)[2:4], ["settings", "sleep"])
        self.assertEqual(svc.seen[3]["body"]["sleep_id"], "s-1")
        self.assertEqual(len(svc.sleeps), 1, "no second sleep opened")
        none = await done(mem.sleep_estimate("u", settings=SleepSettings(part_tokens=50_000, sleepy_tokens=1)))
        self.assertEqual((none["cost"], none["memories"]), (0.0, 0))
        svc.give(night_rows(2, DAYS[2]))
        sleepy = await done(mem.sleep_estimate("u", settings=SleepSettings(part_tokens=50_000, sleepy_tokens=1),
                                               count_tokens=len))
        self.assertTrue(sleepy["sleepy"])
        self.assertEqual(sleepy["tokens"], sum(len(r["content"]) + 12 for r in night_rows(2, DAYS[2])))
        self.assertEqual((sleepy["cost"], sleepy["basis"]["prices"]),
                         (round((sleepy["input_tokens"] * 2.0 + sleepy["output_tokens"] * 10.0) * 1.15 / 1_000_000, 6),
                          [2.0, 10.0]), "no prices: Claude Sonnet 5.5's, plus 15%")
        self.assertGreater(sleepy["cost"], 0)
        for bad in (-1, "1", float("nan")):
            with self.assertRaises(ValueError):
                await done(mem.sleep_estimate("u", input_price=bad, output_price=1))
        for one in ({"input_price": 1.0}, {"output_price": 5.0}):
            with self.assertRaises(ValueError):
                await done(mem.sleep_estimate("u", **one))

    async def test_arguments_are_refused_before_anything_is_sent(self):
        svc, mem = self.new()
        model = Model()
        with self.assertRaises(TypeError):
            await done(mem.sleep("u"))
        for kwargs, error in (
            (dict(llm=None), TypeError),
            (dict(llm=model, settings={"readers": 2}), TypeError),
            (dict(llm=model, today="yesterday"), ValueError),
            (dict(llm=model, today="2026-10-20T08:00"), ValueError),
            (dict(llm=model, done=1), ValueError),
            (dict(llm=model, line_time="said_at"), ValueError),
            (dict(llm=model, max_tokens=0), ValueError),
            (dict(llm=model, count_tokens=3), TypeError),
            (dict(llm=model, progress="print"), TypeError),
            (dict(llm=model, being=" "), ValueError),
            (dict(llm=model, assistant=" "), ValueError),
            (dict(llm=model, assistant=3), ValueError),
            (dict(llm=model, prompt=None), TypeError),
            (dict(llm=model, since=""), ValueError),
            (dict(llm=model, sleep_id=" "), ValueError),
            (dict(llm=wontopos.openai_caller(base_url="http://localhost:11434/v1", model="m"),
                  merge_llm=SimpleCaller("https://api.wontopos.com")), ValueError),
        ):
            with self.subTest(**{k: repr(v) for k, v in kwargs.items()}):
                with self.assertRaises(error):
                    await done(mem.sleep("u", **kwargs))
        for kwargs, message in (
            (dict(today="yesterday"), 'today must be "YYYY-MM-DD HH:MM" or "YYYY-MM-DD", the sleep\'s day, got "yesterday".'),
            (dict(done=1), "done must be true or false, got 1."),
            (dict(being=" "), "being is required."),
            (dict(prompt=None), "prompt must be a string, got null."),
        ):
            with self.subTest(**kwargs):
                with self.assertRaises((TypeError, ValueError)) as cm:
                    await done(mem.sleep("u", llm=model, **kwargs))
                self.assertEqual(str(cm.exception), message)
        self.assertEqual(svc.seen, [])
        self.assertEqual(model.calls, [])

    async def test_progress_hears_each_stage(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        heard = []
        await self.sleep(mem, today="2026-09-15 23:00", progress=heard.append)
        self.assertEqual(len(heard), 3)
        self.assertTrue(heard[0].startswith("20 memories, "))
        self.assertTrue(heard[1].startswith("merging 4 items (0 combine rounds)"))
        self.assertTrue(heard[2].startswith("done in "))

    async def test_sleep_read_since_the_last_sleep_or_an_instant_and_done(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        page = await done(mem.sleep_read("u", since_last_sleep=True, budget_tokens=500))
        self.assertEqual(svc.seen[-1]["body"], {"user_id": "u", "budget_tokens": 500, "since_last_sleep": True})
        await done(mem.sleep_read("u", since_last_sleep=True, sleep_id=page["sleep_id"], cursor=page["next_cursor"]))
        self.assertEqual(svc.seen[-1]["body"]["sleep_id"], "s-1")
        svc.on_sleep_x = None
        await done(mem.sleep_read("u", since=" 2026-09-01T00:00:00Z ", sleep_id="s-1"))
        self.assertEqual(svc.seen[-1]["body"]["since"], "2026-09-01T00:00:00Z")
        r = await done(mem.sleep_done(" s-1 ", "u"))
        self.assertEqual(svc.seen[-1]["body"], {"user_id": "u", "sleep_id": "s-1"})
        self.assertIn("Idempotency-Key", svc.seen[-1]["headers"])
        self.assertTrue(r["changed"])
        n = len(svc.seen)
        for kwargs, message in (
            (dict(since_last_sleep=True, since="2026-09-01T00:00:00Z"), "pass one of them"),
            (dict(since_last_sleep=True, days_ago=1), "not with days_ago, room or today_start"),
            (dict(since="2026-09-01T00:00:00Z", room="n1"), "not with days_ago, room or today_start"),
            (dict(since_last_sleep=True, today_start="2026-09-01T00:00:00Z"), "not with days_ago"),
            (dict(since_last_sleep="yes"), 'since_last_sleep must be true or false, got "yes".'),
            (dict(since=" "), "since is required"),
        ):
            with self.subTest(**kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    await done(mem.sleep_read("u", tz=9, **kwargs))
        with self.assertRaisesRegex(ValueError, "sleep_id is required"):
            await done(mem.sleep_done("", "u"))
        self.assertEqual(len(svc.seen), n)
        await done(mem.sleep_read("u", days_ago=1, since_last_sleep=False, tz=9))
        self.assertNotIn("since_last_sleep", svc.seen[-1]["body"])

    async def test_the_file_and_note_routes(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        sid = (await done(mem.sleep_read("u", since_last_sleep=True)))["sleep_id"]
        ids = [r["id"] for r in svc.rows[:2]]
        w = await done(mem.write_sleep_files(sid, [
            {"slot": 0, "base_version": 0, "title": "", "body": "## Me\nI am Mira.\n", "evidence": [], "day": "2026-10-20"},
            {"slot": 2, "base_version": 0, "title": "Garden", "body": "## Beds\n- Two.\n", "evidence": ids},
        ], "u"))
        self.assertEqual([(x["slot"], x["version"], x["evidence_count"]) for x in w["written"]], [(0, 1, 0), (2, 1, 2)])
        self.assertIn("Idempotency-Key", svc.seen[-1]["headers"])
        self.assertEqual(svc.seen[-1]["body"]["writes"][1], {"slot": 2, "base_version": 0, "title": "Garden",
                                                             "body": "## Beds\n- Two.\n", "evidence": ids})
        files = await done(mem.sleep_files("u"))
        self.assertEqual(files["core"]["body"], "## Me\nI am Mira.\n")
        self.assertEqual([(f["slot"], f["version"]) for f in files["files"]], [(1, 0), (2, 1), (3, 0), (4, 0)])
        self.assertNotIn("body", files["files"][1])
        await done(mem.write_sleep_files(sid, [{"slot": 2, "base_version": 1, "title": "Garden", "body": "## Beds\n- Three.\n"}], "u"))
        self.assertEqual((await done(mem.open_sleep_file(2, "u")))["body"], "## Beds\n- Three.\n")
        self.assertEqual((await done(mem.open_sleep_file(2, "u", version=1)))["body"], "## Beds\n- Two.\n")
        self.assertEqual(svc.seen[-1]["body"], {"user_id": "u", "slot": 2, "version": 1})
        hist = await done(mem.sleep_file_history(2, "u"))
        self.assertEqual([v["version"] for v in hist["versions"]], [1, 2])
        notes = await done(mem.sleep_notes("u"))
        self.assertEqual((notes["version"], notes["notes"]), (0, {"proposals": [], "mixed": []}))
        await done(mem.write_sleep_notes(0, {"proposals": [], "mixed": [{"memory": ids[0]}]}, "u"))
        self.assertEqual((await done(mem.sleep_notes("u")))["notes"]["mixed"], [{"memory": ids[0]}])
        with self.assertRaises(ConflictError):
            await done(mem.write_sleep_notes(0, {}, "u"))
        n = len(svc.seen)
        for writes, message in (
            ([], "writes must be a list of 1 to 5"),
            ({"slot": 0}, "writes must be a list"),
            ([{"slot": 5, "base_version": 0, "body": ""}], r"writes\[0\].slot must be 0 \(the core\) or 1 to 4"),
            ([{"slot": True, "base_version": 0, "body": ""}], "slot must be 0"),
            ([{"slot": 1, "base_version": -1, "body": ""}], "base_version must be the version read"),
            ([{"slot": 1, "base_version": 0}], r"writes\[0\].body must be a str"),
            ([{"slot": 1, "base_version": 0, "body": "", "evidence": "id"}], "evidence must be a list of memory ids"),
            ([{"slot": 1, "base_version": 0, "body": "", "day": "today"}], 'day must be "YYYY-MM-DD"'),
            ([{"slot": 1, "base_version": 0, "body": "", "tittle": "x"}], 'did you mean "title"'),
            ([{"slot": 1, "base_version": 0, "body": ""}, {"slot": 1, "base_version": 0, "body": ""}], "written twice"),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    await done(mem.write_sleep_files(sid, writes, "u"))
        for slot in (-1, 5, "1", 1.0, None):
            with self.assertRaisesRegex(ValueError, "slot must be 0"):
                await done(mem.open_sleep_file(slot, "u"))
            with self.assertRaisesRegex(ValueError, "slot must be 0"):
                await done(mem.sleep_file_history(slot, "u"))
        with self.assertRaisesRegex(ValueError, "version must be a whole number"):
            await done(mem.open_sleep_file(1, "u", version=0))
        with self.assertRaisesRegex(ValueError, "base_version must be"):
            await done(mem.write_sleep_notes(-1, {}, "u"))
        with self.assertRaisesRegex(ValueError, "notes must be a dict"):
            await done(mem.write_sleep_notes(0, [], "u"))
        self.assertEqual(len(svc.seen), n)

    def files_service(self):
        svc = Service()
        svc.slots[0] = [{"version": 2, "title": "", "body": "## Jamie\n- Close friend.\n\n## Me\nI am the assistant.\n",
                         "tokens": 10, "evidence": [], "sleep_id": None, "day": "2026-10-01"}]
        svc.slots[1] = [{"version": 1, "title": "Garden", "body": "## Beds\n- Two beds of tomatoes.\n", "tokens": 9,
                         "evidence": [], "sleep_id": None, "day": "2026-09-28"}]
        svc.slots[3] = [{"version": 1, "title": "Reading list", "body": "  ", "tokens": 1, "evidence": [],
                         "day": "2026-10-05"}]
        return svc

    async def test_memory_tools_with_sleep_off_are_the_tools_without_it_to_the_byte(self):
        svc, mem = self.new(self.files_service(), model="scroll-2")
        for (speakers, dates), want in BEFORE_PHOTOS.items():
            with self.subTest(speakers=speakers, dates=dates):
                tools = mem.memory_tools("u", speakers=speakers, dates=dates, sleep=False)
                self.assertEqual(tools_digest(tools), want)
        tools = mem.memory_tools("u", preset="S-2")
        self.assertNotIn("open_box", [d["name"] for d in tools.definitions()])
        with self.assertRaisesRegex(ValueError, "not a memory tool"):
            await done(tools.run({"name": "open_box", "input": {"box": "1"}}))
        await done(tools.begin("q"))
        self.assertEqual(paths(svc), ["work/open"])

    async def test_memory_tools_with_sleep_carry_the_core_the_boxes_and_the_reader_note(self):
        svc, mem = self.new(self.files_service(), model="scroll-2")
        tools = mem.memory_tools("u", preset="S-2", sleep=True, today="2026-10-05")
        base = mem.memory_tools("u").note
        if self.is_async:
            with self.assertRaisesRegex(RuntimeError, "not read yet"):
                tools.note
            await tools.load()
        files = "\n".join([
            "<core last edited 2026-10-01, 4 days ago>",
            "## Me",
            "I am the assistant.",
            "",
            "## Jamie",
            "- Close friend.",
            "</core>",
            "<boxes>",
            "1. Garden (last edited 2026-09-28, 7 days ago)",
            "2. (empty)",
            "3. Reading list (last edited 2026-10-05, today)",
            "4. (empty)",
            "</boxes>",
        ])
        self.assertEqual(tools.note, base + "\n\n" + wontopos.sleep._SLEEP_READER_NOTE + "\n" + files)
        self.assertEqual(paths(svc), ["self"])
        defs = tools.definitions()
        self.assertEqual(defs[:-1], mem.memory_tools("u").definitions())
        self.assertEqual(defs[-1], wontopos.sleep._SLEEP_OPEN_BOX)
        self.assertEqual(tools.definitions("openai")[-1], {"type": "function", "function": {
            "name": "open_box", "description": wontopos.sleep._SLEEP_OPEN_BOX["description"],
            "parameters": wontopos.sleep._SLEEP_OPEN_BOX["input_schema"]}})
        defs[-1]["name"] = "changed"
        self.assertEqual(wontopos.sleep._SLEEP_OPEN_BOX["name"], "open_box", "a copy each time")

        opened = '<box 1 name="Garden" last edited 2026-09-28, 7 days ago>\n## Beds\n- Two beds of tomatoes.\n</box>'
        self.assertEqual(await done(tools.run({"name": "open_box", "input": {"box": "1"}})), opened)
        self.assertEqual(await done(tools.run({"name": "open_box", "arguments": '{"box": "garden"}'})), opened)
        self.assertEqual(svc.seen[-1]["body"], {"user_id": "u", "slot": 1})
        n = len(svc.seen)
        self.assertEqual(await done(tools.run({"name": "open_box", "input": {"box": "2"}})), "Box 2 is empty.")
        self.assertEqual(len(svc.seen), n, "an empty box asks nothing")
        self.assertEqual(await done(tools.run({"name": "open_box", "input": {"box": "Reading list"}})), "Box 3 is empty.")
        self.assertEqual(await done(tools.run({"name": "open_box", "input": {"box": "Recipes"}})),
                         "No box by that name. The boxes are:\n" + files.split("<boxes>\n")[1].split("\n</boxes>")[0])
        svc.fail = [("/api/v1/memory/self/open", 404, {"error": {"code": "self_version_not_found", "message": "gone"}})]
        self.assertTrue((await done(tools.run({"name": "open_box", "input": {"box": "1"}}))).startswith("Refused: [404]"))

        await done(tools.begin("q"))
        self.assertEqual(paths(svc).count("self"), 1)
        self.assertEqual(paths(svc)[-1], "work/open")
        svc.slots[0].append({"version": 3, "title": "", "body": "## Me\nI am the assistant, rested.\n", "tokens": 9,
                             "evidence": [], "sleep_id": None, "day": "2026-10-05"})
        note = await done(tools.load())
        self.assertIn("<core last edited 2026-10-05, today>\n## Me\nI am the assistant, rested.\n</core>", note)
        self.assertEqual(note, tools.note)

    async def test_memory_tools_with_sleep_read_the_files_at_the_first_turn_and_date_them_on_the_clock(self):
        svc, mem = self.new(self.files_service(), model="scroll-2")
        tools = mem.memory_tools("u", preset="S-2", sleep=True, tz=-12)
        await done(tools.begin("q"))
        self.assertEqual(paths(svc), ["self", "work/open"])
        self.assertIn("<core last edited 2026-10-01, ", tools.note)
        for kwargs, message in ((dict(sleep=1), "sleep must be true or false, got 1."),
                                (dict(sleep=True, today="2026-13-01"), "today must be a day"),
                                (dict(today="2026-10-01"), "it goes with sleep=True")):
            with self.assertRaisesRegex(ValueError, message):
                mem.memory_tools("u", **kwargs)
        plain = mem.memory_tools("u")
        with self.assertRaisesRegex(ValueError, "make the tools with sleep=True"):
            await done(plain.load())


class SimpleCaller:

    model = "m"

    def __init__(self, base_url):
        self.base_url = base_url

    def call(self, c):
        raise AssertionError("never called")


class SyncSleepTest(SleepCases, unittest.IsolatedAsyncioTestCase):
    is_async = False

    def new(self, svc=None, **kw):
        svc = svc or Service()
        srv, base = serve(svc)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return svc, wontopos.Client(KEY, base_url=base, **kw)


class AsyncSleepTest(SleepCases, unittest.IsolatedAsyncioTestCase):
    is_async = True

    def new(self, svc=None, **kw):
        if not HAVE_HTTPX:
            self.skipTest("httpx not installed (pip install 'wontopos[async]')")
        svc = svc or Service()
        srv, base = serve(svc)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        mem = wontopos.AsyncClient(KEY, base_url=base, **kw)
        self.addAsyncCleanup(mem.aclose)
        return svc, mem

    async def test_a_cancel_tells_every_running_reader_to_stop_with_asyncio_and_without_it(self):
        import anyio
        for with_asyncio in (True, False):
            with self.subTest(with_asyncio=with_asyncio):
                svc, mem = self.new()
                svc.give(night_rows(0, DAYS[0]))
                signals, lock, both = [], threading.Lock(), threading.Event()

                def slow(c):
                    with lock:
                        signals.append(c.signal)
                        if len(signals) == 2:
                            both.set()
                    c.signal.wait(3)
                    raise ModelCallError(400, "stopped")

                async def night():
                    await mem.sleep("u", llm=slow, settings=SleepSettings(**SETTINGS), today="2026-09-01 23:00")
                with contextlib.ExitStack() as stack:
                    if not with_asyncio:
                        for module in (wontopos, wontopos.sleep):
                            stack.enter_context(mock.patch.object(module, "_asyncio_loop", return_value=None))
                    t0 = CLOCK.monotonic()
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(night)
                        while not both.is_set() and CLOCK.monotonic() - t0 < 10:
                            await anyio.sleep(0.01)
                        tg.cancel_scope.cancel()
                    self.assertLess(CLOCK.monotonic() - t0, 2.5)
                self.assertEqual([s.aborted for s in signals], [True, True])


class SyncSleepCancelTest(unittest.TestCase):

    def test_ctrl_c_tells_every_running_reader_to_stop_and_starts_no_other(self):
        svc = Service()
        srv, base = serve(svc)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        mem = wontopos.Client(KEY, base_url=base)
        self.addCleanup(mem.close)
        svc.give(night_rows(0, DAYS[0]))
        self.addCleanup(signal.signal, signal.SIGINT, signal.signal(signal.SIGINT, signal.default_int_handler))
        signals, lock = [], threading.Lock()

        def slow(c):
            with lock:
                signals.append(c.signal)
                if len(signals) == 2:
                    signal.pthread_kill(threading.main_thread().ident, signal.SIGINT)
            c.signal.wait(3)
            raise ModelCallError(400, "stopped")
        t0 = CLOCK.monotonic()
        with self.assertRaises(KeyboardInterrupt):
            mem.sleep("u", llm=slow, settings=SleepSettings(**SETTINGS), today="2026-09-01 23:00")
        self.assertLess(CLOCK.monotonic() - t0, 2.5)
        self.assertEqual([s.aborted for s in signals], [True, True])



class SleepNotesReadTest(unittest.TestCase):

    def test_a_proposal_the_sleep_cannot_read_is_left_out_of_its_work_and_kept_in_its_notes_as_it_was(self):
        from wontopos.sleep import _sleep_notes, _sleep_proposal
        good = {"id": "p1", "text": "short answers", "sleeps": 2.0, "first": 1.5, "first_day": "2026-10-01"}
        bad = [{"id": "p2", "text": "x", "first_day": "d", "first": 1}, {"id": "p3", "text": "x", "sleeps": 1, "first": 1},
               {"id": "p4", "text": "x", "sleeps": "2", "first": 1, "first_day": "d"},
               {"id": "p5", "text": "x", "sleeps": 0, "first": 1, "first_day": "d"},
               {"id": "p6", "text": "x", "sleeps": 1, "first": True, "first_day": "d"}, "p7", None]
        version, notes = _sleep_notes({"version": 3, "notes": {"proposals": [good] + bad, "mixed": []}})
        self.assertEqual(version, 3)
        self.assertEqual(notes["proposals"], [dict(good, sleeps=2)] + bad)
        self.assertEqual([p for p in notes["proposals"] if _sleep_proposal(p)], [dict(good, sleeps=2)])
        self.assertIs(type(notes["proposals"][0]["sleeps"]), int)


NOTES_FIT = [
    ({"proposals": [{"id": "p1", "text": "a", "sleeps": 1, "first": 1, "first_day": "d", "applied": "d"}],
      "mixed": [{"memory": "m-1", "why": "w"}]}, ["p1"], ["m-1"]),
    ({"proposals": [
        {"id": "p1", "text": "a" * 60_000, "sleeps": 1, "first": 1, "first_day": "d", "applied": "d"},
        {"id": "p2", "text": "b" * 60_000, "sleeps": 1, "first": 1, "first_day": "d"},
        {"id": "p3", "text": "c" * 60_000, "sleeps": 1, "first": 1, "first_day": "d", "applied": "d"},
        {"id": "p4", "text": "d", "sleeps": 1, "first": 1, "first_day": "d", "applied": "d"},
        {"id": "p5", "text": "e" * 60_000, "applied": "d"}],
      "mixed": [{"memory": "m-1", "why": "x" * 20_000}, {"memory": "m-2", "why": "y"}]},
     ["p2", "p4", "p5"], ["m-1", "m-2"]),
    ({"proposals": [
        {"id": "p1", "text": "a" * 60_000, "sleeps": 1, "first": 1, "first_day": "d", "applied": "d"},
        {"id": "p2", "text": "b" * 150_000, "sleeps": 1, "first": 1, "first_day": "d"}],
      "mixed": [{"memory": "m-1", "why": "x" * 60_000}, {"memory": "m-2", "why": "y"}]},
     ["p2"], ["m-2"]),
    ({"proposals": [{"id": "p1", "text": "b" * 250_000, "sleeps": 1, "first": 1, "first_day": "d"}],
      "mixed": [{"memory": "m-1", "why": "y"}, {"memory": "m-2", "why": "z"}]},
     ["p1"], []),
]


class SleepNotesFitTest(unittest.TestCase):

    def test_over_their_cap_the_notes_drop_the_oldest_applied_proposals_then_the_oldest_marks_never_an_open_one(self):
        from wontopos.sleep import _sleep_notes_fit
        for notes, proposals, marks in NOTES_FIT:
            notes = json.loads(json.dumps(notes))
            _sleep_notes_fit(notes)
            self.assertEqual(([p["id"] for p in notes["proposals"]], [m["memory"] for m in notes["mixed"]]),
                             (proposals, marks))


class SleepBoxPickTest(unittest.TestCase):

    def test_a_box_is_picked_by_its_number_or_whole_name_before_part_of_one_and_never_by_a_blank(self):
        from wontopos.sleep import _sleep_box_pick
        view = {0: {"title": ""}, 1: {"title": "Notes on work"}, 2: {"title": "Work"}, 3: {"title": ""},
                4: {"title": "WORK"}}
        for which, want in (("", None), ("   ", None), (None, None), ("Work", 2), ("work", 2), ("WORK", 4),
                            ("notes", 1), ("on wo", 1), ("2", 2), (" 4 ", 4), ("3", 3), (3, 3), ("5", None),
                            ("Recipes", None)):
            self.assertEqual(_sleep_box_pick(view, which), want, repr(which))


class SleepFencedJsonTest(unittest.TestCase):

    def test_the_fenced_json_is_found_in_one_pass_with_the_same_result_as_before(self):
        from wontopos.sleep import _sleep_parse_json
        for text, want in (('Here: ```json\n{"a": {"b": 2}}\n``` done', {"a": {"b": 2}}),
                           ('```\n{"a": 1}\n``` and ```json {"b": 2} ```', None),
                           ('````{"a": 1}```', {"a": 1}),
                           ('```json\n{"a": 1}\n``` then {"b": 2}', {"a": 1}),
                           ('```{"a": 1} no fence after it, {"b": 2}', None),
                           ('x {"a": "}```"} y', {"a": "}```"})):
            if want is None:
                with self.assertRaises(ValueError, msg=text):
                    _sleep_parse_json(text)
            else:
                self.assertEqual(_sleep_parse_json(text), want, text)
        t0 = CLOCK.monotonic()
        with self.assertRaises(ValueError):
            _sleep_parse_json("```{}x" * 20_000)
        self.assertLess(CLOCK.monotonic() - t0, 1)


if __name__ == "__main__":
    unittest.main()
