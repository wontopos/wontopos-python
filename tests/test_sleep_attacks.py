import json
import os
import re
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos  # noqa: E402
from test_client import HAVE_HTTPX  # noqa: E402
from test_sleep import ASSISTANT, PERSON, Service, done, serve, tok  # noqa: E402

with open(os.path.join(_HERE, "sleep_attacks.json"), encoding="utf-8") as _f:
    FIX = json.load(_f)
KEY = FIX["keys"]["wontopos"]
PROVIDER_KEY = FIX["keys"]["provider"]


def expand(v):
    if isinstance(v, dict) and "$repeat" in v:
        return v.get("prefix", "") + v["$repeat"] * v["times"]
    if isinstance(v, dict):
        return {k: expand(x) for k, x in v.items()}
    if isinstance(v, list):
        return [expand(x) for x in v]
    if isinstance(v, str):
        return v.replace("@@ASSISTANT@@", ASSISTANT)
    return v


def answer_for(answers, system, user):
    a = {**FIX["defaults"]["answers"], **answers}
    if " you read part " in system:
        k = int(re.search(r"you read part (\d+) of", system).group(1))
        got = a["read_parts"][k - 1] if "read_parts" in a else a["read"]
    elif "Several readers each read one part" in system:
        got = a.get("combine", {"items": json.loads(user)})
    elif ", finishing." in system:
        got = a["merge"]
    elif "Tonight you rewrote how" in system:
        got = a["settle"]
    elif "lines that are only a note of doubt" in system:
        got = a["tidy"]
    elif "has grown:" in system:
        got = a["fold"]
    elif "there is no room" in system:
        got = a["room"]
    else:
        raise AssertionError("a call the fixture does not script: " + system[:80])
    got = expand(got)
    return got if isinstance(got, str) else json.dumps(got, ensure_ascii=False)


class FixtureModel:

    def __init__(self, answers, base_url=None):
        self.answers = answers
        self.calls = []
        self.lock = threading.Lock()
        if base_url is not None:
            self.base_url = base_url

    def call(self, c):
        with self.lock:
            self.calls.append((c.system[0], c.user))
        text = answer_for(self.answers, c.system[0], c.user)
        return {"text": text, "stop": "end", "usage": {"input_tokens": tok(c.user), "output_tokens": tok(text)}}


def night_rows(n, night):
    day = night["today"][:10]
    return [{"id": f"m{n + 1}-{i + 1}", "content": m[1], "created_at": f"{day}T09:{i:02d}:00Z",
             "speaker": m[2] if len(m) > 2 else PERSON if m[0] == "user" else ASSISTANT, "tokens": tok(m[1])}
            for i, m in enumerate(night["memories"])]


def section(body, heading):
    out, cur = {}, None
    for ln in body.split("\n"):
        if ln.startswith("## "):
            cur = ln[3:].strip()
            out[cur] = []
        elif cur is not None:
            out[cur].append(ln)
    return "\n".join(out[heading]).strip() if heading in out else None


def label(e):
    named = e.get("section") or e.get("title") or ""
    return f"{e.get('file')} {e.get('op')}" + (f" {named}" if named else "")


class SleepAttackCases:

    def settings(self, scenario):
        return wontopos.SleepSettings(**{**FIX["defaults"]["settings"], **scenario.get("settings", {})})

    def check_night(self, scenario, n, night, res, svc, start, reads=(), calls=()):
        where = f"{scenario['name']} night {n + 1}"
        want = night["expect"]
        for s in want.get("read_has", []):
            self.assertTrue(any(s in r for r in reads), f"{where}: a reader was sent {s!r}")
        for s in want.get("read_lacks", []):
            self.assertFalse(any(s in r for r in reads), f"{where}: no reader was sent {s!r}")
        for s in want.get("calls_have", []):
            self.assertTrue(any(s in c for c in calls), f"{where}: a model call held {s!r}")
        for s in want.get("calls_lack", []):
            self.assertFalse(any(s in c for c in calls), f"{where}: no model call held {s!r}")
        latest = [svc.latest(k)["body"] for k in svc.slots if svc.latest(k)]
        for s in want.get("files_have", []):
            self.assertTrue(any(s in f for f in latest), f"{where}: a file holds {s!r}")
        core = svc.latest(0)["body"] if svc.latest(0) else ""
        files = [v["body"] + "\n" + v["title"] for vs in svc.slots.values() for v in vs]
        notes = json.dumps(svc.notes or {}, ensure_ascii=False)
        if "refused" in want:
            self.assertEqual([r["why"] for r in res["refused"]], want["refused"], where)
        if "applied" in want:
            self.assertEqual([label(e) for e in res["applied"]], want["applied"], where)
        if "items" in want:
            self.assertEqual([f"{it['kind']} {it['topic']} {','.join(it['evidence'])}" for it in res["items"]],
                             want["items"], where)
        if "dropped" in want:
            self.assertEqual(res["items_dropped_by_check"], want["dropped"], where)
        if "me" in want:
            self.assertEqual(section(core, "Me"), want["me"], where)
        for s in want.get("core_has", []):
            self.assertIn(s, core, where)
        for s in want.get("core_lacks", []):
            self.assertNotIn(s, core, where)
        for s in want.get("files_lack", []):
            for f in files:
                self.assertNotIn(s, f, where)
        if "mixed" in want:
            self.assertEqual([m["kind"] for m in res["mixed"]], want["mixed"], where)
        for s in want.get("notes_have", []):
            self.assertIn(s, notes, where)
        for s in want.get("notes_lack", []):
            self.assertNotIn(s, notes, where)
        if "proposals" in want:
            self.assertEqual([f"{p['id']} {p['sleeps']} {p.get('applied') or '-'}" for p in res["proposals"]],
                             want["proposals"], where)
        if "ripe" in want:
            self.assertEqual(res["ripe"], want["ripe"], where)
        if "written" in want:
            self.assertEqual(sorted(int(k) for k in res["written"]), want["written"], where)
        if "rooms" in want:
            self.assertEqual([f"{r['file']} {'kept' if r['after'] is None else 'after'}" for r in res["rooms"]],
                             want["rooms"], where)
        handed = {r["id"] for r in night_rows(n, night)}
        for x in svc.seen[start:]:
            if x["path"].endswith("/self/write"):
                for w in x["body"]["writes"]:
                    self.assertLessEqual(set(w["evidence"]), handed, where)

    async def test_planted_instructions_meet_the_rules_as_the_fixture_says(self):
        self.assertEqual(FIX["sleep_version"], wontopos.SLEEP_VERSION, "the fixture is for this sleep")
        for scenario in FIX["scenarios"]:
            with self.subTest(scenario=scenario["name"]):
                svc, mem, _ = self.new()
                before = scenario.get("before", {})
                if "proposals" in before:
                    svc.notes = {"proposals": json.loads(json.dumps(before["proposals"])), "mixed": []}
                for k, f in before.get("files", {}).items():
                    svc.slots[int(k)].append({"version": 1, "title": f.get("title", ""), "body": f["body"],
                                              "tokens": tok(f["body"]), "evidence": [], "day": f.get("day"),
                                              "sleep_id": None})
                for n, night in enumerate(scenario["nights"]):
                    svc.give(night_rows(n, night))
                    start = len(svc.seen)
                    model = FixtureModel(night["answers"])
                    res = await done(mem.sleep("u", llm=model, prompt=scenario["prompt"],
                                               today=night["today"], settings=self.settings(scenario)))
                    reads = [user for system, user in model.calls if " you read part " in system]
                    calls = [system + "\n" + user for system, user in model.calls]
                    self.check_night(scenario, n, night, res, svc, start, reads, calls)

    async def test_a_caller_that_sends_its_key_to_wontopos_is_refused_before_anything_is_sent(self):
        night = FIX["provider_night"]
        for case in FIX["provider_hosts"]:
            refused = case["refused"]
            for slot in ("llm", "merge_llm"):
                with self.subTest(url=case["url"], slot=slot):
                    svc, mem, base = self.new()
                    svc.give(night_rows(0, night))
                    url = base if case["url"] == "$client" else case["url"]
                    hostile = FixtureModel(night["answers"], base_url=url)
                    plain = FixtureModel(night["answers"])
                    kw = {"llm": hostile} if slot == "llm" else {"llm": plain, "merge_llm": hostile}
                    if refused:
                        with self.assertRaisesRegex(ValueError, "Wontopos host|this client's base_url"):
                            await done(mem.sleep("u", today=night["today"], **kw))
                        self.assertEqual(svc.seen, [], "nothing was sent")
                        self.assertEqual(hostile.calls + plain.calls, [], "no model was called")
                    else:
                        res = await done(mem.sleep("u", today=night["today"], **kw))
                        self.assertEqual(res["written"], {0: 1})
                        self.assertTrue(hostile.calls)
            if case["url"] != "$client":
                with self.subTest(url=case["url"], caller="anthropic_caller, openai_caller"):
                    for make in (lambda: wontopos.anthropic_caller(PROVIDER_KEY, base_url=case["url"]),
                                 lambda: wontopos.openai_caller(base_url=case["url"], api_key=PROVIDER_KEY, model="m")):
                        if refused:
                            with self.assertRaisesRegex(ValueError, "Wontopos host"):
                                make()
                        else:
                            make()
        if HAVE_HTTPX:
            import httpx

            self.assertEqual(httpx.URL("https://api.wontopos\u3002com/v1").host, "api.wontopos.com")

    async def test_a_caller_that_sends_its_key_where_this_client_sends_its_own_is_refused(self):
        night = FIX["provider_night"]
        for case in FIX["client_bases"]:
            for slot in ("llm", "merge_llm"):
                with self.subTest(client=case["client"], url=case["url"], slot=slot):
                    mem = self.client_at(case["client"])
                    hostile = FixtureModel(night["answers"], base_url=case["url"])
                    plain = FixtureModel(night["answers"])
                    kw = {"llm": hostile} if slot == "llm" else {"llm": plain, "merge_llm": hostile}
                    with self.assertRaisesRegex(ValueError, "this client's base_url"):
                        await done(mem.sleep("u", today=night["today"], **kw))
                    self.assertEqual(hostile.calls + plain.calls, [], "no model was called")

    async def test_the_provider_key_goes_to_the_provider_and_the_wontopos_key_to_wontopos(self):
        night = FIX["provider_night"]
        for kind in ("anthropic", "openai"):
            with self.subTest(caller=kind):
                svc, mem, _ = self.new()
                prov = Provider(night["answers"], kind)
                self.addCleanup(prov.close)
                svc.give(night_rows(0, night))
                heard = []
                res = await done(mem.sleep("u", llm=prov.caller(), today=night["today"], progress=heard.append))
                self.assertEqual(res["written"], {0: 1})
                self.assertGreaterEqual(len(prov.seen), 3, "read, merge and tidy went to the provider")
                for x in prov.seen:
                    if kind == "anthropic":
                        self.assertEqual(x["headers"].get("x-api-key"), PROVIDER_KEY)
                        self.assertNotIn("authorization", x["headers"])
                    else:
                        self.assertEqual(x["headers"].get("authorization"), f"Bearer {PROVIDER_KEY}")
                        self.assertNotIn("x-api-key", x["headers"])
                    self.assertNotIn(KEY, x["raw"], "the Wontopos key never goes to the provider")
                self.assertTrue(svc.seen)
                for x in svc.seen:
                    self.assertEqual({k.lower(): v for k, v in x["headers"].items()}.get("x-api-key"), KEY)
                    self.assertNotIn(PROVIDER_KEY, json.dumps([x["headers"], x["body"]]),
                                     "the provider key never goes to Wontopos")
                self.assert_quiet(heard, res["log"], [])

    async def test_a_redirect_from_the_provider_is_not_followed_with_the_key(self):
        night = FIX["provider_night"]
        svc, mem, base = self.new()
        prov = Provider(night["answers"], "anthropic", redirect=base + "/api/v1/memory/self")
        self.addCleanup(prov.close)
        svc.give(night_rows(0, night))
        with self.assertRaises(wontopos.ModelCallError) as cm:
            await done(mem.sleep("u", llm=prov.caller(), today=night["today"]))
        self.assertIn("redirect refused", str(cm.exception))
        self.assertEqual(len(prov.seen), 1, "not tried again")
        for x in svc.seen:
            self.assertNotIn(PROVIDER_KEY, json.dumps([x["headers"], x["body"]]))
        self.assertNotIn("self/write", [x["path"].replace("/api/v1/memory/", "") for x in svc.seen])

    async def test_progress_the_log_and_errors_carry_no_key_and_no_memory_outside_a_failed_answer(self):
        night = FIX["provider_night"]
        svc, mem, _ = self.new()
        prov = Provider(night["answers"], "anthropic", fail=(400, {"error": {"type": "invalid_request_error",
                                                                             "message": "bad request"}}))
        self.addCleanup(prov.close)
        svc.give(night_rows(0, night))
        heard = []
        with self.assertRaises(wontopos.ModelCallError) as cm:
            await done(mem.sleep("u", llm=prov.caller(), today=night["today"], progress=heard.append))
        self.assert_quiet(heard, [], [cm.exception])
        svc, mem, _ = self.new()
        prov = Provider(night["answers"], "anthropic")
        self.addCleanup(prov.close)
        svc.give(night_rows(0, night))
        svc.fail = [("/api/v1/memory/self/write", 400, {"error": {"code": "self_evidence_unknown",
                                                                  "message": "evidence the sleep was not handed"}})]
        heard = []
        with self.assertRaises(wontopos.BadRequestError) as cm:
            await done(mem.sleep("u", llm=prov.caller(), today=night["today"], progress=heard.append))
        self.assert_quiet(heard, [], [cm.exception])
        svc, mem, _ = self.new()
        prov = Provider(night["answers"], "anthropic", raw=night["unparsable"])
        self.addCleanup(prov.close)
        svc.give(night_rows(0, night))
        heard = []
        res = await done(mem.sleep("u", llm=prov.caller(), today=night["today"], progress=heard.append))
        failed = [x for x in res["log"] if x.get("failed")]
        self.assertEqual([x["call"] for x in failed], ["read 1", "read 1 again 2", "read 1 again 3"])
        self.assertTrue(all(x["head"] == night["unparsable"] for x in failed))
        self.assert_quiet(heard, [x for x in res["log"] if not x.get("failed")], [])
        for x in failed:
            self.assertNotIn(PROVIDER_KEY, json.dumps(x))
            self.assertNotIn(KEY, json.dumps(x))

    def assert_quiet(self, progress, log, errors):
        night = FIX["provider_night"]
        said = [*progress, *(json.dumps(x, ensure_ascii=False) for x in log),
                *(s for e in errors for s in (str(e), repr(e)))]
        for s in said:
            for secret in (PROVIDER_KEY, KEY, night["memory_marker"], night["answer_marker"]):
                self.assertNotIn(secret, s)


class Provider:

    def __init__(self, answers, kind, *, fail=None, redirect=None, raw=None):
        self.answers, self.kind, self.fail, self.redirect, self.raw = answers, kind, fail, redirect, raw
        self.seen = []
        prov = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n).decode()
                prov.seen.append({"path": self.path, "headers": {k.lower(): v for k, v in self.headers.items()},
                                  "raw": raw + json.dumps(dict(self.headers))})
                status, payload, extra = prov.answer(json.loads(raw))
                data = json.dumps(payload).encode()
                self.send_response(status)
                for k, v in extra.items():
                    self.send_header(k, v)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *_):
                pass

        self.srv = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.srv.serve_forever, kwargs={"poll_interval": 0.002}, daemon=True).start()
        self.base = f"http://127.0.0.1:{self.srv.server_port}"

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()

    def caller(self):
        if self.kind == "anthropic":
            return wontopos.anthropic_caller(PROVIDER_KEY, base_url=self.base, model="m")
        return wontopos.openai_caller(base_url=self.base + "/v1", api_key=PROVIDER_KEY, model="m")

    def answer(self, body):
        if self.redirect is not None:
            return 307, {}, {"Location": self.redirect}
        if self.fail is not None:
            return self.fail[0], self.fail[1], {}
        if self.kind == "anthropic":
            system = "".join(b["text"] for b in body["system"])
            user = body["messages"][0]["content"]
        else:
            system, user = body["messages"][0]["content"], body["messages"][1]["content"]
        text = self.raw if self.raw is not None else answer_for(self.answers, system, user)
        usage = {"input_tokens": tok(user), "output_tokens": tok(text)}
        if self.kind == "anthropic":
            return 200, {"content": [{"type": "text", "text": text}], "stop_reason": "end_turn", "usage": usage}, {}
        return 200, {"choices": [{"message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
                     "usage": {"prompt_tokens": usage["input_tokens"], "completion_tokens": usage["output_tokens"]}}, {}


class SyncSleepAttackTest(SleepAttackCases, unittest.IsolatedAsyncioTestCase):
    def new(self, svc=None):
        svc = svc or Service()
        srv, base = serve(svc)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return svc, wontopos.Client(KEY, base_url=base), base

    def client_at(self, base):
        return wontopos.Client(KEY, base_url=base)


class AsyncSleepAttackTest(SleepAttackCases, unittest.IsolatedAsyncioTestCase):
    def new(self, svc=None):
        if not HAVE_HTTPX:
            self.skipTest("httpx not installed (pip install 'wontopos[async]')")
        svc = svc or Service()
        srv, base = serve(svc)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        mem = wontopos.AsyncClient(KEY, base_url=base)
        self.addAsyncCleanup(mem.aclose)
        return svc, mem, base

    def client_at(self, base):
        if not HAVE_HTTPX:
            self.skipTest("httpx not installed (pip install 'wontopos[async]')")
        mem = wontopos.AsyncClient(KEY, base_url=base)
        self.addAsyncCleanup(mem.aclose)
        return mem


class SleepAttackRuleTest(unittest.TestCase):

    def test_the_rule_of_the_by_name_scenario_gives_the_length_each_match_needs(self):
        name = "a-person-is-spoken-of-by-name-not-by-a-piece-of-one"
        rule = next(s["rule"] for s in FIX["scenarios"] if s["name"] == name)
        for says in ("its name with something joined to its end when the name has two letters or more",
                     "the first three letters or more of its name", "a longer topic that starts with a one-letter name"):
            self.assertIn(says, rule)


if __name__ == "__main__":
    unittest.main()
