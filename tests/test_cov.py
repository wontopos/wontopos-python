import asyncio
import copy
import datetime
import functools
import inspect
import itertools
import json
import os
import pickle
import socket
import sys
import threading
import unittest
import warnings
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos  # noqa: E402
from wontopos import AsyncClient, Client, ModelCall, ModelCallError, WosError, anthropic_caller, openai_caller  # noqa: E402
from test_client import HAVE_HTTPX  # noqa: E402
from test_router import _Server  # noqa: E402

KEY = "wos-test-xxxxxxxxxx"
LLM_KEY = "sk-test-0000000000000000"
MODEL_MAX = 32 * 1024 * 1024
DAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


def answer(status, body, headers=None, length=True):
    data = body.encode() if isinstance(body, str) else body
    head = [f"HTTP/1.1 {status} X", "Content-Type: application/json", "Connection: close"]
    if length:
        head.append(f"Content-Length: {len(data)}")
    head += [f"{k}: {v}" for k, v in (headers or {}).items()]
    return ("\r\n".join(head) + "\r\n\r\n").encode() + data


def endless(status=200):
    def send(conn):
        conn.sendall(answer(status, b"", length=False))
        block = b"0" * (1 << 20)
        for _ in range(MODEL_MAX // len(block) + 2):
            conn.sendall(block)
    return send


class RawServer:

    def __init__(self, *replies):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.base = f"http://127.0.0.1:{self.sock.getsockname()[1]}"
        self.paths = []
        threading.Thread(target=self._serve, args=(list(replies),), daemon=True).start()

    def _serve(self, replies):
        for reply in replies:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            with conn:
                try:
                    buf = b""
                    while b"\r\n\r\n" not in buf and (not buf or buf[:1].isalpha()):
                        got = conn.recv(65536)
                        if not got:
                            break
                        buf += got
                    head, _, body = buf.partition(b"\r\n\r\n")
                    self.paths.append(" ".join(head.split(b"\r\n")[0].decode().split(" ")[:2]))
                    n = next((int(x.split(b":")[1]) for x in head.split(b"\r\n") if x.lower().startswith(b"content-length:")), 0)
                    while len(body) < n:
                        got = conn.recv(65536)
                        if not got:
                            break
                        body += got
                    if callable(reply):
                        reply(conn)
                    else:
                        conn.sendall(reply)
                except OSError:
                    pass

    def close(self):
        self.sock.close()


def rfc850(year):
    day = DAYS[datetime.date(year, 1, 1).weekday()]
    return f"{day}, 01-Jan-{year % 100:02d} 00:00:00 GMT"
PLAN_INPUT = {
    "type": "object",
    "properties": {
        "tags": {"type": "array", "minItems": 1, "maxItems": 2, "items": {"type": "string"}},
        "mode": {"enum": ["fast", {"at": 1, "by": [True, None]}]},
        "fixed": {"const": {"a": 1, "b": "x"}},
        "size": {"anyOf": [{"type": "number", "minimum": 1}, {"type": "null"}]},
        "flag": {"type": "boolean"},
        "extra": {"type": "object", "additionalProperties": {"type": "integer"}},
    },
    "additionalProperties": False,
}
LOOKUP = {"name": "lookup", "description": "Look it up.", "kind": "llm", "instructions": "Answer.",
          "input": {"type": "object"}}


async def done(x):
    return await x if inspect.isawaitable(x) else x


def decided(**d):
    return {"text": json.dumps({"action": "skill", "skill": "", "input": "", "note": "", "text": "", **d}),
            "stop": "end"}


class Recorder:

    def __init__(self, *replies):
        self.calls = []
        self.replies = list(replies)

    def call(self, c):
        self.calls.append(c)
        r = self.replies.pop(0) if self.replies else decided(action="cannot", text="no")
        if isinstance(r, Exception):
            raise r
        return r


def plan_skill(input=None):
    return {"name": "plan", "description": "Plan it.", "kind": "code", "input": input or {"type": "object"}}


class RouterCovCases:

    async def refused(self, aw, message):
        with self.assertRaises((ValueError, TypeError)) as cm:
            await done(aw())
        self.assertEqual(str(cm.exception), message)

    async def test_a_skills_input_is_checked_against_lists_enums_consts_any_of_types_and_extra_fields(self):
        ran = []
        mem = self.make(router={"router_caller": Recorder(), "handlers": {"plan": lambda i, c: ran.append(i) or "planned"}})
        await done(mem.add_skill(plan_skill(PLAN_INPUT)))
        refused = [
            ({"tags": []}, "input.tags must hold at least 1 items"),
            ({"tags": ["a", "b", "c"]}, "input.tags must hold at most 2 items"),
            ({"tags": ["a", 2]}, "input.tags[1] must be string"),
            ({"tags": "a"}, "input.tags must be array"),
            ({"mode": "slow"}, 'input.mode must be one of ["fast",{"at":1,"by":[true,null]}]'),
            ({"mode": {"at": 1, "by": [True]}}, 'input.mode must be one of ["fast",{"at":1,"by":[true,null]}]'),
            ({"fixed": {"a": 1}}, 'input.fixed must be {"a":1,"b":"x"}'),
            ({"size": 0}, "input.size fits none of the shapes it may take"),
            ({"size": "big"}, "input.size fits none of the shapes it may take"),
            ({"flag": "yes"}, "input.flag must be boolean"),
            ({"extra": {"n": 1.5}}, "input.extra.n must be integer"),
            ({"other": 1}, "input.other is not a field this skill takes"),
        ]
        for inp, why in refused:
            with self.subTest(inp=inp):
                await self.refused(lambda: mem.run_skill("plan", inp), f"plan: {why}.")
        self.assertEqual(ran, [])
        inp = {"tags": ["a"], "mode": {"by": [True, None], "at": 1}, "fixed": {"b": "x", "a": 1}, "size": None,
               "flag": False, "extra": {"n": 2}}
        r = await done(mem.run_skill("plan", inp))
        self.assertEqual((r["ok"], r["text"], ran), (True, "planned", [inp]), r.get("detail"))
        self.assertTrue((await done(mem.run_skill("plan", {"size": 2.5, "mode": "fast"})))["ok"])

    async def test_a_skills_input_is_checked_against_string_lengths_and_number_bounds(self):
        mem = self.make(router={"router_caller": Recorder(), "handlers": {"plan": lambda i, c: "ok"}})
        schema = {"type": "object", "properties": {
            "s": {"type": "string", "minLength": 2, "maxLength": 3},
            "n": {"type": "number", "minimum": 1, "maximum": 9, "exclusiveMinimum": 2, "exclusiveMaximum": 8}}}
        await done(mem.add_skill(plan_skill(schema)))
        refused = [({"s": "a"}, "input.s must be at least 2 characters"), ({"s": "abcd"}, "input.s must be at most 3 characters"),
                   ({"n": 0}, "input.n must be at least 1"), ({"n": 10}, "input.n must be at most 9"),
                   ({"n": 2}, "input.n must be over 2"), ({"n": 8.5}, "input.n must be under 8")]
        for inp, why in refused:
            with self.subTest(inp=inp):
                await self.refused(lambda: mem.run_skill("plan", inp), f"plan: {why}.")
        self.assertTrue((await done(mem.run_skill("plan", {"s": "ab", "n": 5})))["ok"])

    async def test_a_skills_input_schema_is_refused_where_a_list_keyword_holds_no_list(self):
        mem = self.make(router={"router_caller": Recorder(), "handlers": {"plan": lambda i, c: "ok"}})
        refused = [
            ({"type": "object", "properties": {"tags": {"type": "array", "items": "string"}}},
             "input.tags[]: a schema is an object."),
            ({"type": "object", "properties": {"mode": {"enum": []}}}, "input.mode: enum must be a non-empty list."),
            ({"type": "object", "properties": {"mode": {"enum": "fast"}}}, "input.mode: enum must be a non-empty list."),
            ({"type": "object", "properties": {"size": {"anyOf": []}}},
             "input.size: anyOf must be a non-empty list of schemas."),
            ({"type": "object", "properties": {"size": {"anyOf": [{"type": "nope"}]}}},
             "input.size.anyOf[0]: type must be one of object, array, string, number, integer, boolean, null, "
             "or a list of them."),
            ({"type": "object", "additionalProperties": 1}, "input.*: a schema is an object."),
            ({"type": "object", "properties": []}, "input: properties must be an object of schemas."),
            ({"type": "object", "required": [1]}, "input: required must be a list of property names."),
            ({"type": "object", "properties": {"s": {"minLength": -1}}},
             "input.s: minLength must be a whole number of 0 or more."),
            ({"type": "object", "properties": {"n": {"maximum": "9"}}}, "input.n: maximum must be a number."),
        ]
        for schema, why in refused:
            with self.subTest(why=why):
                await self.refused(lambda: mem.add_skill(plan_skill(schema)), why)
        deep = {"type": "object"}
        for _ in range(33):
            deep = {"type": "object", "properties": {"a": deep}}
        await self.refused(lambda: mem.add_skill(plan_skill(deep)),
                           "input" + ".a" * 33 + ": the schema nests deeper than 32 levels.")
        await self.refused(lambda: mem.add_skill(plan_skill({"type": "object", "items": 1})), "input[]: a schema is an object.")
        self.assertEqual(await done(mem.list_skills()), [])

    async def test_skill_md_frontmatter_skips_comments_joins_indented_lines_and_reads_single_quotes(self):
        mem = self.make(router={"router_caller": Recorder()})
        text = "\n".join([
            "---", "# kept out", "name: plan-trip", "", "description: Plans a trip", "  over two lines", "",
            "note: 'It''s fine'", "empty: |", "tags: [a, 'b c']", "---", "Plan it.",
        ])
        d = await done(mem.import_skill_md(None, text))
        self.assertEqual(d["description"], "Plans a trip over two lines")
        self.assertEqual(d["metadata"]["frontmatter"], {"note": "It's fine", "empty": "", "tags": ["a", "b c"]})
        refused = [
            ("---\n  name: a\ndescription: x\n---\nDo it.",
             "SKILL.md frontmatter, line 2: an indented line with no key above it."),
            ('---\nname: a\ndescription: "quoted"\n  continued\n---\nDo it.',
             "SKILL.md frontmatter, line 3: a quoted value cannot go on over the indented lines below it."),
            ('---\nname: a\ndescription: "bad \\q"\n---\nDo it.',
             "SKILL.md frontmatter, line 3: the double-quoted value is not valid."),
        ]
        for md, why in refused:
            with self.subTest(why=why):
                await self.refused(lambda: mem.import_skill_md(None, md), why)

    async def test_a_code_skills_answer_that_is_not_json_is_its_text_and_its_throw_is_a_tool_error(self):
        cyclic = {}
        cyclic["self"] = cyclic

        def fail(i, c):
            raise RuntimeError("the oven is off")

        mem = self.make(router={"router_caller": Recorder(), "handlers": {"loop": lambda i, c: cyclic, "fail": fail}})
        for name in ("loop", "fail"):
            await done(mem.add_skill({**plan_skill(), "name": name}))
        r = await done(mem.run_skill("loop", {}))
        self.assertEqual((r["ok"], r["text"]), (True, str(cyclic)), r.get("detail"))
        f = await done(mem.run_skill("fail", {}))
        self.assertEqual((f["ok"], f["reason"], f["detail"], f["skill"]),
                         (False, "tool_error", "fail threw: the oven is off", "fail"))
        self.assertIsInstance(f["text"], str)

    async def test_an_llm_skill_with_no_caller_is_not_configured_and_one_answering_no_text_failed(self):
        lonely = self.make(router={"router_caller": Recorder()})
        await done(lonely.add_skill(LOOKUP))
        r = await done(lonely.run_skill("lookup", {}))
        self.assertEqual((r["ok"], r["reason"], r["detail"]),
                         (False, "not_configured", "an llm skill needs router.key, router.skill_key or router.skill_caller"))
        mute = Recorder({"text": "  ", "stop": "end", "usage": {"input_tokens": 3, "cache": {"read": 1}, "tier": "standard"}})
        mem = self.make(router={"router_caller": Recorder(), "skill_caller": mute})
        await done(mem.add_skill(LOOKUP))
        e = await done(mem.run_skill("lookup", {}))
        self.assertEqual((e["ok"], e["reason"], e["detail"]), (False, "failed", "the skill model answered with no text"))
        self.assertEqual(e["usage"], {"skill": {"input_tokens": 3, "cache": {"read": 1}, "tier": "standard"}})

    async def test_a_provider_error_with_a_status_the_router_has_no_reason_for_is_failed(self):
        mem = self.make(router={"router_caller": Recorder(ModelCallError(409, "busy elsewhere"))})
        await done(mem.add_skill(LOOKUP))
        r = await done(mem.ask("q", history="none"))
        self.assertEqual((r["ok"], r["reason"], r["detail"]), (False, "failed", "409: busy elsewhere"))

    async def test_a_router_decision_inside_prose_is_found_past_escaped_quotes_and_letters(self):
        said = ('Thinking {"x":"{\\"y", z} then {"action":"cannot","skill":"","input":"","note":"a\\tb",'
                '"text":"no \\"way\\"\\n"} done')
        mem = self.make(router={"router_caller": Recorder({"text": said, "stop": "end"})})
        await done(mem.add_skill(LOOKUP))
        r = await done(mem.ask("q", history="none"))
        self.assertEqual((r["ok"], r["action"], r["text"]), (True, "cannot", 'no "way"'), r.get("detail"))

    async def test_router_options_that_are_not_what_they_should_be_are_refused(self):
        rec = Recorder()
        refused = [
            ([("router_caller", rec)], "router must be a dict of router options."),
            ({"router_caller": rec, "callers": [rec]}, "router.callers must map names to callers."),
            ({"router_caller": rec, "callers": {"Big": rec}},
             'router.callers: "Big" is not a name (1-32 of [a-z0-9_], a letter first).'),
            ({"router_caller": rec, "prompt": "  \n"}, "router.prompt must be the whole router prompt: a non-blank string."),
            ({"router_caller": rec, "prompt": 7}, "router.prompt must be the whole router prompt: a non-blank string."),
            ({"router_caller": rec, "name": " "}, "router.name must be a non-blank string."),
            ({"router_caller": rec, "handlers": ["plan"]}, "router.handlers must map skill names to functions."),
            ({"router_caller": rec, "handlers": {"9plan": len}},
             'router.handlers: "9plan" is not a skill name (1-32 of [a-z0-9_], a letter first).'),
            ({"router_caller": rec, "handlers": {"plan": "run"}}, "router.handlers.plan must be a function."),
            ({"router_caller": rec, "limits": [1]}, "router.limits must be a dict."),
            ({"router_caller": rec, "time_zone": " "},
             'router.time_zone must be an IANA time zone name such as "Asia/Seoul".'),
        ]
        for router, why in refused:
            with self.subTest(why=why):
                with self.assertRaises((ValueError, TypeError)) as cm:
                    self.make(router=router)
                self.assertEqual(str(cm.exception), why)
        for bad in (0, -1, "1", True):
            with self.assertRaises(ValueError) as cm:
                self.make(router={"router_caller": rec, "limits": {"time": bad}})
            self.assertTrue(str(cm.exception).startswith("router.limits.time must be a number of seconds over 0"),
                            str(cm.exception))

    async def test_run_skill_refuses_an_input_that_is_no_dict_or_no_json_and_a_bad_speaker(self):
        mem = self.make(router={"router_caller": Recorder(), "handlers": {"plan": lambda i, c: "ok"}})
        await done(mem.add_skill(plan_skill()))
        await self.refused(lambda: mem.run_skill("plan", ["a"]), "input must be a dict: the input the skill takes.")
        await self.refused(lambda: mem.run_skill("plan", {"n": {1, 2}}), "input must be JSON: a set cannot be JSON")
        await self.refused(lambda: mem.run_skill("plan", {True: 1}), "input must be JSON: a key of bool cannot be JSON")
        await self.refused(lambda: mem.run_skill("plan", {}, speaker="A" * 257), "speaker is at most 256 bytes.")
        await self.refused(lambda: mem.run_skill("plan", {}, speaker="A\x01"),
                           "speaker contains a line break or a control character.")
        r = await done(mem.run_skill("plan", {"n": {1: "a", 2.5: "b"}}))
        self.assertEqual(r["input"], {"n": {"1": "a", "2.5": "b"}})

    async def test_a_skill_that_cannot_be_kept_as_json_is_refused_with_the_reason(self):
        mem = self.make(router={"router_caller": Recorder(), "handlers": {"plan": lambda i, c: "ok"}})
        await self.refused(lambda: mem.add_skill({**plan_skill(), "metadata": {"n": {1, 2}}}),
                           "plan: the skill cannot be kept as JSON (a set cannot be JSON).")

    async def test_an_answer_that_used_its_whole_time_asks_no_more(self):
        class Slow:
            async def call(self, c):
                await asyncio.sleep(0.3)
                return decided(action="cannot", text="no")

        mem = self.make(router={"router_caller": Slow(), "limits": {"time": 0.1}})
        await done(mem.add_skill(LOOKUP))
        first = await done(mem.ask("q", turn="a", history="none"))
        self.assertEqual(first["reason"], "timeout")
        second = await done(mem.ask("q", turn="a", history="none"))
        self.assertEqual((second["reason"], second["detail"]), ("limit", "this answer has used its 0.1 s"))


    async def test_a_provider_answer_too_large_on_its_stream_fails_the_ask_with_that_detail(self):
        srv = RawServer(endless(), endless())
        self.addCleanup(srv.close)
        mem = self.make(router={"router_caller": anthropic_caller(LLM_KEY, base_url=srv.base)})
        await done(mem.add_skill(LOOKUP))
        r = await done(mem.ask("q", history="none"))
        self.assertEqual((r["ok"], r["reason"], r["detail"]), (False, "failed", "200: the provider's answer is too large"))


    async def test_the_router_says_what_it_holds_and_refuses_callers_that_are_not_what_they_should_be(self):
        seen = {}

        def plan(i, ctx, *, tag):
            seen["ctx"] = repr(ctx)
            return tag

        class Spy(Recorder):
            def call(self, c):
                seen["call"], seen["signal"] = repr(c), repr(c.signal)
                return super().call(c)

        mem = self.make(router={"router_caller": Spy(), "handlers": {"plan": functools.partial(plan, tag="planned")}})
        await done(mem.add_skill(plan_skill()))
        r = await done(mem.run_skill("plan", {}, ask_id="ask-1"))
        self.assertEqual((r["ok"], r["text"]), (True, "planned"), r.get("detail"))
        self.assertEqual(seen["ctx"], "SkillContext(store='u', ask_id='ask-1', speaker=None)")
        await done(mem.ask("q", history="none"))
        self.assertTrue(seen["call"].startswith("ModelCall(purpose='router', model="), seen["call"])
        self.assertEqual(seen["signal"], "AskSignal(aborted=False)")
        self.assertTrue(await done(mem.remove_skill("plan")))
        self.assertEqual(await done(mem.list_skills()), [])
        self.assertIs(copy.copy(mem)._router, mem._router)
        self.assertIs(copy.deepcopy(mem)._router, mem._router)

        class Odd:
            def __init__(self, **kw):
                self.__dict__.update(kw)

            def call(self, c):
                return decided(action="cannot", text="no")

        refused = [
            ({"router_caller": Odd(count_tokens=3)}, TypeError, "router.router_caller.count_tokens must be a function."),
            ({"router_caller": Odd(base_url=5)}, TypeError, "router.router_caller.base_url must be a URL."),
            ({"router_caller": Recorder(), "provider_base_url": 5}, ValueError,
             "the model provider's base URL must be a URL, e.g. https://api.anthropic.com."),
        ]
        for router, kind, why in refused:
            with self.subTest(why=why):
                with self.assertRaises(kind) as cm:
                    self.make(router=router)
                self.assertEqual(str(cm.exception), why)

    async def test_a_skill_copy_to_a_store_id_that_is_none_is_refused(self):
        mem = self.make(router={"router_caller": Recorder(), "handlers": {"plan": lambda i, c: "ok"}})
        await done(mem.add_skill(plan_skill()))
        await self.refused(lambda: mem.copy_skill("plan", "u", ["v", 5]), "to holds 5, which is not a store id.")

    async def test_an_llm_skill_whose_caller_fails_says_why(self):
        class Locked:
            def call(self, c):
                raise ModelCallError(401, "bad key", type="authentication_error")

        mem = self.make(router={"router_caller": Recorder(), "skill_caller": Locked()})
        await done(mem.add_skill(LOOKUP))
        r = await done(mem.run_skill("lookup", {}))
        self.assertEqual((r["ok"], r["reason"], r["detail"]), (False, "auth", "401 authentication_error: bad key"))

    async def test_an_earlier_exchanges_time_with_an_offset_is_shown_on_the_routers_clock(self):
        exchanges = [{"request": "east", "answer": "a", "at": "2026-10-20T09:00:00+09:00"},
                     {"request": "west", "answer": "b", "at": "2026-10-20T09:00:00-05:30"},
                     {"request": "bad", "answer": "c", "at": "2026-10-20T09:00:00+24:00"}]
        engine = _Server(lambda req: (200, {}, {"exchanges": exchanges} if req["path"].endswith("/channel/read")
                                      else {"status": "ok"}))
        self.addCleanup(engine.close)
        rec = Recorder()
        mem = self.make(router={"router_caller": rec}, base_url=engine.base)
        await done(mem.add_skill(LOOKUP))
        r = await done(mem.ask("q"))
        self.assertTrue(r["ok"], r.get("detail"))
        shown = ("[2026-10-20 09:00:00] request: east\nanswer: a\n[2026-10-20 23:30:00] request: west\nanswer: b\n"
                 "[?] request: bad\nanswer: c")
        self.assertIn(shown, rec.calls[0].user)


class ClientCovTest(unittest.TestCase):

    def offline(self, **kw):
        mem = Client(KEY, base_url="http://127.0.0.1:9", max_retries=0, **kw)
        self.addCleanup(mem.close)
        return mem

    def test_shapes_no_test_gave_before_are_refused_before_anything_is_sent(self):
        mem = self.offline()
        with self.assertRaises(ValueError) as cm:
            mem.recall("q", "u", context_limit="5")
        self.assertEqual(str(cm.exception), "context_limit must be an int, got str")
        with self.assertRaises(ValueError) as cm:
            mem.write_sleep_files("s-1", [1], "u")
        self.assertEqual(str(cm.exception), "writes[0] must be a dict.")
        for bad in ("http://127.0.0.1:99999", "ftp://llm.example"):
            with self.assertRaises(ValueError) as cm:
                anthropic_caller(LLM_KEY, base_url=bad)
            self.assertTrue(str(cm.exception).startswith("the model provider's base URL is not an http(s) URL"),
                            str(cm.exception))

    def test_the_unknown_filter_warning_forgets_a_key_past_its_bound_and_warns_for_it_again(self):
        srv = _Server(lambda req: (200, {}, {"memories": []}))
        self.addCleanup(srv.close)
        mem = Client(KEY, base_url=srv.base, max_retries=0)
        self.addCleanup(mem.close)
        with mock.patch.object(wontopos._checks, "_WARNED_FILTER_KEYS_MAX", 1), \
                mock.patch.object(wontopos._checks, "_warned_filter_keys", set()), \
                self.assertLogs("wontopos", "WARNING") as logs:
            for _ in range(2):
                mem.search("q", "u", filters={"cov_a": 1, "cov_b": 2})
        self.assertGreater(len([r for r in logs.records if "unknown search filter" in r.getMessage()]), 2)

    def test_memory_tools_say_nothing_is_held_and_refuse_a_name_that_is_no_text(self):
        srv = _Server(lambda req: (200, {}, {"work_id": "w-1", "memories": []}))
        self.addCleanup(srv.close)
        mem = Client(KEY, base_url=srv.base, max_retries=0, model="scroll-2", tz=9)
        self.addCleanup(mem.close)
        tools = mem.memory_tools("u", dates=True)
        tools.begin("tea")
        self.assertEqual(tools.run({"name": "memory_timeline", "input": {}}), "Nothing is held yet.")
        with self.assertRaises(ValueError) as cm:
            tools.run({"name": object(), "input": {}})
        self.assertTrue(str(cm.exception).startswith("not a memory tool: <object object at"), str(cm.exception))

    def test_a_blank_base_url_is_refused(self):
        kinds = (Client, AsyncClient) if HAVE_HTTPX else (Client,)
        for kind in kinds:
            for bad in ("  ", "\t\n"):
                with self.assertRaises(ValueError) as cm:
                    kind(KEY, base_url=bad)
                self.assertEqual(str(cm.exception), "base_url must be a non-empty string")

    def test_a_client_is_a_context_manager_and_comes_from_the_environment(self):
        with Client(KEY) as mem:
            self.assertIsInstance(mem.with_deadline(5), Client)
        with mock.patch.dict(os.environ, {"WONTOPOS_API_KEY": KEY}):
            self.assertIsInstance(Client.from_env(), Client)
            if HAVE_HTTPX:
                amem = AsyncClient.from_env()
                self.assertIsInstance(amem.with_deadline(5), AsyncClient)
                asyncio.run(amem.aclose())


class CallerCovTest(unittest.TestCase):

    def call(self, caller):
        with self.assertRaises(ModelCallError) as cm:
            caller.call(ModelCall("router", "m", ["s"], "u", 10))
        return cm.exception

    def serve(self, *replies):
        srv = RawServer(*replies)
        self.addCleanup(srv.close)
        return srv

    def test_a_provider_answer_that_is_not_json_or_too_large_by_its_length_or_its_bytes_is_refused(self):
        big = MODEL_MAX + 1
        long = answer(200, b"{}", {"Content-Length": str(big)}, length=False)
        srv = self.serve(answer(200, "not json"), answer(200, "[1]"), long, endless())
        c = anthropic_caller(LLM_KEY, base_url=srv.base)
        for why in ("the provider's answer is not JSON", "the provider's answer is not a JSON object",
                    f"the provider's answer is too large ({big} bytes)", "the provider's answer is too large"):
            e = self.call(c)
            self.assertEqual((e.status, e.message), (200, why))

    def test_a_provider_error_whose_status_has_no_reason_keeps_none_and_its_message_is_found_where_it_was_put(self):
        bodies = [(json.dumps({"error": {"message": "conflict"}}), "conflict"), ("plain words", "plain words"),
                  ("[1, 2]", "[1, 2]"), (json.dumps({"error": "said as a string"}), "said as a string"),
                  (json.dumps({"message": "said at the top"}), "said at the top")]
        srv = self.serve(*(answer(409, b) for b, _ in bodies))
        c = openai_caller(base_url=srv.base, model="m", api_key=LLM_KEY)
        for _, said in bodies:
            e = self.call(c)
            self.assertEqual((e.status, e.reason, e.message), (409, None, said))

    def test_a_retry_after_in_the_old_rfc_850_form_reads_its_two_digit_year_as_at_most_50_years_ahead(self):
        year = datetime.datetime.now(datetime.timezone.utc).year
        dates = [rfc850(year + 49), rfc850(year - 49), rfc850(year - 1)]
        srv = self.serve(*(answer(429, "{}", {"Retry-After": d}) for d in dates))
        c = openai_caller(base_url=srv.base, model="m", api_key=LLM_KEY)
        ahead = self.call(c).retry_after
        self.assertGreater(ahead or 0, 48 * 365 * 86400, dates[0])
        for d in dates[1:]:
            self.assertEqual(self.call(c).retry_after, 0, d)

    def test_an_openai_answer_stopped_by_its_content_filter_is_a_refusal(self):
        said = {"choices": [{"message": {"content": "partial"}, "finish_reason": "content_filter"}]}
        srv = self.serve(answer(200, json.dumps(said)))
        r = openai_caller(base_url=srv.base, model="m", api_key=LLM_KEY).call(ModelCall("router", "m", ["s"], "u", 10))
        self.assertEqual((r["text"], r["stop"]), ("partial", "refusal"))

    def test_a_count_without_input_tokens_is_an_error(self):
        srv = self.serve(answer(200, "{}"))
        c = anthropic_caller(LLM_KEY, base_url=srv.base)
        with self.assertRaises(ModelCallError) as cm:
            c.count_tokens(ModelCall("router", "m", ["s"], "u", 10))
        self.assertEqual((cm.exception.status, cm.exception.message), (200, "count_tokens answered without input_tokens"))

    def test_caller_settings_that_are_not_what_they_should_be_are_refused(self):
        refused = [
            (lambda: anthropic_caller("  "), "api_key is required: the model provider's key."),
            (lambda: anthropic_caller(None), "api_key is required: the model provider's key."),
            (lambda: anthropic_caller("sk-t\N{LATIN SMALL LETTER E WITH ACUTE}st"),
             "api_key contains a non-ASCII character: copy the key from a plain-text field."),
            (lambda: anthropic_caller(LLM_KEY, cache_ttl="2h"), 'cache_ttl must be "5m" or "1h".'),
            (lambda: openai_caller(base_url="https://llm.example", model="m", max_tokens_field="max"),
             'max_tokens_field must be "max_tokens" or "max_completion_tokens".'),
            (lambda: openai_caller(base_url="https://llm.example", model="m", reasoning_effort="yes"),
             'reasoning_effort must be true or false, got "yes".'),
        ]
        for make, why in refused:
            with self.subTest(why=why):
                with self.assertRaises(ValueError) as cm:
                    make()
                self.assertEqual(str(cm.exception), why)
        with self.assertRaises(TypeError) as cm:
            pickle.dumps(anthropic_caller(LLM_KEY))
        self.assertEqual(str(cm.exception),
                         "a model caller cannot be pickled: it holds the provider's key. Build one per process.")


class MemoryCovCases:

    def served(self, answer):
        srv = _Server(answer)
        self.addCleanup(srv.close)
        return srv, self.make(base_url=srv.base, max_retries=0)

    async def test_usage_takes_a_whole_number_of_days_from_1_to_365_and_sends_nothing_otherwise(self):
        srv, mem = self.served(lambda req: (200, {}, {}))
        for bad in (0, 366, 1.5):
            with self.assertRaises(ValueError) as cm:
                await done(mem.usage(bad))
            self.assertEqual(str(cm.exception), f"days must be an integer between 1 and 365, got {bad}.")
        self.assertEqual(srv.seen, [])

    async def test_an_image_answer_with_no_bytes_is_an_error_and_an_image_that_is_no_dict_is_refused(self):
        srv, mem = self.served(lambda req: (200, {}, "" if req["path"].endswith("/image") else {"status": "ok"}))
        with self.assertRaises(WosError) as cm:
            await done(mem.get_image("u", "m1"))
        self.assertEqual(cm.exception.message, "empty image body — the service returned no bytes")
        refused = [
            ("abc", TypeError, "image must be a dict — {'data': '<base64>'}"),
            ({"data": " "}, ValueError, "image['data'] is required — base64 of the image (a data: URL is fine)"),
            ({"data": "data:image/png;base64,"}, ValueError, "image['data'] is empty after stripping its data: prefix"),
        ]
        for image, kind, why in refused:
            with self.assertRaises(kind) as cm:
                await done(mem.add("a photo", "u", image=image))
            self.assertEqual(str(cm.exception), why)
        self.assertEqual(len(srv.seen), 1)
        image = {"data": "data:image/png;base64,QUJD", "reference": "r1", "taken_at": "2026-10-20", "other": 1}
        await done(mem.add("a photo", "u", image=image))
        self.assertEqual(srv.seen[-1]["body"]["image"], {"data": "QUJD", "reference": "r1", "taken_at": "2026-10-20"})

    async def test_a_page_cursor_the_service_repeats_as_an_object_ends_the_walk(self):
        rows = []
        srv, mem = self.served(lambda req: (200, {}, {"memories": rows, "next_cursor": {"page": 2}}))
        self.assertEqual(await done(mem.export_memories()), [])
        self.assertEqual(len(srv.seen), 2)
        rows.append({"id": "m1", "content": "x"})
        with self.assertRaises(RuntimeError) as cm:
            await done(mem.export_memories())
        self.assertEqual(str(cm.exception), "the service returned a page cursor it had already returned. "
                                            "This is a truncated answer, not the whole store.")

    async def test_a_walk_whose_every_page_brings_a_new_cursor_stops_at_the_page_limit(self):
        n = itertools.count()

        def answer(req):
            i = next(n)
            if req["path"].endswith("/images"):
                return 200, {}, {"images": [], "has_more": True, "next_before": f"b{i}"}
            return 200, {}, {"memories": [], "next_cursor": f"c{i}"}

        srv, mem = self.served(answer)
        with mock.patch.object(wontopos._memory, "_MAX_PAGES", 3), mock.patch.object(wontopos._images, "_MAX_PAGES", 3):
            for walk in (mem.export_memories, mem.export_images):
                with self.assertRaises(RuntimeError) as cm:
                    await done(walk())
                self.assertEqual(str(cm.exception), "stopped after 3 pages — the store did not end. "
                                                    "This is a truncated answer, not the whole store.")
        self.assertEqual(len(srv.seen), 6)
        same, again = self.served(lambda req: (200, {}, {"images": [], "has_more": True, "next_before": "b",
                                                         "next_skip_ids": ["i1"]}))
        self.assertEqual(await done(again.export_images()), [])
        self.assertEqual(len(same.seen), 2)

    async def test_an_image_walk_ends_where_the_service_says_there_is_no_more(self):
        srv, mem = self.served(lambda req: (200, {}, {"images": [{"id": "i1"}], "has_more": False, "next_before": "b"}))
        self.assertEqual(await done(mem.export_images()), [{"id": "i1"}])
        self.assertEqual(len(srv.seen), 1)

    async def test_a_body_over_the_cap_is_refused_as_it_streams_or_by_its_length(self):
        srv = RawServer(answer(200, b"x" * 100, length=False), answer(200, b"x" * 100))
        self.addCleanup(srv.close)
        mem = self.make(base_url=srv.base, max_retries=0)
        with mock.patch.object(wontopos, "_MAX_RESPONSE_BYTES", 10):
            for why in ("response too large — refusing to buffer it", "response too large (100 bytes) — refusing to buffer it"):
                with self.assertRaises(WosError) as cm:
                    await done(mem.stats("u"))
                self.assertEqual(cm.exception.message, why)

    async def test_https_to_a_plain_http_server_is_a_network_error_that_names_tls(self):
        srv = RawServer(answer(200, "{}"))
        self.addCleanup(srv.close)
        mem = self.make(base_url=srv.base.replace("http://", "https://"), max_retries=0)
        with self.assertRaises(WosError) as cm:
            await done(mem.stats("u"))
        self.assertEqual(cm.exception.status, 0)
        self.assertIn("(TLS: ", cm.exception.message)

    async def test_engrams_and_a_store_delete_go_where_they_should(self):
        srv = RawServer(answer(200, '{"engrams": []}'), answer(200, '{"status": "ok"}'))
        self.addCleanup(srv.close)
        mem = self.make(base_url=srv.base, max_retries=0)
        await done(mem.list_engrams())
        await done(mem.delete_store("u"))
        self.assertEqual(srv.paths, ["GET /api/v1/engram", "DELETE /api/v1/memory/collection"])

    async def test_the_plain_http_warning_forgets_its_oldest_host_past_its_bound(self):
        with mock.patch.object(wontopos._http, "_WARNED_PLAIN_HTTP_MAX", 2), warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            for host in ("a", "b", "c", "c", "a"):
                self.make(base_url=f"http://cov-{host}-{self.kind}.example")
        self.assertEqual(len([x for x in w if "plain HTTP" in str(x.message)]), 4)

    async def test_two_error_keys_that_clean_to_one_keep_the_last_value_as_typescript_does(self):
        srv, mem = self.served(lambda req: (400, {}, '{"error":{"message":"dup","de\\u0001tail":1,"detail":2}}'))
        with self.assertRaises(WosError) as cm:
            await done(mem.stats("u"))
        self.assertEqual(cm.exception.details, {"detail": 2})

    async def test_an_error_body_nested_600_levels_keeps_its_message_and_its_details_on_every_python(self):
        nest = "[" * 600 + "0" + "]" * 600
        srv, mem = self.served(lambda req: (400, {}, '{"error":{"message":"deep","nest":' + nest + "}}"))
        with self.assertRaises(WosError) as cm:
            await done(mem.stats("u"))
        self.assertEqual(cm.exception.message, "deep")
        self.assertEqual(sorted(cm.exception.details or {}), ["nest"])


    async def test_a_sleep_estimate_reads_from_a_given_time_and_stops_at_a_repeat_or_the_page_limit(self):
        pages = []
        n = itertools.count()

        def answer(req):
            if req["path"].endswith("/sleep"):
                return 200, {}, pages.pop(0) if pages else {"memories": [], "next_cursor": f"c{next(n)}"}
            return 200, {}, SELF

        srv, mem = self.served(answer)
        pages[:] = [{"memories": NIGHT, "next_cursor": "c1"}, {"memories": [], "next_cursor": "c1"}]
        est = await done(mem.sleep_estimate("u", since="2026-10-19T00:00:00Z"))
        self.assertEqual(est["memories"], 1)
        first, second = srv.seen[0]["body"], srv.seen[1]["body"]
        self.assertEqual((first.get("since"), "since_last_sleep" in first, second["cursor"]),
                         ("2026-10-19T00:00:00Z", False, "c1"))
        pages[:] = [{"memories": NIGHT, "next_cursor": "c1"}, {"memories": NIGHT, "next_cursor": "c1"}]
        with self.assertRaises(RuntimeError) as cm:
            await done(mem.sleep_estimate("u"))
        self.assertEqual(str(cm.exception), "the service returned a page cursor it had already returned. "
                                            "This is a truncated answer, not the whole store.")
        with mock.patch.object(wontopos.sleep, "_MAX_PAGES", 2):
            with self.assertRaises(RuntimeError) as cm:
                await done(mem.sleep_estimate("u"))
        self.assertEqual(str(cm.exception), "the sleep's reading passed its page limit. "
                                            "This is a truncated answer, not the whole store.")


    async def test_a_sleep_refuses_a_rewrite_with_no_section_a_dated_box_section_and_remakes_it_cannot_take(self):
        lines = NIGHT + [{"id": "m-2", "content": "Dana said the roses need water.", "created_at": "2026-10-19T09:01:00Z",
                          "speaker": "Dana", "tokens": 9}]
        night = {"user_id": "u", "sleep_id": "s-1", "memories": lines, "count": 2, "tokens": 17, "exhausted": True,
                 "next_cursor": None}
        evidence = ["e1", "e2"]

        def answer(req):
            p = req["path"]
            return 200, {}, night if p.endswith("/sleep") else {"notes": {"proposals": [], "mixed": []}} \
                if p.endswith("/self/notes") else SELF

        srv, mem = self.served(answer)
        edits = [{"file": "core", "op": "rewrite", "section": "", "text": "Dana gardens.", "evidence": evidence},
                 {"file": "1", "op": "rewrite", "section": "2026-10-20", "text": "Roses went in.", "evidence": evidence},
                 {"file": "core", "op": "remake", "title": "Core", "text": "Everything anew.", "evidence": evidence},
                 {"file": "1", "op": "remake", "title": "", "text": "## Garden\nRoses.", "evidence": evidence}]
        read = {"items": [{"kind": "person", "topic": "Dana", "text": "Dana planted roses and waters them.",
                           "evidence": evidence}], "mixed": []}

        def llm(c):
            said = {"edits": edits, "notes": []} if "<items>" in c.user else read if "<memories>" in c.user \
                else {"add": [], "op": "none", "drop": [], "text": ""}
            return {"text": json.dumps(said), "stop": "end"}

        res = await done(mem.sleep("u", llm=llm, assistant="Kai", today="2026-10-20 09:30"))
        self.assertEqual([r["why"] for r in res["refused"]], [
            "rewrite needs a section and text",
            "a box section is a matter, not its state on a date: rewrite the matter's section",
            "the core is rewritten by sections, not remade",
            "remake needs a title and the whole file",
        ])


SELF = {
    "user_id": "u",
    "core": {"title": "", "version": 0, "tokens": 0, "written_at": None, "day": None, "body": ""},
    "files": [{"slot": s, "title": "", "version": 0, "tokens": 0, "written_at": None, "day": None} for s in (1, 2, 3, 4)],
    "notes_version": 0,
}
NIGHT = [{"id": "m-1", "content": "Dana planted roses today.", "created_at": "2026-10-19T09:00:00Z", "speaker": "Dana",
          "tokens": 8}]


class SyncMemoryCovTest(MemoryCovCases, unittest.IsolatedAsyncioTestCase):
    kind = "sync"

    def make(self, **kw):
        mem = Client(KEY, **kw)
        self.addCleanup(mem.close)
        return mem


@unittest.skipUnless(HAVE_HTTPX, "httpx not installed (pip install 'wontopos[async]')")
class AsyncMemoryCovTest(MemoryCovCases, unittest.IsolatedAsyncioTestCase):
    kind = "async"

    def make(self, **kw):
        mem = AsyncClient(KEY, **kw)
        self.addAsyncCleanup(mem.aclose)
        return mem


class SyncRouterCovTest(RouterCovCases, unittest.IsolatedAsyncioTestCase):

    def make(self, **kw):
        mem = Client(KEY, tz=9, user_id="u", **kw)
        self.addCleanup(mem.close)
        return mem


@unittest.skipUnless(HAVE_HTTPX, "httpx not installed (pip install 'wontopos[async]')")
class AsyncRouterCovTest(RouterCovCases, unittest.IsolatedAsyncioTestCase):

    def make(self, **kw):
        mem = AsyncClient(KEY, tz=9, user_id="u", **kw)
        self.addAsyncCleanup(mem.aclose)
        return mem
