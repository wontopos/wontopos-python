import inspect
import json
import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos  # noqa: E402
from test_client import AsyncMake, SyncMake, envelope  # noqa: E402


async def done(x):
    return await x if inspect.isawaitable(x) else x


NOTHING_OPEN = "Nothing is open: memory tools answer within a turn that began with begin()."
OPEN_TEA = json.dumps({"work_id": "w-1", "memories": [{"n": 1, "id": "m-1", "content": "Tea at nine."}]})
OPEN_COFFEE = json.dumps({"work_id": "w-2", "memories": [{"n": 1, "id": "m-9", "content": "Coffee at ten."}]})
LATE = {"work_id": "w-1", "memories": [{"n": 2, "id": "m-2", "content": "Green tea."}], "deeper_left": 2}


class HuntToolsCases:

    async def test_a_reply_for_a_turn_that_has_ended_is_kept_out_of_the_turn_open_now(self):
        srv, mem = self.new([(200, {}, OPEN_TEA), (200, {}, OPEN_COFFEE)], model="scroll-2")
        tools = mem.memory_tools("u", preset="S-2", dates=True)
        await done(tools.begin("tea"))
        if inspect.iscoroutinefunction(mem.work_deeper):
            async def deeper(*_args, **_kw):
                await tools.begin("coffee")
                return LATE
        else:
            def deeper(*_args, **_kw):
                tools.begin("coffee")
                return LATE
        mem.work_deeper = deeper
        self.assertEqual(await done(tools.run({"name": "memory_deeper", "input": {}})),
                         "[02] Green tea.\n(deeper left 2)")
        self.assertEqual(tools.work_id, "w-2")
        self.assertEqual(await done(tools.run({"name": "memory_timeline", "input": {}})),
                         "In the order they happened, oldest first:\n(no date)\n[01] Coffee at ten.")
        with self.assertRaisesRegex(ValueError, r"^\[02\] is not a number this turn gave\.$"):
            await done(tools.answered(2))
        self.assertEqual(len(srv.seen), 2)

    async def test_a_blank_question_is_refused_before_anything_is_sent(self):
        srv, mem = self.new([], model="scroll-2")
        tools = mem.memory_tools("u", speakers=True, sleep=True)
        for blank in ("", " \t", None):
            with self.subTest(blank=blank), self.assertRaisesRegex(ValueError, r"^question is required\.$"):
                await done(tools.begin(blank))
        self.assertEqual(srv.seen, [])

    async def test_a_turn_that_failed_to_begin_leaves_nothing_open(self):
        closed = envelope("invalid_request_error", "that store is closed", "req_1")
        srv, mem = self.new([(200, {}, OPEN_TEA), (400, {}, closed)], model="scroll-2")
        tools = mem.memory_tools("u", preset="S-2", dates=True)
        await done(tools.begin("tea"))
        with self.assertRaises(wontopos.WosError):
            await done(tools.begin("coffee"))
        self.assertIsNone(tools.work_id)
        self.assertEqual(await done(tools.run({"name": "memory_timeline", "input": {}})), NOTHING_OPEN)
        self.assertIsNone(await done(tools.answered(1)))
        self.assertEqual(len(srv.seen), 2)

    async def test_a_memory_with_an_empty_id_is_neither_a_photo_nor_an_answer(self):
        opened = json.dumps({"work_id": "w-1", "memories": [
            {"n": 1, "id": "", "content": "The beach.", "image_ref": "img-1"},
            {"n": 2, "id": "m-2", "content": "The dunes.", "image_ref": "img-2"}]})
        srv, mem = self.new([(200, {}, opened)], model="scroll-2")
        tools = mem.memory_tools("u", preset="S-2", images=True)
        self.assertEqual(await done(tools.begin("beach")), "[01] The beach.\n[02] (photo) The dunes.")
        self.assertEqual(await done(tools.run({"name": "memory_photo", "input": {"n": 1}})),
                         "Refused: [01] is not a number this turn gave.")
        with self.assertRaisesRegex(ValueError, r"^\[01\] is not a number this turn gave\.$"):
            await done(tools.answered(1))
        self.assertEqual(len(srv.seen), 1)

    async def test_answered_with_none_closes_the_turn_with_no_answer(self):
        srv, mem = self.new([(200, {}, OPEN_TEA), (200, {}, '{"work_id": "w-1", "closed": true}')], model="scroll-2")
        tools = mem.memory_tools("u", preset="S-2")
        await done(tools.begin("tea"))
        self.assertEqual(await done(tools.answered(None)), {"work_id": "w-1", "closed": True})
        self.assertIsNone(tools.work_id)
        self.assertEqual(json.loads(srv.seen[1]["body"]), {"user_id": "u", "work_id": "w-1"})

    async def test_work_id_is_none_when_the_open_reply_names_no_work(self):
        replies = ['{"work_id": 5, "memories": []}', '{"work_id": "", "memories": []}', '{"memories": []}', OPEN_TEA]
        srv, mem = self.new([(200, {}, r) for r in replies], model="scroll-2")
        tools = mem.memory_tools("u", preset="S-2")
        for want in (None, None, None, "w-1"):
            await done(tools.begin("tea"))
            self.assertEqual(tools.work_id, want)

    async def test_a_name_that_reads_as_a_label_once_its_invisible_characters_are_gone_is_quoted(self):
        names = ["you\N{ZERO WIDTH SPACE}", "\N{ZERO WIDTH JOINER}user", "no\N{SOFT HYPHEN}te", "U\N{WORD JOINER}SER",
                 "Yo\N{COMBINING GRAPHEME JOINER}u", "you\N{VARIATION SELECTOR-16}", "\N{ZERO WIDTH SPACE} you",
                 "y\N{TAG SPACE}ou", "Da\N{ZERO WIDTH SPACE}na"]
        opened = json.dumps({"work_id": "w-1", "memories": [
            {"n": i + 1, "id": f"m-{i + 1}", "content": "Paid.", "speaker": s} for i, s in enumerate(names)]})
        history = json.dumps({"turns": [{"role": "user", "speaker": s, "content": "Paid."} for s in names]})
        srv, mem = self.new([(200, {}, opened), (200, {}, history)], model="scroll-2")
        tools = mem.memory_tools("u", preset="S-2", speakers=True, seen=True, assistant="Kai")
        shown = [f'"{s}"' for s in names[:-1]] + [names[-1]]
        self.assertEqual(await done(tools.begin("paid")),
                         "\n".join(f"[{i + 1:02d}] {s}: Paid." for i, s in enumerate(shown)))
        self.assertEqual(await done(tools.run({"name": "memory_recent", "input": {}})),
                         "The recent conversation, newest first:\n"
                         + "\n".join(f"#{i + 1} {s}: Paid." for i, s in enumerate(reversed(shown))))

    async def test_each_schema_in_the_definitions_is_its_own_object(self):
        srv, mem = self.new([], model="scroll-2")
        for format in ("anthropic", "openai"):
            defs = mem.memory_tools("u").definitions(format)
            schema = defs[1]["input_schema"] if format == "anthropic" else defs[1]["function"]["parameters"]
            schema["properties"]["from"]["description"] = "Where to start."
            self.assertEqual([k for k, v in schema["properties"].items() if "description" in v], ["from"])


class SyncHuntToolsTest(SyncMake, HuntToolsCases, unittest.IsolatedAsyncioTestCase):
    def new(self, script=(), **kw):
        return self.make(list(script), **kw)


class AsyncHuntToolsTest(AsyncMake, HuntToolsCases, unittest.IsolatedAsyncioTestCase):
    def new(self, script=(), **kw):
        srv, mem = self.amake(list(script), **kw)
        self.addAsyncCleanup(mem.aclose)
        return srv, mem
