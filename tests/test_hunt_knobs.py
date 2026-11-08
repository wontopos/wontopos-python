import copy
import inspect
import json
import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos  # noqa: E402,F401
from test_client import AsyncMake, SyncMake  # noqa: E402

OPEN = json.dumps({"work_id": "w-1", "steps_left": 5, "deeper_left": 3,
                   "memories": [{"n": 1, "id": "a", "content": "Theo visits Friday."}]})
DEEPER = json.dumps({"work_id": "w-1", "memories": [{"n": 2, "id": "b", "content": "Milo brings the cake."}],
                     "deeper_left": 2})
SEARCH = json.dumps({"work_id": "w-1", "memories": [], "none": True, "steps_left": 4})
TURN = [(200, {}, OPEN), (200, {}, DEEPER), (200, {}, SEARCH)]
PATHS = ["/api/v1/memory/work/open", "/api/v1/memory/work/deeper", "/api/v1/memory/work/search"]
PLAIN = b'{"user_id":"u","question":"what did we plan"}'
OTHERS = b'"filters":{"source_types":["user_said","ai_thought","inferred","system"]}'
DIVERSITIES = ("none", "mmr", "temporal", "full")


async def done(x):
    return await x if inspect.isawaitable(x) else x


def refused(name, bad):
    shown = json.dumps(bad, separators=(",", ":"))
    if name == "diversity":
        return f'diversity must be "none", "mmr", "temporal" or "full", got {shown}.'
    return f"{name} must be true or false, got {shown}."


def plus(*parts):
    return PLAIN[:-1] + b"".join(b"," + p for p in parts) + b"}"


class HuntKnobsCases:

    async def turn(self, **settings):
        srv, mem = self.new(TURN, tz=9, model="scroll-2")
        tools = mem.memory_tools("u", preset="S-2", **settings)
        await done(tools.begin("what did we plan"))
        await done(tools.run({"name": "memory_deeper", "input": {}}))
        await done(tools.run({"name": "memory_search", "input": {"query": "the cake"}}))
        self.assertEqual([s["path"] for s in srv.seen], PATHS)
        return [s["body"] for s in srv.seen]

    async def test_defaults_open_the_work_with_the_same_bytes_as_before(self):
        plain = await self.turn()
        self.assertEqual(plain[0], PLAIN)
        self.assertEqual(await self.turn(own_words=True, diversity=None), plain)
        srv, mem = self.new([(200, {}, OPEN)], tz=9, model="scroll-2")
        await done(mem.memory_tools("u", preset="S-2", rooms="all", images=True).begin("what did we plan"))
        self.assertEqual(srv.seen[0]["body"],
                         b'{"user_id":"u","question":"what did we plan","max_images":3,"rooms":"all",'
                         b'"tz_offset_minutes":540}')

    async def test_own_words_off_names_every_source_but_the_assistants_and_later_looks_send_nothing_new(self):
        plain = await self.turn()
        off = await self.turn(own_words=False)
        self.assertEqual(off[0], plus(OTHERS))
        self.assertEqual(json.loads(off[0])["filters"],
                         {"source_types": ["user_said", "ai_thought", "inferred", "system"]})
        self.assertEqual(off[1:], plain[1:])

    async def test_each_diversity_goes_through_and_anything_else_is_refused_before_a_call(self):
        plain = await self.turn()
        for d in DIVERSITIES:
            sent = await self.turn(diversity=d)
            self.assertEqual(sent[0], plus(b'"diversity":"' + d.encode() + b'"'), d)
            self.assertEqual(sent[1:], plain[1:], d)
        srv, mem = self.new(model="scroll-2")
        for bad in ("nope", "MMR", " mmr", "", 3, True, ["mmr"], {"mode": "mmr"}):
            with self.assertRaises(ValueError) as cm:
                mem.memory_tools("u", diversity=bad)
            self.assertEqual(str(cm.exception), refused("diversity", bad))
        for bad in (0, "no"):
            with self.assertRaises(ValueError) as cm:
                mem.memory_tools("u", own_words=bad)
            self.assertEqual(str(cm.exception), refused("own_words", bad))
        self.assertEqual(srv.seen, [])

    async def test_both_together(self):
        plain = await self.turn()
        both = await self.turn(own_words=False, diversity="temporal")
        self.assertEqual(both[0], plus(OTHERS, b'"diversity":"temporal"'))
        self.assertEqual(both[1:], plain[1:])

    async def test_open_work_takes_both_as_the_memory_tools_do(self):
        srv, mem = self.new([(200, {}, OPEN)] * 3, model="scroll-2")
        await done(mem.open_work("what did we plan", "u", own_words=False, diversity="temporal"))
        await done(mem.open_work("what did we plan", "u", own_words=True))
        await done(mem.open_work("what did we plan", "u"))
        self.assertEqual([s["body"] for s in srv.seen], [plus(OTHERS, b'"diversity":"temporal"'), PLAIN, PLAIN])
        srv, mem = self.new(model="scroll-2")
        for bad in ("nope", "MMR", " mmr", "", 3, True, ["mmr"], {"mode": "mmr"}):
            with self.assertRaises(ValueError) as cm:
                await done(mem.open_work("q", "u", diversity=bad))
            self.assertEqual(str(cm.exception), refused("diversity", bad))
        for bad in (0, "no"):
            with self.assertRaises(ValueError) as cm:
                await done(mem.open_work("q", "u", own_words=bad))
            self.assertEqual(str(cm.exception), refused("own_words", bad))
        self.assertEqual(srv.seen, [])

    async def test_a_copy_and_tools_on_another_models_client_keep_them(self):
        want = plus(b'"max_images":3', OTHERS, b'"diversity":"mmr"')
        srv, mem = self.new([(200, {}, OPEN)] * 3, tz=9, model="scroll-2")
        tools = mem.memory_tools("u", preset="S-2", images=True, own_words=False, diversity="mmr")
        for t in (tools, copy.copy(tools), copy.deepcopy(tools)):
            await done(t.begin("what did we plan"))
        self.assertEqual([s["body"] for s in srv.seen], [want] * 3)
        srv, mem = self.new([(200, {}, OPEN)], tz=9, model="tablet-2")
        tools = mem.with_user("x").memory_tools("u", preset="S-2", images=True, own_words=False, diversity="mmr")
        await done(tools.begin("what did we plan"))
        self.assertEqual(srv.seen[0]["body"], want)
        self.assertEqual(srv.seen[0]["headers"]["X-WOS-Model"], "scroll-2")

    async def test_nothing_the_model_sees_changes(self):
        srv, mem = self.new(model="scroll-2")
        for flags in ({}, {"speakers": True, "dates": True, "images": True, "seen": True, "usage": True}):
            plain = mem.memory_tools("u", **flags)
            for settings in ({"own_words": False}, {"diversity": "full"}, {"own_words": False, "diversity": "none"}):
                tools = mem.memory_tools("u", **flags, **settings)
                self.assertEqual(tools.note, plain.note)
                for fmt in ("anthropic", "openai"):
                    self.assertEqual(tools.definitions(fmt), plain.definitions(fmt))
        self.assertEqual(srv.seen, [])


class SyncHuntKnobsTest(SyncMake, HuntKnobsCases, unittest.IsolatedAsyncioTestCase):
    def new(self, script=(), **kw):
        return self.make(list(script), **kw)


class AsyncHuntKnobsTest(AsyncMake, HuntKnobsCases, unittest.IsolatedAsyncioTestCase):
    def new(self, script=(), **kw):
        srv, mem = self.amake(list(script), **kw)
        self.addAsyncCleanup(mem.aclose)
        return srv, mem


if __name__ == "__main__":
    unittest.main()
