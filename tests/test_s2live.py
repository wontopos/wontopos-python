import inspect
import json
import os
import sys
import unittest
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos  # noqa: E402,F401
from test_client import AsyncMake, SyncMake  # noqa: E402

Q = "what did we plan"
OPEN = json.dumps({"work_id": "w-1", "steps_left": 5, "deeper_left": 3,
                   "memories": [{"n": 1, "id": "a", "content": "Theo visits Friday."}]})
DEEPER = json.dumps({"work_id": "w-1", "memories": [{"n": 2, "id": "b", "content": "Milo brings the cake."}],
                     "deeper_left": 2})
SEARCH = json.dumps({"work_id": "w-1", "memories": [], "none": True, "steps_left": 4})
WORK = "/api/v1/memory/work/"
HISTORY = "/api/v1/memory/history"
RESULTS = [
    "[01] Theo visits Friday.\n(steps left 5 \N{MIDDLE DOT} deeper left 3)",
    "[02] Milo brings the cake.\n(deeper left 2)",
    "Nothing new for that.\n(steps left 4)",
]
PLAIN = b'{"user_id":"u","question":"what did we plan"}'
LATER = [b'{"user_id":"u","work_id":"w-1"}', b'{"user_id":"u","work_id":"w-1","query":"the cake"}']
OTHERS = ',"own_words":false'
LIVE = OTHERS + ',"diversity":"full"'


async def done(x):
    return await x if inspect.isawaitable(x) else x


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def history(*lines):
    now = datetime.now(timezone.utc).timestamp() * 1000
    turns = [{"role": "assistant" if i % 2 else "user", "content": text, "timestamp": iso(now - minutes * 60_000)}
             for i, (text, minutes) in enumerate(lines)]
    return json.dumps({"turns": turns})


def asked(question, tail=LIVE):
    return (b'{"user_id":"u","question":' + json.dumps(question).encode() + tail.encode() + b"}")


class S2LiveCases:

    async def turn(self, tools):
        return [await done(tools.begin(Q)),
                await done(tools.run({"name": "memory_deeper", "input": {}})),
                await done(tools.run({"name": "memory_search", "input": {"query": "the cake"}}))]

    async def first_look(self, lines, question=Q, **settings):
        srv, mem = self.new([(200, {}, history(*lines)), (200, {}, OPEN)], tz=9, model="scroll-2")
        self.assertEqual(await done(mem.memory_tools("u", **settings).begin(question)), RESULTS[0])
        self.assertEqual([s["path"] for s in srv.seen], [HISTORY, WORK + "open"])
        self.assertEqual(srv.seen[0]["body"], b'{"user_id":"u"}')
        return srv.seen[1]["body"]

    async def test_s2_named_sends_and_answers_byte_for_byte_as_before(self):
        for model in ("scroll-2", "tablet-2"):
            srv, mem = self.new([(200, {}, OPEN), (200, {}, DEEPER), (200, {}, SEARCH)], tz=9, model=model)
            tools = mem.memory_tools("u", preset="S-2")
            self.assertEqual(tools.preset, "S-2")
            self.assertEqual(await self.turn(tools), RESULTS)
            self.assertEqual([s["path"] for s in srv.seen], [WORK + "open", WORK + "deeper", WORK + "search"])
            self.assertEqual([s["body"] for s in srv.seen], [PLAIN] + LATER)
            self.assertEqual({s["headers"]["X-WOS-Model"] for s in srv.seen}, {"scroll-2"})

    async def test_no_preset_on_scroll_2_is_s2_live(self):
        srv, mem = self.new(model="scroll-2")
        self.assertEqual(mem.memory_tools("u").preset, "S-2-live")
        self.assertEqual(mem.with_model("tablet-2").memory_tools("u", preset="S-2-live").preset, "S-2-live")
        self.assertEqual(await self.first_look([]), asked(Q))
        srv, mem = self.new([(200, {}, history()), (200, {}, OPEN)], model="tablet-2")
        await done(mem.memory_tools("u", preset="S-2-live").begin(Q))
        self.assertEqual([s["headers"]["X-WOS-Model"] for s in srv.seen], ["scroll-2", "scroll-2"])

    async def test_the_first_look_reads_the_last_two_lines_of_the_last_five_minutes(self):
        cases = (
            ((), Q),
            ((("we booked the cabin", 1),), "we booked the cabin\n" + Q),
            ((("we booked the cabin", 2), ("Theo drives up Friday", 1)), "we booked the cabin\nTheo drives up Friday\n" + Q),
            ((("first", 3), ("second", 2), ("third", 1)), "second\nthird\n" + Q),
            ((("last week", 60 * 24 * 7), ("too early", 6), ("just now", 4)), "just now\n" + Q),
            ((("we booked the cabin", 2), ("Theo drives up Friday", 1), (Q, 0)),
             "we booked the cabin\nTheo drives up Friday\n" + Q),
        )
        for lines, want in cases:
            self.assertEqual(await self.first_look(lines), asked(want), lines)

    async def test_a_long_line_is_cut_from_its_start_and_a_long_question_goes_whole_alone(self):
        line = "start " + "x" * 600
        body = await self.first_look([("Theo drives up Friday", 2), (line, 1)])
        sent = json.loads(body)["question"]
        self.assertEqual(sent, line[-(500 - len(Q) - 1):] + "\n" + Q)
        self.assertEqual(len(sent), 500)
        self.assertEqual(body, asked(sent))
        cut = "x" * (500 - len(Q) - 2)
        self.assertEqual(await self.first_look([("start " + cut, 1)]), asked(cut + "\n" + Q))
        question = "y" * 600
        self.assertEqual(await self.first_look([("we booked the cabin", 1)], question), asked(question))

    async def test_a_failed_recent_read_opens_with_the_question_alone(self):
        srv, mem = self.new([(500, {}, '{"error":"down"}'), (200, {}, OPEN)], tz=9, model="scroll-2", retries=0)
        self.assertEqual(await done(mem.memory_tools("u").begin(Q)), RESULTS[0])
        self.assertEqual([s["path"] for s in srv.seen], [HISTORY, WORK + "open"])
        self.assertEqual(srv.seen[1]["body"], asked(Q))

    async def test_the_filter_and_diversity_go_on_the_open_and_later_looks_send_nothing_new(self):
        script = [(200, {}, history(("we booked the cabin", 1))), (200, {}, OPEN), (200, {}, DEEPER), (200, {}, SEARCH)]
        srv, mem = self.new(script, tz=9, model="scroll-2")
        self.assertEqual(await self.turn(mem.memory_tools("u")), RESULTS)
        self.assertEqual([s["path"] for s in srv.seen], [HISTORY, WORK + "open", WORK + "deeper", WORK + "search"])
        opened = json.loads(srv.seen[1]["body"])
        self.assertIs(opened["own_words"], False)
        self.assertNotIn("filters", opened)
        self.assertEqual(opened["diversity"], "full")
        self.assertEqual(srv.seen[1]["body"], asked("we booked the cabin\n" + Q))
        self.assertEqual([s["body"] for s in srv.seen[2:]], LATER)

    async def test_own_words_and_diversity_set_on_top_of_the_preset_win(self):
        for settings, tail in (
            ({"own_words": True}, ',"diversity":"full"'),
            ({"diversity": "mmr"}, OTHERS + ',"diversity":"mmr"'),
            ({"own_words": True, "diversity": "none"}, ',"diversity":"none"'),
            ({"own_words": None, "diversity": None}, LIVE),
        ):
            self.assertEqual(await self.first_look([], **settings), asked(Q, tail), settings)
        for settings, tail in (({"own_words": False}, OTHERS), ({"diversity": "full"}, ',"diversity":"full"'),
                               ({"own_words": None, "diversity": None}, "")):
            srv, mem = self.new([(200, {}, OPEN)], tz=9, model="scroll-2")
            await done(mem.memory_tools("u", preset="S-2", **settings).begin(Q))
            self.assertEqual(srv.seen[0]["body"], asked(Q, tail), settings)

    async def test_refusals_name_both_presets_and_the_model_sees_the_same_tools(self):
        srv, mem = self.new(model="scroll-2")
        with self.assertRaises(ValueError) as cm:
            mem.memory_tools("u", preset="S-3")
        self.assertEqual(str(cm.exception), 'unknown memory tools preset "S-3": the presets are "S-2", "S-2-live".')
        with self.assertRaises(ValueError) as cm:
            mem.with_model("tablet-2").memory_tools("u")
        self.assertEqual(str(cm.exception), 'model "tablet-2" has no memory tools preset: pass preset "S-2" '
                                            '(scroll-2) or "S-2-live" (scroll-2).')
        for flags in ({}, {"speakers": True, "dates": True, "images": True, "seen": True, "usage": True}):
            live, s2 = mem.memory_tools("u", **flags), mem.memory_tools("u", preset="S-2", **flags)
            self.assertEqual(live.note, s2.note)
            for fmt in ("anthropic", "openai"):
                self.assertEqual(live.definitions(fmt), s2.definitions(fmt))
        self.assertEqual(srv.seen, [])


class SyncS2LiveTest(SyncMake, S2LiveCases, unittest.IsolatedAsyncioTestCase):
    def new(self, script=(), **kw):
        return self.make(list(script), **kw)


class AsyncS2LiveTest(AsyncMake, S2LiveCases, unittest.IsolatedAsyncioTestCase):
    def new(self, script=(), **kw):
        srv, mem = self.amake(list(script), **kw)
        self.addAsyncCleanup(mem.aclose)
        return srv, mem


if __name__ == "__main__":
    unittest.main()
