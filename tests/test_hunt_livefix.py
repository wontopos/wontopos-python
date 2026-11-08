import inspect
import json
import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos  # noqa: E402
from test_client import HAVE_HTTPX, KEY, scripted_server  # noqa: E402
from test_sleep import Service, paths, serve  # noqa: E402


async def done(x):
    return await x if inspect.isawaitable(x) else x


SEARCH_ANSWER = json.dumps({
    "memories": [{"id": "w1", "content": "The office moves to the fourth floor.", "category": "work"}],
    "self_memories": [
        {"id": "s1", "content": "You like green tea.", "speaker": "me", "category": "general"},
        {"id": "s2", "content": "hello", "speaker": "me", "category": "general"},
    ],
})

NIGHT = [
    {"id": "m-1", "content": "Dana planted roses today.", "created_at": "2026-10-19T09:00:00Z",
     "speaker": "Dana", "tokens": 8},
    {"id": "m-2", "content": "Kai said the roses need water.", "created_at": "2026-10-19T09:01:00Z",
     "speaker": "Kai", "tokens": 9},
]


class Unreadable:

    def __call__(self, c):
        return {"text": "", "stop": "end"}


READ = {"items": [{"kind": "person", "topic": "Dana", "text": "Dana planted roses.", "evidence": ["e1", "e2"]}],
        "mixed": []}


class MergeUnreadable:

    def __call__(self, c):
        return {"text": json.dumps(READ) if "you read part" in c.system[0] else "", "stop": "end"}


class LiveFixCases:

    async def test_a_filtered_search_leaves_out_the_assistants_words_the_filter_did_not_narrow(self):
        srv, mem = self.new([(200, {}, SEARCH_ANSWER), (200, {}, SEARCH_ANSWER)])
        got = await done(mem.search("office", "u", 5, filters={"categories": ["work"]}))
        self.assertEqual([m["id"] for m in got], ["w1"])
        got = await done(mem.search("office", "u", 5))
        self.assertEqual([m["id"] for m in got], ["w1", "s1", "s2"])

    async def test_a_turn_with_both_sides_blank_is_refused_before_anything_is_sent(self):
        srv, mem = self.new([(200, {}, '{"status": "ok"}')])
        for args in (("", ""), ("  ", "\n")):
            with self.assertRaises(ValueError) as cm:
                await done(mem.add_turn(*args, "u"))
            self.assertEqual(str(cm.exception), "user_msg and assistant_msg are both blank: there is no turn to store.")
        with self.assertRaises(ValueError):
            await done(mem.add_turn(user_id="u"))
        self.assertEqual(srv.seen, [])
        await done(mem.add_turn("hello", "", "u"))
        self.assertEqual(json.loads(srv.seen[0]["body"])["user_msg"], "hello")

    async def test_update_refuses_a_blank_old_memory_id_as_the_other_id_calls_do(self):
        srv, mem = self.new([])
        for bad in ("", "   "):
            with self.assertRaises(ValueError) as cm:
                await done(mem.update(bad, "new text", "u"))
            self.assertEqual(str(cm.exception), "old_memory_id is required.")
        with self.assertRaises(ValueError):
            await done(mem.update(user_id="u"))
        self.assertEqual(srv.seen, [])

    async def test_a_sleep_whose_reading_failed_does_not_end_the_last_sleep(self):
        svc = Service()
        svc.give([dict(r) for r in NIGHT])
        mem = self.over(svc)
        res = await done(mem.sleep("u", llm=Unreadable(), assistant="Kai", today="2026-10-20 09:30"))
        self.assertEqual(res["memories"], 2)
        self.assertIn({"call": "unread parts", "parts": [1]}, res["log"])
        self.assertIsNone(res["done"])
        self.assertNotIn("sleep/done", paths(svc))
        self.assertEqual(svc.last, 0, "the next sleep reads these memories again")

    async def test_a_sleep_whose_merge_came_back_unreadable_twice_does_not_end_the_last_sleep(self):
        svc = Service()
        svc.give([dict(r) for r in NIGHT])
        mem = self.over(svc)
        res = await done(mem.sleep("u", llm=MergeUnreadable(), assistant="Kai", today="2026-10-20 09:30"))
        self.assertEqual(len(res["items"]), 1)
        self.assertEqual([x["call"] for x in res["log"] if x.get("failed")], ["merge", "merge shorter"])
        self.assertIsNone(res["done"])
        self.assertNotIn("sleep/done", paths(svc))
        self.assertEqual(svc.last, 0, "the next sleep reads these memories again")


class SyncLiveFixTest(LiveFixCases, unittest.IsolatedAsyncioTestCase):

    def new(self, script):
        srv, base = scripted_server(script)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return srv, wontopos.Client(KEY, base_url=base, retries=0)

    def over(self, svc):
        srv, base = serve(svc)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return wontopos.Client(KEY, base_url=base)


class AsyncLiveFixTest(LiveFixCases, unittest.IsolatedAsyncioTestCase):

    def new(self, script):
        if not HAVE_HTTPX:
            self.skipTest("httpx not installed (pip install 'wontopos[async]')")
        srv, base = scripted_server(script)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        mem = wontopos.AsyncClient(KEY, base_url=base, retries=0)
        self.addAsyncCleanup(mem.aclose)
        return srv, mem

    def over(self, svc):
        if not HAVE_HTTPX:
            self.skipTest("httpx not installed (pip install 'wontopos[async]')")
        srv, base = serve(svc)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        mem = wontopos.AsyncClient(KEY, base_url=base)
        self.addAsyncCleanup(mem.aclose)
        return mem


class LiveFixDocsTest(unittest.TestCase):

    def test_server_error_says_a_501_is_a_call_the_model_does_not_serve(self):
        self.assertIn("501", wontopos.ServerError.__doc__)


if __name__ == "__main__":
    unittest.main()
