import json
import math
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos  # noqa: E402
from wontopos import AsyncClient, Client, RateLimitError, WosError  # noqa: E402
from test_client import HAVE_HTTPX, KEY, scripted_server  # noqa: E402

CLASSES = (Client, AsyncClient) if HAVE_HTTPX else (Client,)


async def settled(got):
    return await got if hasattr(got, "__await__") else got


async def drained(it):
    if hasattr(it, "__aiter__"):
        return [m async for m in it]
    return list(it)


def served(test, script):
    srv, base = scripted_server(script)
    test.addCleanup(srv.server_close)
    test.addCleanup(srv.shutdown)
    return srv, base


def sent(srv, i):
    return json.loads(srv.seen[i]["body"])


class SleepFileListsTest(unittest.IsolatedAsyncioTestCase):

    async def test_a_sleep_file_answer_that_leaves_its_list_out_or_mixes_in_non_objects_holds_only_its_objects(self):
        write = {"slot": 1, "base_version": 0, "title": "t", "body": "b"}
        for cls in CLASSES:
            srv, base = served(self, [
                (200, {}, '{"core": {"slot": 0}, "files": [null, 3, {"slot": 1}]}'),
                (200, {}, '{"slot": 2}'),
                (200, {}, '{"written": "x"}'),
            ])
            mem = cls(KEY, base_url=base)
            with self.subTest(cls=cls.__name__):
                self.assertEqual((await settled(mem.sleep_files("u")))["files"], [{"slot": 1}])
                self.assertEqual((await settled(mem.sleep_file_history(2, "u")))["versions"], [])
                self.assertEqual((await settled(mem.write_sleep_files("s1", [write], "u")))["written"], [])
            if cls is AsyncClient:
                await mem.aclose()


class MistypedPreviewTest(unittest.IsolatedAsyncioTestCase):

    async def test_a_preview_of_none_is_refused_and_nothing_is_deleted(self):
        srv, base = served(self, [])
        for cls in CLASSES:
            mem = cls(KEY, base_url=base)
            for call in (lambda: mem.forget_image("u", "m1", preview=None),
                         lambda: mem.forget_speaker("Dana", "u", preview=None)):
                with self.subTest(cls=cls.__name__):
                    with self.assertRaises(ValueError) as cm:
                        await settled(call())
                    self.assertEqual(str(cm.exception), "preview must be true or false, got null.")
            if cls is AsyncClient:
                await mem.aclose()
        self.assertEqual(srv.seen, [])


class AnsweredWithNoNumberTest(unittest.IsolatedAsyncioTestCase):

    async def test_answered_with_no_number_closes_the_work_without_an_answer(self):
        opened = '{"work_id": "w1", "memories": [{"n": 1, "id": "m1", "content": "x"}]}'
        for cls in CLASSES:
            srv, base = served(self, [(200, {}, opened), (200, {}, '{"closed": true}')])
            mem = cls(KEY, base_url=base)
            tools = mem.memory_tools("u", preset="S-2")
            with self.subTest(cls=cls.__name__):
                await settled(tools.begin("q"))
                self.assertEqual(await settled(tools.answered(None)), {"closed": True})
                self.assertEqual(srv.seen[1]["path"], "/api/v1/memory/work/close")
                self.assertEqual(sent(srv, 1), {"user_id": "u", "work_id": "w1"})
                self.assertIsNone(await settled(tools.answered(None)), "nothing is open any more")
            if cls is AsyncClient:
                await mem.aclose()


class PageSizeNameTest(unittest.IsolatedAsyncioTestCase):

    async def test_a_page_size_out_of_range_is_refused_under_its_own_name(self):
        srv, base = served(self, [])
        for cls in CLASSES:
            mem = cls(KEY, base_url=base)
            for call, said in (
                (lambda: drained(mem.iter_memories("u", page_size=0)),
                 "page_size must be an integer between 1 and 500, got 0."),
                (lambda: drained(mem.iter_images("u", page_size=3)),
                 "page_size must be an integer between 5 and 20, got 3."),
                (lambda: mem.export_images("u", page_size=21),
                 "page_size must be an integer between 5 and 20, got 21."),
            ):
                with self.subTest(cls=cls.__name__, said=said):
                    with self.assertRaises(ValueError) as cm:
                        await settled(call())
                    self.assertEqual(str(cm.exception), said)
            if cls is AsyncClient:
                await mem.aclose()
        self.assertEqual(srv.seen, [])


class KeyCheckOrderTest(unittest.TestCase):

    def test_a_key_with_a_stray_newline_a_no_break_space_or_a_control_character_is_refused_for_what_it_holds(self):
        spaced = "api_key contains whitespace - check for a stray newline or paste error"
        for cls in CLASSES:
            for key, said in (
                ("wos-test\nxxxxxxxxxx", spaced),
                ("wos-test\N{NO-BREAK SPACE}xxxxxxxxxx", spaced),
                ("wos-test\x01xxxxxxxxxx", "api_key contains a control character - check for a paste error"),
            ):
                with self.subTest(cls=cls.__name__, key=key):
                    with self.assertRaises(ValueError) as cm:
                        cls(key)
                    self.assertEqual(str(cm.exception), said)
            with self.assertRaises(ValueError) as cm:
                cls("wos-test\x85xxxxxxxxxx")
            self.assertTrue(str(cm.exception).startswith("api_key contains a non-ASCII character ("), str(cm.exception))


class RetryAfterSecondsTest(unittest.IsolatedAsyncioTestCase):

    async def test_a_retry_after_given_as_a_date_is_kept_in_whole_seconds(self):
        for cls in CLASSES:
            when = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True)
            srv, base = served(self, [(429, {"Retry-After": when}, '{"error": "slow down"}')])
            mem = cls(KEY, base_url=base, retries=0)
            with self.subTest(cls=cls.__name__):
                with self.assertRaises(RateLimitError) as cm:
                    await settled(mem.stats("u"))
                got = cm.exception.retry_after
                self.assertEqual(got, math.ceil(got))
                self.assertTrue(28 <= got <= 31, got)
            if cls is AsyncClient:
                await mem.aclose()


class BlankAnswerTest(unittest.IsolatedAsyncioTestCase):

    async def test_an_answer_of_only_whitespace_is_refused_as_an_empty_body(self):
        for cls in CLASSES:
            srv, base = served(self, [(200, {}, "  \n")])
            mem = cls(KEY, base_url=base)
            with self.subTest(cls=cls.__name__):
                with self.assertRaises(WosError) as cm:
                    await settled(mem.stats("u"))
                self.assertEqual(cm.exception.status, 200)
                self.assertTrue(str(cm.exception).startswith("[200] empty response body"), str(cm.exception))
            if cls is AsyncClient:
                await mem.aclose()


class BlankStoreWarningTest(unittest.IsolatedAsyncioTestCase):

    async def test_a_blank_store_id_is_refused_without_a_store_id_warning(self):
        blank = "\N{IDEOGRAPHIC SPACE}"
        srv, base = served(self, [])
        for cls in CLASSES:
            mem = cls(KEY, base_url=base)
            for call in (lambda: mem.delete_store(blank), lambda: mem.delete_all(blank),
                         lambda: mem.forget_speaker("Dana", blank)):
                with self.subTest(cls=cls.__name__):
                    with self.assertRaises(ValueError) as cm:
                        await settled(call())
                    self.assertTrue(str(cm.exception).startswith("user_id is required"), str(cm.exception))
            if cls is AsyncClient:
                await mem.aclose()
        self.assertNotIn(blank, wontopos._warned_store_ids)
        self.assertEqual(srv.seen, [])


if __name__ == "__main__":
    unittest.main()
