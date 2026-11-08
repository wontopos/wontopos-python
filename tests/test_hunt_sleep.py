import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos  # noqa: E402
from wontopos import SleepSettings  # noqa: E402
from test_client import HAVE_HTTPX, KEY  # noqa: E402
from test_sleep import Service, done, serve  # noqa: E402


class HuntSleepCases:

    async def test_today_takes_a_clock_of_hours_0_to_23_and_minutes_0_to_59(self):
        svc, mem = self.new()
        calls = []

        def llm(c):
            calls.append(c)
            return {"text": "{}"}
        for today, want in (("2026-10-20 24:00", "today must be"), ("2026-10-20 23:60", "today must be"),
                            ("2026-10-20 99:99", "today must be"), ("2026-10-20 23:59", "max_tokens must be"),
                            ("2026-10-20 00:00", "max_tokens must be"), ("2026-10-20 19:05", "max_tokens must be")):
            with self.subTest(today=today):
                with self.assertRaises(ValueError) as cm:
                    await done(mem.sleep("u", llm=llm, today=today, max_tokens=0))
                self.assertTrue(str(cm.exception).startswith(want), str(cm.exception))
        self.assertEqual(svc.seen, [])
        self.assertEqual(calls, [])

    async def test_settings_that_are_not_sleep_settings_are_named_by_their_type_not_printed(self):
        svc, mem = self.new()
        settings = {"readers": 2, "token": "SECRET-MARKER-1234"}
        for call in (lambda: mem.sleep("u", llm=lambda c: {"text": "{}"}, settings=settings),
                     lambda: mem.sleep_estimate("u", settings=settings)):
            with self.assertRaises(TypeError) as cm:
                await done(call())
            self.assertTrue(str(cm.exception).startswith("settings must be SleepSettings, got "), str(cm.exception))
            self.assertNotIn("SECRET-MARKER", str(cm.exception))
        self.assertEqual(svc.seen, [])


class SyncHuntSleepTest(HuntSleepCases, unittest.IsolatedAsyncioTestCase):

    def new(self):
        svc = Service()
        srv, base = serve(svc)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return svc, wontopos.Client(KEY, base_url=base)


class AsyncHuntSleepTest(HuntSleepCases, unittest.IsolatedAsyncioTestCase):

    def new(self):
        if not HAVE_HTTPX:
            self.skipTest("httpx not installed (pip install 'wontopos[async]')")
        svc = Service()
        srv, base = serve(svc)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        mem = wontopos.AsyncClient(KEY, base_url=base)
        self.addAsyncCleanup(mem.aclose)
        return svc, mem


class HuntSleepSettingsTest(unittest.TestCase):

    def test_settings_cannot_be_deleted_either(self):
        s = SleepSettings(readers=3)
        for name in SleepSettings.__slots__:
            with self.subTest(name=name):
                with self.assertRaises(AttributeError) as cm:
                    delattr(s, name)
                self.assertEqual(str(cm.exception), "SleepSettings do not change: make new ones.")
        self.assertEqual(s, SleepSettings(readers=3))
        self.assertEqual(s.readers, 3)


if __name__ == "__main__":
    unittest.main()
