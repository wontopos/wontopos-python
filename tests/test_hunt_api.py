import json
import math
import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

from test_client import AsyncMake, SyncMake


def sent(srv, i=0):
    return json.loads(srv.seen[i]["body"])


BAD_TZ = (15, -14.5, math.nan, "9", True)


def tz_calls(mem):
    return (
        ("recall", lambda tz: mem.recall("q", "u", tz=tz)),
        ("recall rooms=False", lambda tz: mem.recall("q", "u", tz=tz, rooms=False)),
        ("engram", lambda tz: mem.engram("e", "q", "u", tz=tz)),
        ("search", lambda tz: mem.search("q", "u", tz=tz)),
        ("search_full extra", lambda tz: mem.search_full("q", "u", extra={"tz": tz})),
        ("search_self", lambda tz: mem.search_self("q", "u", tz=tz)),
        ("open_work", lambda tz: mem.open_work("q", "u", tz=tz)),
        ("work_search", lambda tz: mem.work_search("w", "q", "u", tz=tz)),
    )


class UpdateOldIdTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    BAD = ("", "   ", "\t\n", None, 5)

    def test_update_trims_the_old_id_and_refuses_a_blank_one(self):
        srv, mem = self.make([])
        said = []
        for bad in self.BAD:
            try:
                mem.update(bad, "new", "u")
                said.append(f"{bad!r}: sent")
            except ValueError as e:
                said.append(str(e))
        try:
            mem.update()
            said.append("update(): sent")
        except ValueError as e:
            said.append(str(e))
        self.assertEqual(said, ["old_memory_id is required."] * (len(self.BAD) + 1))
        self.assertEqual(srv.seen, [])
        mem.update(" m1 ", "new", "u")
        self.assertEqual(sent(srv)["old_memory_id"], "m1")

    async def test_async_update_trims_the_old_id_and_refuses_a_blank_one(self):
        srv, mem = self.amake([])
        said = []
        async with mem:
            for bad in self.BAD:
                try:
                    await mem.update(bad, "new", "u")
                    said.append(f"{bad!r}: sent")
                except ValueError as e:
                    said.append(str(e))
            self.assertEqual(said, ["old_memory_id is required."] * len(self.BAD))
            self.assertEqual(srv.seen, [])
            await mem.update(" m1 ", "new", "u")
        self.assertEqual(sent(srv)["old_memory_id"], "m1")


class UpdateNewContentTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    BAD = ("", "   ", None, 5)

    def test_update_refuses_a_blank_new_content_and_sends_the_rest_as_given(self):
        srv, mem = self.make([])
        said = []
        for bad in self.BAD:
            try:
                mem.update("m1", bad, "u")
                said.append(f"{bad!r}: sent")
            except ValueError as e:
                said.append(str(e))
        try:
            mem.update("m1")
            said.append("update('m1'): sent")
        except ValueError as e:
            said.append(str(e))
        self.assertEqual(said, ["new_content is required."] * (len(self.BAD) + 1))
        self.assertEqual(srv.seen, [])
        mem.update("m1", " the fix ", "u")
        self.assertEqual(sent(srv)["new_content"], " the fix ")

    async def test_async_update_refuses_a_blank_new_content_and_sends_the_rest_as_given(self):
        srv, mem = self.amake([])
        said = []
        async with mem:
            for bad in self.BAD:
                try:
                    await mem.update("m1", bad, "u")
                    said.append(f"{bad!r}: sent")
                except ValueError as e:
                    said.append(str(e))
        self.assertEqual(said, ["new_content is required."] * len(self.BAD))
        self.assertEqual(srv.seen, [])


class AddSpeakerTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    BAD = ({"speaker": "  "}, {"metadata": {"speaker": ""}}, {"speaker": "A" * 257})
    SAID = ["speaker is required (a non-blank string).", "speaker is required (a non-blank string).",
            "speaker is at most 256 bytes."]

    def test_add_checks_its_speaker_as_add_bulk_does(self):
        srv, mem = self.make([])
        said = []
        for kw in self.BAD:
            try:
                mem.add("x", "u", **kw)
                said.append(f"{kw!r}: sent")
            except ValueError as e:
                said.append(str(e))
        self.assertEqual(said, self.SAID)
        self.assertEqual(srv.seen, [])
        given = {"speaker": "\t Ana \n", "category": "work"}
        mem.add("x", "u", speaker=" Ana ")
        mem.add("x", "u", metadata=given)
        mem.add_bulk("x", "u", speaker=" Ana ")
        self.assertEqual([sent(srv, i)["metadata"]["speaker"] for i in range(3)], ["Ana"] * 3)
        self.assertEqual(given["speaker"], "\t Ana \n")

    async def test_async_add_checks_its_speaker_as_add_bulk_does(self):
        srv, mem = self.amake([])
        said = []
        async with mem:
            for kw in self.BAD:
                try:
                    await mem.add("x", "u", **kw)
                    said.append(f"{kw!r}: sent")
                except ValueError as e:
                    said.append(str(e))
            self.assertEqual(said, self.SAID)
            self.assertEqual(srv.seen, [])
            await mem.add("x", "u", speaker=" Ana ")
        self.assertEqual(sent(srv)["metadata"]["speaker"], "Ana")


class SpeakerNamesTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    SAID = ["speaker is required (a non-blank string).", "speaker is required (a non-blank string).",
            "speaker is at most 256 bytes.", "speaker is required.", "speaker is required."]

    @staticmethod
    def calls(mem):
        return (lambda: mem.add_speaker("  ", "u"), lambda: mem.add_speaker(None, "u"),
                lambda: mem.add_speaker("A" * 257, "u"), lambda: mem.remove_speaker("  ", "u"),
                lambda: mem.remove_speaker(None, "u"))

    def test_add_and_remove_speaker_trim_the_name_and_refuse_a_blank_one(self):
        srv, mem = self.make([])
        said = []
        for call in self.calls(mem):
            try:
                call()
                said.append("sent")
            except ValueError as e:
                said.append(str(e))
        self.assertEqual(said, self.SAID)
        self.assertEqual(srv.seen, [])
        mem.add_speaker(" Ana ", "u")
        mem.remove_speaker(" Ana ", "u")
        self.assertEqual([sent(srv, i)["speaker"] for i in range(2)], ["Ana", "Ana"])

    async def test_async_add_and_remove_speaker_trim_the_name_and_refuse_a_blank_one(self):
        srv, mem = self.amake([])
        said = []
        async with mem:
            for call in self.calls(mem):
                try:
                    await call()
                    said.append("sent")
                except ValueError as e:
                    said.append(str(e))
            self.assertEqual(said, self.SAID)
            self.assertEqual(srv.seen, [])
            await mem.add_speaker(" Ana ", "u")
        self.assertEqual(sent(srv)["speaker"], "Ana")


class SpeakerCapTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    LONG = "A" * 257
    SAID = ["default_speakers['user'] is at most 256 bytes.", "to_speaker is at most 256 bytes."]

    def calls(self, mem):
        return (lambda: mem.set_settings("u", default_speakers={"user": self.LONG}),
                lambda: mem.relabel_speaker("Ana", self.LONG, "u"))

    def test_a_default_or_relabelled_speaker_is_at_most_256_bytes_as_add_speaker_takes(self):
        srv, mem = self.make([])
        said = []
        for call in self.calls(mem):
            try:
                call()
                said.append("sent")
            except ValueError as e:
                said.append(str(e))
        self.assertEqual(said, self.SAID)
        self.assertEqual(srv.seen, [])
        mem.set_settings("u", default_speakers={"user": " " + "A" * 256 + " "})
        self.assertEqual(sent(srv)["default_speakers"]["user"], "A" * 256)

    async def test_async_a_default_or_relabelled_speaker_is_at_most_256_bytes_as_add_speaker_takes(self):
        srv, mem = self.amake([])
        said = []
        async with mem:
            for call in self.calls(mem):
                try:
                    await call()
                    said.append("sent")
                except ValueError as e:
                    said.append(str(e))
        self.assertEqual(said, self.SAID)
        self.assertEqual(srv.seen, [])


class NumberCursorTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    SCRIPT = [(200, {}, '{"memories": [{"id": "a"}], "next_cursor": 7}'),
              (200, {}, '{"memories": [{"id": "b"}], "next_cursor": "c2"}'),
              (200, {}, '{"memories": [{"id": "c"}], "next_cursor": 0}')]

    def test_an_export_goes_on_past_a_cursor_that_is_a_number(self):
        srv, mem = self.make(self.SCRIPT)
        self.assertEqual([m["id"] for m in mem.export_memories("u")], ["a", "b", "c"])
        self.assertEqual([json.loads(s["body"]).get("cursor") for s in srv.seen], [None, 7, "c2"])

    async def test_async_an_export_goes_on_past_a_cursor_that_is_a_number(self):
        srv, mem = self.amake(self.SCRIPT)
        async with mem:
            self.assertEqual([m["id"] for m in await mem.export_memories("u")], ["a", "b", "c"])
        self.assertEqual([json.loads(s["body"]).get("cursor") for s in srv.seen], [None, 7, "c2"])


class PerCallTzTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):

    def test_a_per_call_tz_is_checked_without_rooms_as_with_them(self):
        srv, mem = self.make([])
        missed = []
        for name, call in tz_calls(mem):
            for bad in BAD_TZ:
                try:
                    call(bad)
                    missed.append(f"{name} {bad!r}: sent")
                except ValueError as e:
                    if "between -14 and 14" not in str(e):
                        missed.append(f"{name} {bad!r}: {e}")
        self.assertEqual(missed, [])
        self.assertEqual(srv.seen, [])
        mem.engram("e", "q", "u", tz=5.5)
        mem.open_work("q", "u", tz=9)
        self.assertEqual(sent(srv, 0)["tz"], 5.5)
        self.assertEqual(sent(srv, 1), {"user_id": "u", "question": "q"})

    async def test_async_a_per_call_tz_is_checked_without_rooms_as_with_them(self):
        srv, mem = self.amake([])
        missed = []
        async with mem:
            for name, call in tz_calls(mem):
                for bad in BAD_TZ:
                    try:
                        await call(bad)
                        missed.append(f"{name} {bad!r}: sent")
                    except ValueError as e:
                        if "between -14 and 14" not in str(e):
                            missed.append(f"{name} {bad!r}: {e}")
        self.assertEqual(missed, [])
        self.assertEqual(srv.seen, [])


class SleepFileListsTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    SCRIPT = [(200, {}, '{"files": [null, {"slot": 1}, 3], "core": {"slot": 0}}'),
              (200, {}, '{"versions": [{"version": 2}, "x"]}'),
              (200, {}, '{"written": [{"slot": 1}, null]}')]
    WANT = [{"files": [{"slot": 1}], "core": {"slot": 0}}, {"versions": [{"version": 2}]}, {"written": [{"slot": 1}]},
            {"files": []}, {"versions": []}, {"written": []}]
    WRITE = [{"slot": 1, "base_version": 0, "body": "b"}]

    def test_sleep_file_answers_keep_only_records_and_always_carry_the_list(self):
        _, mem = self.make(self.SCRIPT)
        got = []
        for _ in range(2):
            got += [mem.sleep_files("u"), mem.sleep_file_history(1, "u"), mem.write_sleep_files("s", self.WRITE, "u")]
        self.assertEqual(got, self.WANT)

    async def test_async_sleep_file_answers_keep_only_records_and_always_carry_the_list(self):
        _, mem = self.amake(self.SCRIPT)
        got = []
        async with mem:
            for _ in range(2):
                got += [await mem.sleep_files("u"), await mem.sleep_file_history(1, "u"),
                        await mem.write_sleep_files("s", self.WRITE, "u")]
        self.assertEqual(got, self.WANT)


class RevisionsPageTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    SCRIPT = [(200, {}, '{"revised": 1, "memories": [null, {"id": "a"}, 2, "b"], "has_more": false}'),
              (200, {}, '{"revised": 0, "unrevised": 3}')]
    WANT = [{"revised": 1, "memories": [{"id": "a"}], "has_more": False}, {"revised": 0, "unrevised": 3}]

    def test_a_revisions_page_keeps_only_records_as_every_other_page_does(self):
        _, mem = self.make(self.SCRIPT)
        self.assertEqual([mem.revisions("u", include="revised"), mem.revisions("u")], self.WANT)

    async def test_async_a_revisions_page_keeps_only_records_as_every_other_page_does(self):
        _, mem = self.amake(self.SCRIPT)
        async with mem:
            self.assertEqual([await mem.revisions("u", include="revised"), await mem.revisions("u")], self.WANT)


def verify_calls(mem):
    return (
        ("search", lambda v: mem.search("q", "u", verify=v)),
        ("search_full", lambda v: mem.search_full("q", "u", verify=v)),
        ("search_self", lambda v: mem.search_self("q", "u", verify=v)),
        ("search extra", lambda v: mem.search("q", "u", extra={"verify": v})),
    )


class VerifyRangeTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    BAD = (-1, 4, 10, True, 2.0, "2")

    def test_verify_is_0_to_3_as_max_images_is_0_to_5(self):
        srv, mem = self.make([(200, {}, '{"memories": []}')] * 2)
        missed = []
        for name, call in verify_calls(mem):
            for bad in self.BAD:
                try:
                    call(bad)
                    missed.append(f"{name} {bad!r}: sent")
                except ValueError as e:
                    if "verify must be an integer between 0 and 3" not in str(e):
                        missed.append(f"{name} {bad!r}: {e}")
        self.assertEqual(missed, [])
        self.assertEqual(srv.seen, [])
        mem.search("q", "u", verify=0)
        mem.search("q", "u", verify=3)
        self.assertEqual([sent(srv, i)["verify"] for i in range(2)], [0, 3])

    async def test_async_verify_is_0_to_3_as_max_images_is_0_to_5(self):
        srv, mem = self.amake([])
        missed = []
        async with mem:
            for name, call in verify_calls(mem):
                for bad in self.BAD:
                    try:
                        await call(bad)
                        missed.append(f"{name} {bad!r}: sent")
                    except ValueError as e:
                        if "verify must be an integer between 0 and 3" not in str(e):
                            missed.append(f"{name} {bad!r}: {e}")
        self.assertEqual(missed, [])
        self.assertEqual(srv.seen, [])


class EngramNameTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    BAD = ("", "   ", None, 5)

    def test_engram_trims_its_name_and_refuses_a_blank_one(self):
        srv, mem = self.make([])
        said = []
        for bad in self.BAD:
            try:
                mem.engram(bad, "q", "u")
                said.append(f"{bad!r}: sent")
            except ValueError as e:
                said.append(str(e))
        self.assertEqual(said, ["name is required."] * len(self.BAD))
        self.assertEqual(srv.seen, [])
        mem.engram(" timeline ", "q", "u")
        self.assertEqual(sent(srv)["name"], "timeline")

    async def test_async_engram_trims_its_name_and_refuses_a_blank_one(self):
        srv, mem = self.amake([])
        said = []
        async with mem:
            for bad in self.BAD:
                try:
                    await mem.engram(bad, "q", "u")
                    said.append(f"{bad!r}: sent")
                except ValueError as e:
                    said.append(str(e))
        self.assertEqual(said, ["name is required."] * len(self.BAD))
        self.assertEqual(srv.seen, [])


class MinImportanceTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    BAD = (50, -0.1, 1.5, math.nan, True, "0.5")

    def calls(self, mem):
        return (("search", lambda f: mem.search("q", "u", filters=f)),
                ("search_full", lambda f: mem.search_full("q", "u", filters=f)),
                ("recall", lambda f: mem.recall("q", "u", filters=f)),
                ("search extra", lambda f: mem.search("q", "u", extra={"filters": f})),
                ("search_full extra", lambda f: mem.search_full("q", "u", extra={"filters": f})))

    def test_min_importance_is_0_to_1(self):
        srv, mem = self.make([(200, {}, '{"memories": []}')] * 3)
        missed = []
        for name, call in self.calls(mem):
            for bad in self.BAD:
                try:
                    call({"min_importance": bad})
                    missed.append(f"{name} {bad!r}: sent")
                except ValueError as e:
                    if "filters.min_importance must be a number between 0 and 1" not in str(e):
                        missed.append(f"{name} {bad!r}: {e}")
        self.assertEqual(missed, [])
        self.assertEqual(srv.seen, [])
        for ok in (0, 0.5, 1):
            mem.search("q", "u", filters={"min_importance": ok})
        self.assertEqual([sent(srv, i)["filters"]["min_importance"] for i in range(3)], [0, 0.5, 1])

    async def test_async_min_importance_is_0_to_1(self):
        srv, mem = self.amake([])
        missed = []
        async with mem:
            for name, call in self.calls(mem):
                for bad in self.BAD:
                    try:
                        await call({"min_importance": bad})
                        missed.append(f"{name} {bad!r}: sent")
                    except ValueError as e:
                        if "filters.min_importance must be a number between 0 and 1" not in str(e):
                            missed.append(f"{name} {bad!r}: {e}")
        self.assertEqual(missed, [])
        self.assertEqual(srv.seen, [])


if __name__ == "__main__":
    unittest.main()
