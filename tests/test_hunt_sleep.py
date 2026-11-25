import json
import os
import sys
import types
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos  # noqa: E402
from wontopos import SleepSettings  # noqa: E402
from test_client import HAVE_HTTPX, KEY  # noqa: E402
from test_sleep import DAYS, Service, done, night_rows, serve, tok  # noqa: E402


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


    async def test_one_merge_answer_makes_the_sleep_ask_at_most_100_times_more(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1]))
        many = "\n\n".join(f"## Matter {i:03d}\n- A note about matter {i}.\n- More.\n- More still.\n- And more.\n- And a fifth."
                             for i in range(150))
        asked = []

        def llm(c):
            system = c.system[0]
            if " you read part " in system:
                return {"text": json.dumps({"items": [{"kind": "work", "topic": "Matters", "text": "Notes were taken.",
                                                       "evidence": ["e1", "e2"]}], "mixed": []})}
            if "lines that are only a note of doubt" in system:
                asked.append("tidy")
                return {"text": '{"drop": []}'}
            if "has grown:" in system:
                asked.append("fold")
                return {"text": '{"text": ""}'}
            return {"text": json.dumps({"edits": [{"file": "1", "op": "remake", "title": "Matters", "text": many,
                                                   "evidence": ["e1", "e2"]}], "proposals": [], "notes": []})}

        res = await done(mem.sleep("u", llm=llm, today="2026-09-15 23:00"))
        self.assertEqual((asked.count("tidy"), asked.count("fold")), (100, 0))
        self.assertEqual([r["why"] for r in res["refused"]],
                         ["the sleep puts at most 100 questions to the model to settle, tidy and fold after the merge; 200 more were not "
                          "asked, and their sections stay as they are"])
        self.assertEqual(svc.latest(1)["body"].count("## Matter "), 150)

    async def test_a_box_the_service_never_has_room_for_is_put_back_and_the_sleep_ends(self):
        svc, mem = self.new()
        core = "## Me\n- I am the assistant.\n\n## Dana\n- Dana likes green tea.\n"
        box = "## Dana\n- Dana moved to Busan.\n\n## Garden\n- The beans went in in May.\n"
        for slot, title, body in ((0, "", core), (1, "People", box)):
            svc.slots[slot].append({"version": 1, "title": title, "body": body, "tokens": tok(body), "evidence": [],
                                    "day": None})
        svc.caps[1] = 1
        counted = []

        def count(text):
            counted.append(text)
            return tok(text) if len(counted) <= 40 else 0

        svc.count = count
        svc.give(night_rows(1, DAYS[1]))
        asked = []

        def llm(c):
            asked.append(c.system[0])
            if " you read part " in c.system[0]:
                return {"text": '{"items": [], "mixed": []}'}
            if "there is no room" in c.system[0]:
                return {"text": '{"text": ""}'}
            return {"text": '{"edits": [], "proposals": [], "notes": []}'}

        res = await done(mem.sleep("u", llm=llm, today="2026-09-15 23:00"))
        writes = [[w["slot"] for w in x["body"]["writes"]] for x in svc.seen if x["path"].endswith("/self/write")]
        self.assertEqual(writes, [[1], [1]])
        self.assertEqual(sum("there is no room" in system for system in asked), 1)
        self.assertEqual(res["refused"][-1], {"edit": {"file": 1, "op": "room"},
                                              "why": "no room: the service counts it over its cap; the file stays as it was"})
        self.assertEqual((svc.latest(1)["body"], len(svc.slots[1])), (box, 1))

    async def test_a_room_made_for_the_service_that_leaves_a_section_out_is_not_used(self):
        svc, mem = self.new()
        core = "## Me\n- I am the assistant.\n\n## Dana\n- Dana likes green tea.\n"
        box = ("## Dana\n- Dana moved to Busan.\n\n## Garden\n- The beans went in in May.\n\n"
               "## Trips\n- Busan in the spring.\n")
        for slot, title, body in ((0, "", core), (1, "People", box)):
            svc.slots[slot].append({"version": 1, "title": title, "body": body, "tokens": tok(body), "evidence": [],
                                    "day": None})
        svc.caps[1] = 30
        counted = []

        def count(text):
            counted.append(text)
            return 2 * tok(text) if len(counted) <= 40 else 0

        svc.count = count
        svc.give(night_rows(1, DAYS[1]))
        shown = []

        def llm(c):
            if " you read part " in c.system[0]:
                return {"text": '{"items": [], "mixed": []}'}
            if "there is no room" in c.system[0]:
                shown.append(c.user)
                return {"text": json.dumps({"text": "## Garden\n- The beans went in in May."})}
            return {"text": '{"edits": [], "proposals": [], "notes": []}'}

        res = await done(mem.sleep("u", llm=llm, today="2026-09-15 23:00"))
        writes = [[w["slot"] for w in x["body"]["writes"]] for x in svc.seen if x["path"].endswith("/self/write")]
        self.assertEqual(writes, [[1], [1]])
        self.assertEqual(len(shown), 1)
        self.assertIn("## Trips", shown[0])
        self.assertEqual(res["refused"][-2:], [
            {"edit": {"file": 1, "op": "room"},
             "why": 'no room: the service counts it over its cap, and the room made left out "Trips"; '
                    "the file stays as it was"},
            {"edit": {"file": 1, "op": "room"},
             "why": "no room: the service counts it over its cap; the file stays as it was"}])
        self.assertEqual(res["rooms"], [{"file": 1, "before": res["rooms"][0]["before"], "after": None}])
        self.assertEqual((svc.latest(1)["body"], len(svc.slots[1])), (box, 1))

    async def test_a_write_the_service_refuses_past_ten_times_ends_the_sleep_with_that_refusal(self):
        svc, mem = self.new()
        stored = {1: "## Garden\n- The beans went in in May.\n", 2: "## Trips\n- Busan in the spring.\n"}
        for slot, body in stored.items():
            svc.slots[slot].append({"version": 1, "title": f"Box {slot}", "body": body, "tokens": tok(body), "evidence": [],
                                    "day": None})
        svc.caps[1] = svc.caps[2] = 1
        counted = []

        def count(text):
            counted.append(text)
            return tok(text) if len(counted) <= 60 else 0

        svc.count = count
        svc.give(night_rows(1, DAYS[1]))

        def llm(c):
            system = c.system[0]
            if " you read part " in system:
                return {"text": json.dumps({"items": [{"kind": "work", "topic": "Notes", "text": "Notes were taken.",
                                                       "evidence": ["e1", "e2"]}], "mixed": []})}
            if "there is no room" in system:
                return {"text": '{"text": ""}'}
            if "lines that are only a note of doubt" in system:
                return {"text": '{"drop": []}'}
            return {"text": json.dumps({"edits": [
                {"file": "1", "op": "rewrite", "section": "Aaa", "text": "- one", "evidence": ["e1", "e2"]},
                {"file": "1", "op": "move_section", "section": "Aaa", "to": "2"},
                {"file": "2", "op": "rewrite", "section": "Bbb", "text": "- two", "evidence": ["e1", "e2"]},
                {"file": "2", "op": "move_section", "section": "Bbb", "to": "1"}], "proposals": [], "notes": []})}

        with self.assertRaises(wontopos.BadRequestError) as cm:
            await done(mem.sleep("u", llm=llm, today="2026-09-15 23:00"))
        self.assertEqual(cm.exception.details, {"code": "self_no_room"})
        writes = [[w["slot"] for w in x["body"]["writes"]] for x in svc.seen if x["path"].endswith("/self/write")]
        self.assertEqual(len(writes), 11, writes)
        self.assertEqual({k: (svc.latest(k)["body"], len(svc.slots[k])) for k in stored},
                         {k: (body, 1) for k, body in stored.items()})
        self.assertNotIn("/api/v1/memory/sleep/done", [x["path"] for x in svc.seen])


class HuntUnreadCases:
    """A part whose answer was cut at its length is read in halves, and a cut answer is not asked for again."""

    SETTINGS = dict(part_tokens=50_000)

    @staticmethod
    def reader(cut=None, cut_when_asked_again=False, garbage=None):
        calls = []

        def llm(c):
            system = c.system[0]
            if " you read part " not in system:
                return {"text": '{"edits": [], "proposals": [], "notes": []}'}
            k = int(system.split(" you read part ", 1)[1].split(" ", 1)[0])
            lines = [ln for ln in c.user.split("<memories>\n", 1)[1].split("\n</memories>", 1)[0].split("\n") if ln]
            calls.append((k, len(lines)))
            again = c.user.endswith("Answer with the JSON object only.")
            if cut is not None and cut(k, len(lines)) and again == cut_when_asked_again:
                return {"text": '{"items": [{"kind": "work", "topic": "Nigh', "stop": "max_tokens"}
            if cut is not None and cut(k, len(lines)):
                return {"text": "I cannot answer in JSON tonight.", "stop": "end"}
            if garbage is not None and garbage(k, len(lines), lines[0].split(" ", 1)[0]):
                return {"text": "I cannot answer in JSON tonight.", "stop": "end"}
            first, second = lines[0].split(" ", 1)[0], lines[min(1, len(lines) - 1)].split(" ", 1)[0]
            return {"text": json.dumps({"items": [{"kind": "work", "topic": f"Night {k}", "text": "Lines were said.",
                                                   "evidence": [first, second]}], "mixed": []})}
        llm.calls = calls
        return llm

    async def night(self, mem, llm, day):
        return await done(mem.sleep("u", llm=llm, today=f"{day} 23:00", settings=SleepSettings(**self.SETTINGS)))

    async def test_an_answer_cut_at_its_length_is_not_asked_again_and_its_part_is_read_in_halves(self):
        svc, mem = self.new()
        rows = night_rows(0, DAYS[0])
        svc.give(rows)
        whole = {}

        def cut(k, lines):
            whole.setdefault(k, lines)
            return k == 2 and lines == whole[2]

        llm = self.reader(cut=cut)
        res = await self.night(mem, llm, DAYS[0])
        of_two = [n for k, n in llm.calls if k == 2]
        self.assertEqual(len(of_two), 3, "the cut answer is not asked again; each half is asked once")
        self.assertEqual(of_two[1] + of_two[2], of_two[0])
        self.assertIn({"call": "unread parts", "parts": []}, res["log"])
        self.assertIsNotNone(res["done"])
        self.assertIn({"call": "read 2", "cut": True, "halves": [of_two[1], of_two[2]]}, res["log"])
        self.assertEqual([x["call"] for x in res["log"] if x.get("call", "").startswith("read 2") and "attempt" in x],
                         ["read 2", "read 2a", "read 2b"])
        self.assertEqual(len(res["items"]), res["readers"] + 1, "both halves gave their item")
        first = [int(it["evidence"][0][1:]) for it in res["items"]]
        self.assertEqual(first, sorted(first), "the first half's item comes before the second half's")

    async def test_a_half_that_cannot_be_read_leaves_its_part_unread_after_one_try(self):
        for second in (True, False):
            with self.subTest(unreadable="second half" if second else "first half"):
                svc, mem = self.new()
                svc.give(night_rows(0, DAYS[0]))
                whole, firsts = {}, {}

                def cut(k, lines):
                    whole.setdefault(k, lines)
                    return k == 2 and lines == whole[2]

                def garbage(k, lines, first):
                    firsts.setdefault(k, first)
                    return k == 2 and lines < whole[2] and (first != firsts[2]) == second

                llm = self.reader(cut=cut, garbage=garbage)
                res = await self.night(mem, llm, DAYS[0])
                of_two = [n for k, n in llm.calls if k == 2]
                a, b = res["log"][[x.get("cut") for x in res["log"]].index(True)]["halves"]
                self.assertEqual(of_two, [a + b, a, b, b] if second else [a + b, a, a, b],
                                 "a piece is tried once: one ask, and one more when the answer is not JSON")
                self.assertIn({"call": "unread parts", "parts": [2]}, res["log"])
                self.assertIsNone(res["done"])
                self.assertEqual(len(res["items"]), res["readers"], "the half that was read still gives its item")

    async def test_a_part_is_halved_by_its_tokens_not_by_its_count(self):
        svc, mem = self.new()
        rows = night_rows(1, DAYS[1])[:8]
        rows[0]["tokens"] = 5_000
        svc.give(rows)
        whole = {}

        def cut(k, lines):
            whole.setdefault(k, lines)
            return lines == whole[k]

        res = await self.night(mem, self.reader(cut=cut), DAYS[1])
        self.assertIn({"call": "read 1", "cut": True, "halves": [2, 6]}, res["log"])
        self.assertIn({"call": "unread parts", "parts": []}, res["log"])

    async def test_a_part_cut_at_every_size_is_halved_twice_and_asked_seven_times_in_all(self):
        svc, mem = self.new()
        svc.give(night_rows(0, DAYS[0]))
        llm = self.reader(cut=lambda k, lines: k == 2)
        res = await self.night(mem, llm, DAYS[0])
        of_two = [n for k, n in llm.calls if k == 2]
        self.assertEqual(len(of_two), 1 + 2 + 4)
        self.assertEqual(sum(sorted(of_two)[:4]), of_two[0], "the four smallest pieces are the part")
        self.assertGreaterEqual(min(of_two), 2, "no piece of one memory: an item needs two lines")
        self.assertIn({"call": "unread parts", "parts": [2]}, res["log"])
        self.assertIsNone(res["done"])
        self.assertIsNone(svc.notes, "nothing but proposals and marks goes into the notes")

    async def test_an_answer_cut_only_when_asked_again_is_not_halved(self):
        svc, mem = self.new()
        svc.give(night_rows(0, DAYS[0]))
        llm = self.reader(cut=lambda k, lines: k == 2, cut_when_asked_again=True)
        res = await self.night(mem, llm, DAYS[0])
        of_two = [n for k, n in llm.calls if k == 2]
        self.assertEqual(of_two, [of_two[0]] * 6, "three tries of two asks each, the whole part every time")
        self.assertFalse([x for x in res["log"] if x.get("cut")])
        self.assertIn({"call": "unread parts", "parts": [2]}, res["log"])

    async def test_a_part_too_small_to_halve_is_asked_as_before_when_its_answer_is_cut(self):
        svc, mem = self.new()
        svc.give(night_rows(1, DAYS[1])[:3])
        llm = self.reader(cut=lambda k, lines: True)
        res = await self.night(mem, llm, DAYS[1])
        self.assertEqual(llm.calls, [(1, 3)] * 6)
        self.assertFalse([x for x in res["log"] if x.get("cut")])
        self.assertIn({"call": "unread parts", "parts": [1]}, res["log"])

    async def test_a_renumbered_answer_of_a_half_is_logged_under_the_halfs_name(self):
        svc, mem = self.new()
        svc.give(night_rows(0, DAYS[0]))
        whole = {}

        def llm(c):
            system = c.system[0]
            if " you read part " not in system:
                return {"text": '{"edits": [], "proposals": [], "notes": []}'}
            k = int(system.split(" you read part ", 1)[1].split(" ", 1)[0])
            lines = [ln for ln in c.user.split("<memories>\n", 1)[1].split("\n</memories>", 1)[0].split("\n") if ln]
            whole.setdefault(k, len(lines))
            if k == 2 and len(lines) == whole[2]:
                return {"text": '{"items": [', "stop": "max_tokens"}
            return {"text": json.dumps({"items": [{"kind": "work", "topic": f"Night {k}", "text": "Lines were said.",
                                                   "evidence": ["e1", "e2"]}], "mixed": []})}

        res = await self.night(mem, llm, DAYS[0])
        named = [x["call"] for x in res["log"] if x.get("renumbered")]
        self.assertIn("read 2a", named)
        self.assertIn("read 2b", named)
        self.assertNotIn("read 2", named)


class SyncHuntSleepTest(HuntSleepCases, HuntUnreadCases, unittest.IsolatedAsyncioTestCase):

    def new(self):
        svc = Service()
        srv, base = serve(svc)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return svc, wontopos.Client(KEY, base_url=base)


class AsyncHuntSleepTest(HuntSleepCases, HuntUnreadCases, unittest.IsolatedAsyncioTestCase):

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


class HuntSleepNumbersTest(unittest.TestCase):

    def test_a_line_or_row_number_of_thousands_of_digits_is_past_any_line_as_typescript_and_rust_read_it(self):
        from wontopos import sleep

        long = "9" * 5000
        self.assertEqual([sleep._sleep_digits(d) for d in ("0", "007", "12", "\u0663\u0664")], [0, 7, 12, 34])
        self.assertEqual(sleep._sleep_digits(long), sleep._sleep_digits("0" * 5000 + long))
        self.assertGreater(sleep._sleep_digits(long), 10 ** 18)
        self.assertEqual(sleep._sleep_digits("0" * 5000 + "2"), 2)
        self.assertEqual(sleep._sleep_doubt_lines_out("a\nb\nc", [long, "2"], lambda _row: 1), "a\nc")
        got = {"items": [{"kind": "person", "topic": "Dana", "text": "x", "evidence": ["e" + long, "e" + long + "1"]}]}
        self.assertIs(sleep._sleep_renumbered(got, [types.SimpleNamespace(eid="e7")], [], 1), got)

    def test_a_write_answer_that_names_a_slot_of_thousands_of_digits_is_read(self):
        from wontopos import sleep

        writes = [{"slot": 1, "base_version": 3}, {"slot": 2, "base_version": 3}]
        self.assertEqual(sleep._sleep_written({"versions": {"9" * 5000: 7, "1": 5}}, writes),
                         {sleep._SLEEP_PAST_ANY_LINE: 7, 1: 5, 2: 4})

    def test_a_heading_of_hash_marks_alone_keeps_them_as_its_name(self):
        from wontopos import sleep

        self.assertEqual(sleep._sleep_sections("## #\n- a\n\n## ## Me\n- b\n\n## ##\n- c\n\n## \n- d"),
                         [("#", "- a"), ("Me", "- b"), ("##", "- c"), ("", "- d")])


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
