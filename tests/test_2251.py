import json
import logging
import os
import sys
import unittest
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import inspect

from test_client import AsyncMake, SyncMake


async def done(x):
    return await x if inspect.isawaitable(x) else x


def sent(srv, i=0):
    return json.loads(srv.seen[i]["body"])


class AddCreatedAtTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    """add takes created_at; the two sides of a turn stay add_turn's."""

    AT = "2024-03-01T10:00:00Z"

    def test_add_sends_created_at_in_metadata_and_says_nothing(self):
        srv, mem = self.make([])
        with self.assertRaises(AssertionError):
            with self.assertLogs("wontopos", level="WARNING"):
                mem.add("we met in Busan", "u", created_at=self.AT)
                mem.add("we met in Busan", "u", metadata={"created_at": self.AT})
                mem.add("we met in Busan", "u", created_at=datetime(2024, 3, 1, 10, tzinfo=timezone.utc))
        self.assertEqual([sent(srv, i)["metadata"] for i in range(3)], [{"created_at": self.AT}] * 3)

    async def test_async_add_sends_created_at_in_metadata(self):
        srv, mem = self.amake([])
        async with mem:
            await mem.add("we met in Busan", "u", created_at=self.AT, speaker="Mina")
        self.assertEqual(sent(srv)["metadata"], {"speaker": "Mina", "created_at": self.AT})

    def test_add_refuses_a_created_at_that_is_not_an_instant(self):
        srv, mem = self.make([])
        for bad in ("2024-03-01", "2024-03-01T10:00:00", "yesterday", "", 5, datetime(2024, 3, 1, 10)):
            with self.assertRaises(ValueError, msg=repr(bad)) as e:
                mem.add("x", "u", created_at=bad)
            self.assertIn("created_at must", str(e.exception))
        with self.assertRaises(ValueError):
            mem.add("x", "u", metadata={"created_at": "2024-03-01"})
        self.assertEqual(srv.seen, [])

    def test_add_refuses_the_two_sides_of_a_turn(self):
        srv, mem = self.make([])
        for name in ("user_speaker", "assistant_speaker"):
            for call in (lambda: mem.add("x", "u", **{name: "Mina"}), lambda: mem.add("x", "u", metadata={name: "Mina"})):
                with self.assertRaises(ValueError) as e:
                    call()
                self.assertEqual(str(e.exception),
                                 f"{name} is for add_turn: it names a side of a turn. On add, name who said it with speaker=.")
        self.assertEqual(srv.seen, [])

    async def test_async_add_refuses_the_two_sides_of_a_turn(self):
        srv, mem = self.amake([])
        async with mem:
            with self.assertRaises(ValueError):
                await mem.add("x", "u", user_speaker="Mina")
        self.assertEqual(srv.seen, [])


FAR_OPEN = json.dumps({"work_id": "w-1", "memories": [
    {"n": 1, "id": "m-1", "content": "The far day.", "created_at": "9999-12-31T23:59:59Z"},
    {"n": 2, "id": "m-2", "content": "one\x0btwo\x0cthree\x1cfour\x1dfive\x1esix", "created_at": "2024-03-01T10:00:00Z"},
    {"n": 3, "id": "m-3", "content": "Year five.", "created_at": "0005-06-01T10:00:00Z"},
]})
KEPT = "[02] one\n    two\n    three\n    four\n    five\n    six"


class ToolsShowCases:
    """A date past year 9999 is shown whole, and a rare control character does not start a line."""

    async def test_a_far_date_is_whole_and_rare_breaks_are_indented(self):
        srv, mem = self.new([(200, {}, FAR_OPEN)], model="scroll-2")
        tools = mem.memory_tools("u", preset="S-2", dates=True, tz=9)
        self.assertEqual(await done(tools.begin("q")), "[01] The far day.\n" + KEPT + "\n[03] Year five.")
        self.assertEqual(
            await done(tools.run({"name": "memory_timeline", "input": {}})),
            "In the order they happened, oldest first:\n0005-06-01\n19:00 [03] Year five.\n2024-03-01\n19:00 "
            + KEPT + "\n+010000-01-01\n08:59 [01] The far day.")


class SyncToolsShowTest(SyncMake, ToolsShowCases, unittest.IsolatedAsyncioTestCase):
    def new(self, script=(), **kw):
        return self.make(list(script), **kw)


class AsyncToolsShowTest(AsyncMake, ToolsShowCases, unittest.IsolatedAsyncioTestCase):
    def new(self, script=(), **kw):
        srv, mem = self.amake(list(script), **kw)
        self.addAsyncCleanup(mem.aclose)
        return srv, mem


class CloseProviderSessionsTest(unittest.TestCase):
    """close() releases what this client opened: its own pool, and the sessions of the callers its router built from keys."""

    ROUTER = {"key": "sk-ant-test-router-0000000000000000", "skill_key": "sk-ant-test-skills-1111111111111111"}

    def test_close_also_closes_the_callers_the_router_built_from_keys(self):
        import wontopos
        mem = wontopos.Client("wos-test-xxxxxxxxxx", router=dict(self.ROUTER))
        built = mem._router.built
        self.assertEqual(len(built), 2)
        closed = []
        for c in built:
            session = c._sync_session()
            session.close = lambda session=session: closed.append(session)
        opened = [c._session for c in built]
        mem.close()
        self.assertEqual(closed, opened)
        self.assertEqual([c._session for c in built], [None, None])

    def test_a_copy_and_an_apps_own_caller_are_left_open(self):
        import wontopos
        mine = wontopos.anthropic_caller("sk-ant-test-own-222222222222222222")
        mem = wontopos.Client("wos-test-xxxxxxxxxx", router={"router_caller": mine, "skill_key": self.ROUTER["skill_key"]})
        self.assertEqual(len(mem._router.built), 1)
        session = mine._sync_session()
        copy = mem.with_model("scroll-2")
        built = mem._router.built[0]._sync_session()
        copy.close()
        self.assertIs(mem._router.built[0]._session, built, "a copy shares, so it closes nothing")
        mem.close()
        self.assertIs(mine._session, session, "the app's own caller is the app's to close")
        self.assertIsNone(mem._router.built[0]._session)
        session.close()


class SkillListBreaksTest(unittest.TestCase):
    """A skill's input schema cannot break a line of the router's prompt."""

    def test_the_three_breaks_json_leaves_raw_are_written_as_escapes(self):
        import wontopos
        skill = {"name": "lookup", "description": "Looks things up.",
                 "input": {"type": "object", "description": "a\u2028b\u2029c\x85d", "properties": {"q\u2028": {"type": "string"}}}}
        text = wontopos._skill_list([skill])
        self.assertEqual(
            text,
            "The skills, by name:\n- lookup: Looks things up.\n  input: "
            '{"type":"object","description":"a\\u2028b\\u2029c\\u0085d","properties":{"q\\u2028":{"type":"string"}}}')
        self.assertEqual(len(text.splitlines()), 3)


class PackagingTest(unittest.TestCase):
    """What the package says it needs, and what its source archive leaves out."""

    ROOT = os.path.dirname(_HERE)

    def test_urllib3_is_declared_because_it_is_imported(self):
        with open(os.path.join(self.ROOT, "pyproject.toml"), encoding="utf-8") as f:
            text = f.read()
        deps = text[text.index("\ndependencies = ["):].split("]", 1)[0]
        self.assertIn('"urllib3>=1.26.20,<3"', deps)

    def test_the_source_archive_leaves_the_tests_out(self):
        with open(os.path.join(self.ROOT, "MANIFEST.in"), encoding="utf-8") as f:
            self.assertIn("prune tests", f.read().splitlines())


if __name__ == "__main__":
    unittest.main()
