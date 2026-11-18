import json
import os
import re
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))

from wontopos import Client

FIXTURE = os.path.join(_HERE, "parity_refusals.json")
TS_COPY = os.path.join(os.path.dirname(os.path.dirname(_HERE)), "typescript", "tests", "parity-refusals.json")


def snake(s):
    return re.sub(r"(?<=[a-z0-9])([A-Z])", lambda m: "_" + m.group(1).lower(), s)


class RefusalParityTest(unittest.TestCase):
    """What Python and TypeScript refuse before sending, and the words they use, read from one fixture."""

    @classmethod
    def setUpClass(cls):
        with open(FIXTURE, "rb") as f:
            cls.raw = f.read()
        cls.cases = json.loads(cls.raw)["cases"]

    def test_each_refusal_says_what_the_fixture_says(self):
        mem = Client("wos-test-xxxxxxxxxxxxxxxx", base_url="http://127.0.0.1:9", retries=0, timeout=2)
        self.addCleanup(mem.close)
        self.assertGreaterEqual(len(self.cases), 40)
        for c in self.cases:
            with self.subTest(case=c["case"]):
                with self.assertRaises((ValueError, TypeError)) as cm:
                    eval(c["python"], {"mem": mem})
                self.assertEqual(str(cm.exception), c["python_says"])

    def test_the_two_clients_say_the_same_words_but_for_the_idioms_named(self):
        for c in self.cases:
            with self.subTest(case=c["case"]):
                if "differs_by_idiom" in c:
                    self.assertNotEqual(snake(c["typescript_says"]), c["python_says"], "the idiom no longer differs: drop the note")
                else:
                    self.assertEqual(snake(c["typescript_says"]), c["python_says"])

    def test_typescript_reads_the_same_bytes_when_it_is_here(self):
        if not os.path.exists(TS_COPY):
            self.skipTest("the TypeScript tests are not here")
        with open(TS_COPY, "rb") as f:
            self.assertEqual(f.read(), self.raw)


if __name__ == "__main__":
    unittest.main()


class MetadataShapeTest(unittest.TestCase):
    """2.2.53: a metadata that is not a dict is refused before sending, and what `{**metadata}` takes still passes."""

    def test_an_empty_or_false_one_is_refused_where_2_2_52_sent_it_as_none(self):
        from wontopos._checks import _metadata
        for v in ([], "", 0, False):
            with self.assertRaises(ValueError) as e:
                _metadata(v, {})
            self.assertIn("metadata must be a dict", str(e.exception))

    def test_anything_with_keys_and_brackets_passes_as_a_sqlite_row_does(self):
        import sqlite3
        from wontopos._checks import _metadata
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        row = db.execute("select 'kitchen' as place").fetchone()
        self.assertEqual(_metadata(row, {})["place"], "kitchen")
