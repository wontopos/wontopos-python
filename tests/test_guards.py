import gzip
import json
import os
import socket
import sys
import types
import unittest
import warnings
import zlib
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import requests  # noqa: E402

import wontopos  # noqa: E402
from wontopos import (  # noqa: E402
    APIConnectionError,
    AsyncClient,
    Client,
    ModelCall,
    ModelCallError,
    WosError,
    anthropic_caller,
    openai_caller,
)
from test_client import CLOCK, HAVE_HTTPX, KEY, peak_rss_mb, scripted_server, trickle_server, zeros_gzip  # noqa: E402

LLM_KEY = "sk-ant-test-guards-000000000000000000"
CLASSES = (Client, AsyncClient) if HAVE_HTTPX else (Client,)
ANSWER = json.dumps({"content": [{"type": "text", "text": "hi"}], "stop_reason": "end_turn"}).encode()


def model_call():
    return ModelCall("router", "m", ["s"], "u", 10, signal=wontopos._AskClock(30).signal)


class SyncDecompressionTest(unittest.TestCase):

    def make(self, script, **kw):
        srv, base = scripted_server(script)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        mem = Client(KEY, base_url=base, **kw)
        self.addCleanup(mem.close)
        return srv, mem

    def test_stacked_gzip_is_refused_without_inflating_it(self):
        bomb = gzip.compress(zeros_gzip(256 << 20))
        _, mem = self.make([(200, {"Content-Encoding": "gzip, gzip"}, bomb)], retries=0)
        before = peak_rss_mb()
        with self.assertRaises(WosError) as cm:
            mem.stats("alice")
        grew = peak_rss_mb() - before
        self.assertLess(grew, 64, f"peak RSS grew {grew:.0f}MB")
        self.assertIn("stacked content encodings", str(cm.exception))

    def test_one_gzip_layer_stops_at_the_cap(self):
        old = wontopos._MAX_RESPONSE_BYTES
        wontopos._MAX_RESPONSE_BYTES = 1 << 20
        try:
            _, mem = self.make([(200, {"Content-Encoding": "gzip"}, zeros_gzip(128 << 20))], retries=0)
            before = peak_rss_mb()
            with self.assertRaises(WosError) as cm:
                mem.stats("alice")
            grew = peak_rss_mb() - before
        finally:
            wontopos._MAX_RESPONSE_BYTES = old
        self.assertIn("too large", str(cm.exception))
        self.assertLess(grew, 32, f"peak RSS grew {grew:.0f}MB")

    def test_gzip_and_deflate_bodies_decode(self):
        raw = zlib.compressobj(6, zlib.DEFLATED, -15)
        raw_deflate = raw.compress(b'{"total_memories": 3}') + raw.flush()
        srv, mem = self.make([
            (200, {"Content-Encoding": "gzip"}, gzip.compress(b'{"total_memories": 1}')),
            (200, {"Content-Encoding": "deflate"}, zlib.compress(b'{"total_memories": 2}')),
            (200, {"Content-Encoding": "deflate"}, raw_deflate),
            (200, {"Content-Encoding": "identity"}, '{"total_memories": 4}'),
        ])
        self.assertEqual([mem.stats("alice")["total_memories"] for _ in range(4)], [1, 2, 3, 4])
        self.assertEqual(srv.seen[0]["headers"].get("Accept-Encoding"), "gzip, deflate")

    def test_an_unknown_encoding_is_refused(self):
        _, mem = self.make([(200, {"Content-Encoding": "br"}, "xxxx")], retries=0)
        with self.assertRaises(WosError) as cm:
            mem.stats("alice")
        self.assertIn("unsupported response content encoding 'br'", str(cm.exception))


class ProviderDecompressionTest(unittest.IsolatedAsyncioTestCase):

    def caller(self, script):
        srv, base = scripted_server(script)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return srv, anthropic_caller(LLM_KEY, base_url=base)

    async def ask(self, caller, is_async):
        return await caller._acall(model_call()) if is_async else caller.call(model_call())

    async def test_stacked_gzip_from_a_provider_is_refused(self):
        for is_async in ((False, True) if HAVE_HTTPX else (False,)):
            with self.subTest(is_async=is_async):
                _, caller = self.caller([(200, {"Content-Encoding": "gzip, gzip"}, gzip.compress(gzip.compress(ANSWER)))])
                with self.assertRaises(ModelCallError) as cm:
                    await self.ask(caller, is_async)
                self.assertIn("the provider's answer has stacked content encodings ('gzip, gzip')", str(cm.exception))

    async def test_one_layer_decodes_and_a_bomb_stops_at_the_cap(self):
        for is_async in ((False, True) if HAVE_HTTPX else (False,)):
            with self.subTest(is_async=is_async):
                srv, caller = self.caller([
                    (200, {"Content-Encoding": "gzip"}, gzip.compress(ANSWER)),
                    (200, {"Content-Encoding": "deflate"}, zlib.compress(ANSWER)),
                    (200, {"Content-Encoding": "gzip"}, zeros_gzip(64 << 20)),
                    (200, {"Content-Encoding": "br"}, "xxxx"),
                ])
                self.assertEqual((await self.ask(caller, is_async))["text"], "hi")
                self.assertEqual((await self.ask(caller, is_async))["text"], "hi")
                before = peak_rss_mb()
                with self.assertRaises(ModelCallError) as cm:
                    await self.ask(caller, is_async)
                grew = peak_rss_mb() - before
                self.assertIn("too large", str(cm.exception))
                self.assertLess(grew, 48, f"peak RSS grew {grew:.0f}MB")
                with self.assertRaises(ModelCallError) as cm:
                    await self.ask(caller, is_async)
                self.assertIn("the provider's answer has an unsupported content encoding 'br'", str(cm.exception))
                self.assertEqual(srv.seen[0]["headers"].get("accept-encoding"), "gzip, deflate")


class SocksWatchdogTest(unittest.TestCase):

    def test_a_socks_proxy_still_ends_the_read_at_the_deadline(self):
        fake = types.ModuleType("socks")
        fake.PROXY_TYPE_SOCKS4, fake.PROXY_TYPE_SOCKS5 = 1, 2
        fake.ProxyError = type("ProxyError", (OSError,), {"socket_err": None})
        fake.create_connection = lambda dest, timeout=None, **kw: socket.create_connection(dest, timeout)
        self.addCleanup(getattr(wontopos._http, "_WATCHED_SOCKS_POOLS", {}).clear)
        body = b'{"models": []' + b" " * 80 + b"}"
        head = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n")
        _, base = trickle_server(self, head, body, interval=0.05)
        with mock.patch.dict(sys.modules, {"socks": fake}):
            sys.modules.pop("urllib3.contrib.socks", None)
            from urllib3.contrib import socks as u3_socks
            with mock.patch.object(requests.adapters, "SOCKSProxyManager", u3_socks.SOCKSProxyManager):
                mem = Client(KEY, base_url=base, deadline=0.5, retries=0)
                self.addCleanup(mem.close)
                mem._session.proxies = {"http": "socks5://127.0.0.1:9"}
                t0 = CLOCK.monotonic()
                with self.assertRaises(APIConnectionError):
                    mem.list_models()
                spent = CLOCK.monotonic() - t0
        self.assertLess(spent, 2.5, f"the read ran {spent:.1f}s past a 0.5s deadline")


class BaseUrlQueryTest(unittest.TestCase):

    BAD = ("https://api.wontopos.com/?region=eu", "https://api.wontopos.com#top", "https://proxy.example.com/wos?token=abc123",
           "https://api.wontopos.com?")

    def test_a_base_url_with_a_query_or_an_anchor_is_refused(self):
        for cls in CLASSES:
            for bad in self.BAD:
                with self.subTest(cls=cls.__name__, url=bad):
                    with self.assertRaises(ValueError) as cm:
                        cls(KEY, base_url=bad)
                    self.assertIn("base_url has a ? query or a # anchor", str(cm.exception))
                    self.assertNotIn("abc123", str(cm.exception))
                    self.assertNotIn("region", str(cm.exception))

    def test_a_provider_base_with_a_query_or_an_anchor_is_refused(self):
        bad = ("https://api.example.com/v1?token=abc123", "https://api.example.com/v1#x", "https://api.example.com?")
        for b in bad:
            for make in (lambda: anthropic_caller(LLM_KEY, base_url=b),
                         lambda: openai_caller(base_url=b, model="m"),
                         lambda: Client(KEY, router={"key": LLM_KEY, "provider_base_url": b})):
                with self.subTest(url=b):
                    with self.assertRaises(ValueError) as cm:
                        make()
                    self.assertIn("the model provider's base URL has a ? query or a # anchor", str(cm.exception))
                    self.assertNotIn("abc123", str(cm.exception))

    def test_a_base_url_refusal_that_prints_the_url_leaves_out_its_query_its_anchor_and_its_userinfo(self):
        said = ("base_url {!r} contains whitespace, a backslash or a control character. "
                "Check for a stray space, newline or paste error.")
        for bad, shown in (("https://me:hunter2@api.wontopos.com/v 1?token=abc123#x", "https://***@api.wontopos.com/v 1?"),
                           ("https://api.wontopos.com/v1?token=abc 123", "https://api.wontopos.com/v1?"),
                           ("https://api.wontopos.com/v1#abc\\123", "https://api.wontopos.com/v1#"),
                           ("https://api.wontopos.com/v1?token=abc\x01123", "https://api.wontopos.com/v1?")):
            for cls in CLASSES:
                with self.subTest(cls=cls.__name__, url=bad):
                    with self.assertRaises(ValueError) as cm:
                        cls(KEY, base_url=bad)
                    self.assertEqual(str(cm.exception), said.format(shown))

        class At:
            def __init__(self, base_url):
                self.base_url = base_url

            def call(self, c):
                return {"text": ""}

        provider = "the model provider's base URL is not an http(s) URL: "
        for bad, shown in (("https://me:hunter2@api.example.com/v 1?token=abc123#x", "https://***@api.example.com/v 1?"),
                           ("https://api.example.com/v1?token=abc 123", "https://api.example.com/v1?"),
                           ("https://api.example.com/v1#abc\\123", "https://api.example.com/v1#")):
            for make in (lambda: Client(KEY, router={"router_caller": At(bad)}),
                         lambda: Client(KEY).sleep("u", llm=At(bad))):
                with self.subTest(url=bad):
                    with self.assertRaises(ValueError) as cm:
                        make()
                    self.assertEqual(str(cm.exception), provider + json.dumps(shown))

    def test_a_password_holding_a_question_mark_or_a_hash_is_masked_before_the_url_is_cut(self):
        said = ("base_url {!r} contains whitespace, a backslash or a control character. "
                "Check for a stray space, newline or paste error.")
        for bad in ("https://me:hun?ter2@api.wontopos.com/v 1", "https://me:hun#ter2@api.wontopos.com/v 1"):
            for cls in CLASSES:
                with self.subTest(cls=cls.__name__, url=bad):
                    with self.assertRaises(ValueError) as cm:
                        cls(KEY, base_url=bad)
                    self.assertEqual(str(cm.exception), said.format("https://***@api.wontopos.com/v 1"))

        class At:
            def __init__(self, base_url):
                self.base_url = base_url

            def call(self, c):
                return {"text": ""}

        provider = "the model provider's base URL is not an http(s) URL: "
        for bad, shown in (("https://me:hun?ter2@api.example.com/v 1", "https://***@api.example.com/v 1"),
                           ("https://me:hun#ter2@api.example.com/v1", "https://***@api.example.com/v1")):
            for make in (lambda: Client(KEY, router={"router_caller": At(bad)}),
                         lambda: Client(KEY).sleep("u", llm=At(bad))):
                with self.subTest(url=bad):
                    with self.assertRaises(ValueError) as cm:
                        make()
                    self.assertEqual(str(cm.exception), provider + json.dumps(shown))

    def test_the_query_refusal_tells_each_caller_the_base_url_it_takes(self):
        said = "the model provider's base URL has a ? query or a # anchor: give "
        for b in ("https://api.openai.com/v1?api-version=1", "https://api.openai.com/v1#chat"):
            with self.subTest(url=b):
                with self.assertRaises(ValueError) as cm:
                    openai_caller(base_url=b, model="m")
                self.assertEqual(str(cm.exception), said + "the base URL without them, e.g. https://api.openai.com/v1.")
                for make in (lambda: anthropic_caller(LLM_KEY, base_url=b),
                             lambda: Client(KEY, router={"key": LLM_KEY, "provider_base_url": b})):
                    with self.assertRaises(ValueError) as cm:
                        make()
                    self.assertEqual(str(cm.exception), said + "the API origin without them, e.g. https://api.anthropic.com.")


class PlainHttpWarningTest(unittest.TestCase):

    def setUp(self):
        wontopos._reset_warning_state()
        self.addCleanup(wontopos._reset_warning_state)

    def said(self, caught, words):
        return [str(c.message) for c in caught if words in str(c.message)]

    def test_the_base_url_warning_comes_once_per_base_url(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            mem = Client(KEY, base_url="http://wos-proxy.example.com")
            mem.with_user("a").with_model("tablet-2").with_deadline(5)
            Client(KEY, base_url="http://wos-proxy.example.com/")
            if HAVE_HTTPX:
                AsyncClient(KEY, base_url="http://wos-proxy.example.com")
            Client(KEY, base_url="http://other.example.com")
        self.assertEqual(len(self.said(caught, "so the API key travels unencrypted")), 2)

    def test_a_model_key_over_plain_http_to_a_remote_host_warns_once_per_base(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            anthropic_caller(LLM_KEY, base_url="http://llm.example.com")
            anthropic_caller(LLM_KEY, base_url="http://llm.example.com/")
            openai_caller(base_url="http://llm.example.com/v1", model="m", api_key=LLM_KEY)
            openai_caller(base_url="http://other.example.com/v1", model="m")
            anthropic_caller(LLM_KEY, base_url="http://127.0.0.1:9")
            openai_caller(base_url="http://localhost:11434/v1", model="m", api_key=LLM_KEY)
            Client(KEY, router={"key": LLM_KEY, "provider_base_url": "http://router.example.com"})
        said = self.said(caught, "model key")
        self.assertEqual(said, [
            "wontopos: the model provider's base URL uses plain HTTP on a non-local host, so the model key travels "
            "unencrypted. Use https://."
        ] * 3)


class KeyMixUpTest(unittest.TestCase):

    PROVIDER_KEY = "sk-ant-api03-guards-000000000000000000"
    WOS_KEY = "wos-test-guards-0000000000000000000000"

    def test_the_two_kinds_of_key_cannot_be_swapped_and_the_error_never_prints_the_key(self):
        for cls in CLASSES:
            with self.subTest(cls=cls.__name__):
                with self.assertRaises(ValueError) as cm:
                    cls(self.PROVIDER_KEY)
                self.assertEqual(str(cm.exception), "api_key starts with sk-, so it is a model provider's key: give the "
                                                    "Wontopos key here (it starts with wos-).")
        for what, make in (
            ("api_key", lambda: anthropic_caller(self.WOS_KEY)),
            ("api_key", lambda: openai_caller(base_url="https://api.example.com/v1", model="m", api_key=self.WOS_KEY)),
            ("router.key", lambda: Client(KEY, router={"key": self.WOS_KEY})),
            ("router.skill_key", lambda: Client(KEY, router={"key": LLM_KEY, "skill_key": self.WOS_KEY})),
        ):
            with self.subTest(what=what):
                with self.assertRaises(ValueError) as cm:
                    make()
                self.assertEqual(str(cm.exception), f"{what} starts with wos-, so it is a Wontopos key: a Wontopos key "
                                                    "never goes to a model provider. Give the provider's own key.")
        own = "own-gateway-key-000000000000000000"
        for cls in CLASSES:
            for what, router in (("router.key", {"key": own}), ("router.skill_key", {"key": LLM_KEY, "skill_key": own})):
                with self.subTest(cls=cls.__name__, what=what):
                    with self.assertRaises(ValueError) as cm:
                        cls(own, router=router)
                    self.assertEqual(str(cm.exception), f"{what} is this client's Wontopos key: a Wontopos key never "
                                                        "goes to a model provider. Give the provider's own key.")


class KeyPrintTest(unittest.TestCase):

    def test_a_key_prints_as_its_last_4_characters_and_only_from_24_characters_up(self):
        cases = (("wos-live-0123456789abcdeWXYZ", "***WXYZ"), ("wos-live-0123456789aWXYZ", "***WXYZ"),
                 ("wos-live-0123456789WXYZ", "***"))
        self.assertEqual([len(k) for k, _ in cases], [28, 24, 23])
        for key, shown in cases:
            provider = key.replace("wos-", "sk-a")
            views = [repr(cls(key)) for cls in CLASSES] + [
                repr(anthropic_caller(provider)),
                repr(openai_caller(base_url="https://api.example.com/v1", model="m", api_key=provider)),
            ]
            for view in views:
                with self.subTest(key=key, view=view):
                    self.assertTrue(view.endswith(f"api_key={shown})") or view.endswith(f"api_key='{shown}')"), view)
                    self.assertNotIn("0123456789", view)

    def test_a_printed_base_url_masks_only_the_userinfo_of_its_authority(self):
        for url, shown in (
            ("https://proxy.example.com?@api.wontopos.com", "https://proxy.example.com?@api.wontopos.com"),
            ("https://proxy.example.com#@api.wontopos.com", "https://proxy.example.com#@api.wontopos.com"),
            ("https://proxy.example.com\\@api.wontopos.com", "https://proxy.example.com\\@api.wontopos.com"),
            ("https://user:pw@proxy.example.com/a@b", "https://***@proxy.example.com/a@b"),
        ):
            self.assertEqual(wontopos._mask_userinfo(url), shown)
        for cls in CLASSES:
            mem = cls(KEY, base_url="https://user:pw@proxy.example.com/team@x")
            self.assertIn("base_url='https://***@proxy.example.com/team@x'", repr(mem))
        with self.assertRaises(ValueError) as cm:
            openai_caller(base_url="https://proxy.example.com\\@api.wontopos.com", model="m")
        self.assertIn('"https://proxy.example.com\\\\@api.wontopos.com"', str(cm.exception))

    def test_a_password_with_a_slash_is_masked_when_the_url_does_not_parse(self):
        bad = "https://user:abc/def@proxy.example"
        for make in (lambda: anthropic_caller(LLM_KEY, base_url=bad), lambda: openai_caller(base_url=bad, model="m")):
            with self.assertRaises(ValueError) as cm:
                make()
            self.assertEqual(str(cm.exception),
                             "the model provider's base URL is not an http(s) URL: \"https://***@proxy.example\"")


class ArgumentCheckTest(unittest.IsolatedAsyncioTestCase):

    def test_a_deadline_timeout_or_retry_count_that_is_not_a_number_is_nan_or_is_negative_is_refused_naming_it(self):
        for cls in CLASSES:
            for kw, said in (
                ({"timeout": -1}, "timeout must be a number of seconds, 0 or more, got -1."),
                ({"timeout": float("nan")}, "timeout must be a number of seconds, 0 or more, got NaN."),
                ({"timeout": "5"}, 'timeout must be a number of seconds, 0 or more, got "5".'),
                ({"deadline": -0.5}, "deadline must be a number of seconds, 0 or more, got -0.5."),
                ({"deadline": True}, "deadline must be a number of seconds, 0 or more, got true."),
                ({"retries": -1}, "retries must be a whole number, 0 or more, got -1."),
                ({"retries": 1.5}, "retries must be a whole number, 0 or more, got 1.5."),
                ({"retries": float("inf")}, "retries must be a whole number, 0 or more, got Infinity."),
                ({"retries": "0"}, 'retries must be a whole number, 0 or more, got "0".'),
                ({"max_retries": float("nan")}, "max_retries must be a whole number, 0 or more, got NaN."),
            ):
                with self.subTest(cls=cls.__name__, kw=kw):
                    with self.assertRaises(ValueError) as cm:
                        cls(KEY, **kw)
                    self.assertEqual(str(cm.exception), said)
        mem = Client(KEY, deadline=5)
        ends_at = CLOCK.monotonic() - 0.25
        with self.assertRaisesRegex(ValueError, r"^deadline must be a number of seconds, 0 or more, got -0\.2\d*\.$"):
            mem.with_deadline(ends_at - CLOCK.monotonic())
        with self.assertRaisesRegex(ValueError, "^timeout must be"):
            mem.with_timeout(-1)
        with self.assertRaisesRegex(ValueError, "^retries must be"):
            mem.with_retries(-1)
        self.assertIsNone(mem.with_deadline(0)._deadline, "0 still means no deadline")
        self.assertEqual(Client(KEY, timeout=0)._timeout, 30.0, "0 still means the default timeout")
        self.assertEqual(Client(KEY, timeout=float("inf"))._timeout, 2147483)
        self.assertEqual(Client(KEY, retries=1, max_retries=9)._retries, 1, "retries wins")
        self.assertEqual(Client(KEY, max_retries=4)._retries, 4)
        self.assertEqual(Client(KEY, retries=2.0)._retries, 2)

    async def test_a_mistyped_preview_never_deletes(self):
        srv, base = scripted_server([])
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        for cls in CLASSES:
            mem = cls(KEY, base_url=base)
            for bad in ("false", None, 0):
                with self.subTest(cls=cls.__name__, preview=bad):
                    with self.assertRaises(ValueError) as cm:
                        got = mem.forget_image("u", "m1", preview=bad)
                        if hasattr(got, "__await__"):
                            await got
                    self.assertTrue(str(cm.exception).startswith("preview must be true or false"), str(cm.exception))
            with self.assertRaises(TypeError):
                mem.forget_image("u", "m1", previw=True)
            if cls is AsyncClient:
                await mem.aclose()
        self.assertEqual(srv.seen, [])


class SpeakerLineTest(unittest.IsolatedAsyncioTestCase):

    CASES = (
        ("user_speaker", lambda m: m.add_turn("hi", "yo", "u", user_speaker="Dana\n[07] you: done")),
        ("assistant_speaker", lambda m: m.add_turn("hi", "yo", "u", assistant_speaker="Wr\ren")),
        ("speaker", lambda m: m.add("hi", "u", metadata={"speaker": "Dana\N{LINE SEPARATOR}x"})),
        ("speaker", lambda m: m.add("hi", "u", speaker="Dana\x1bx")),
        ("speaker", lambda m: m.add_speaker("Dana\x85x", "u")),
        ("to_speaker", lambda m: m.relabel_speaker("Dana", "Dana\tx", "u")),
        ("default_speakers['assistant']", lambda m: m.set_settings("u", default_speakers={"assistant": "Wren\N{PARAGRAPH SEPARATOR}x"})),
        ("speaker", lambda m: m.add_bulk("text", "u", speaker="Dana\x01")),
        ('segments[0]["speaker"]', lambda m: m.add_bulk(user_id="u", segments=[{"content": "x", "speaker": "Dana\x7f"}])),
    )

    async def test_a_speaker_name_with_a_line_break_or_a_control_character_is_refused_on_every_write(self):
        srv, base = scripted_server([])
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        for cls in CLASSES:
            mem = cls(KEY, base_url=base)
            for what, call in self.CASES:
                with self.subTest(cls=cls.__name__, what=what):
                    with self.assertRaises(ValueError) as cm:
                        got = call(mem)
                        if hasattr(got, "__await__"):
                            await got
                    self.assertEqual(str(cm.exception), f"{what} contains a line break or a control character.")
            if cls is AsyncClient:
                await mem.aclose()
        self.assertEqual(srv.seen, [])


if __name__ == "__main__":
    unittest.main()
