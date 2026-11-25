import io
import json
import os
import socket
import sys
import threading
import time
import unittest
import zlib

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos  # noqa: E402
from wontopos import AsyncClient, Client, WosError  # noqa: E402
from test_client import HAVE_HTTPX, KEY, needs_httpx, scripted_server  # noqa: E402


def raw_server(case, answers):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    seen = []

    def run():
        for answer in answers:
            try:
                sock, _ = srv.accept()
            except OSError:
                return
            try:
                sock.settimeout(5.0)
                data = b""
                while b"\r\n\r\n" not in data:
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                seen.append(data.split(b"\r\n", 1)[0])
                sock.sendall(answer)
            except OSError:
                pass
            finally:
                sock.close()

    threading.Thread(target=run, daemon=True).start()
    case.addCleanup(srv.close)
    return seen, f"http://127.0.0.1:{srv.getsockname()[1]}"


def chunked(status, headers, pieces):
    fields = {"Content-Type": "application/json", "Transfer-Encoding": "chunked", **headers, "Connection": "close"}
    head = f"HTTP/1.1 {status} X\r\n" + "".join(f"{k}: {v}\r\n" for k, v in fields.items()) + "\r\n"
    body = b"".join(f"{len(p):x}\r\n".encode() + p + b"\r\n" for p in pieces)
    return head.encode() + body + b"0\r\n\r\n"


class Make:

    def make(self, script, **kw):
        srv, base = scripted_server(script)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        mem = Client(KEY, base_url=base, **kw)
        self.addCleanup(mem.close)
        return srv, mem

    def amake(self, script, **kw):
        srv, base = scripted_server(script)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return srv, AsyncClient(KEY, base_url=base, **kw)


class ErrorFieldBoundTest(Make, unittest.IsolatedAsyncioTestCase):

    BODY = json.dumps({"error": {"type": "t" * 1000, "message": "bad key", "request_id": "r" * 1000}})
    TYPE_ONLY = json.dumps({"error": {"type": "t" * 1000}})
    CUT_ID = "r" * 256 + "\N{HORIZONTAL ELLIPSIS}(truncated)"
    CUT_TYPE = "t" * 256 + "\N{HORIZONTAL ELLIPSIS}(truncated)"

    def check(self, e, fallback):
        self.assertEqual((e.status, e.request_id, e.type), (401, self.CUT_ID, self.CUT_TYPE))
        self.assertEqual(str(e), f"[401] bad key (request_id: {self.CUT_ID})")
        self.assertEqual((fallback.message, fallback.type), ("t" * 1000, self.CUT_TYPE))

    def test_a_long_request_id_and_type_are_cut_as_typescript_and_rust_cut_them(self):
        _, mem = self.make([(401, {}, self.BODY), (401, {}, self.TYPE_ONLY)])
        with self.assertRaises(wontopos.AuthenticationError) as cm:
            mem.stats("alice")
        with self.assertRaises(wontopos.AuthenticationError) as fb:
            mem.stats("alice")
        self.check(cm.exception, fb.exception)

    @needs_httpx
    async def test_async_a_long_request_id_and_type_are_cut(self):
        _, mem = self.amake([(401, {}, self.BODY), (401, {}, self.TYPE_ONLY)])
        async with mem:
            with self.assertRaises(wontopos.AuthenticationError) as cm:
                await mem.stats("alice")
            with self.assertRaises(wontopos.AuthenticationError) as fb:
                await mem.stats("alice")
        self.check(cm.exception, fb.exception)


class ErrorTextControlsTest(Make, unittest.IsolatedAsyncioTestCase):

    BODY = json.dumps({"error": {"type": "ty\u0085pe", "message": "a\u0085b\u009bc\u2028d\u2029e\x1bf",
                                 "request_id": "r\u009d1"}})

    def check(self, e):
        self.assertEqual((e.status, e.message, e.type, e.request_id), (400, "abcdef", "type", "r1"))

    def test_an_errors_text_loses_the_c1_controls_and_the_unicode_line_separators_too(self):
        _, mem = self.make([(400, {}, self.BODY)], retries=0)
        with self.assertRaises(WosError) as cm:
            mem.stats("alice")
        self.check(cm.exception)

    @needs_httpx
    async def test_async_an_errors_text_loses_the_c1_controls_and_the_unicode_line_separators_too(self):
        _, mem = self.amake([(400, {}, self.BODY)], retries=0)
        async with mem:
            with self.assertRaises(WosError) as cm:
                await mem.stats("alice")
        self.check(cm.exception)


class ImageTypeTest(unittest.TestCase):

    CASES = (("image/png", "image/png"),
             ("IMAGE/JPEG; charset=binary", "image/jpeg"),
             (" image/svg+xml\t", "image/svg+xml"),
             ("text/html; charset=utf-8", "text/html"),
             ("image/x\\..\\..\\Users\\Public\\run.bat", "application/octet-stream"),
             ("image/../../etc", "application/octet-stream"),
             ("image/", "application/octet-stream"),
             ("/png", "application/octet-stream"),
             ("image/.png", "application/octet-stream"),
             ("image/p\u00f1g", "application/octet-stream"),
             ("", "application/octet-stream"),
             ("application/json, image/webp", "image/webp"), ("image/png, ../x", "image/png"),
             ('text/plain; title="a,b"', "text/plain"),
             (None, "application/octet-stream"))

    def test_a_pictures_type_is_one_bare_type_and_subtype_or_octet_stream(self):
        from wontopos import _images

        for given, want in self.CASES:
            with self.subTest(given=given):
                self.assertEqual(_images._media_type(given), want)
        picture = (b"HTTP/1.1 200 X\r\nContent-Type: image/x\\..\\..\\run.bat\r\nContent-Length: 3\r\n"
                   b"Connection: close\r\n\r\nabc")
        named = b"HTTP/1.1 200 X\r\nContent-Type: IMAGE/PNG; q=1\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc"
        _, base = raw_server(self, [picture, named])
        mem = Client(KEY, base_url=base, retries=0)
        self.addCleanup(mem.close)
        got = [mem.get_image("alice", "11111111-1111-1111-1111-111111111111") for _ in range(2)]
        self.assertEqual(got, [(b"abc", "application/octet-stream"), (b"abc", "image/png")])


class KeyCheckOrderTest(unittest.TestCase):

    CASES = (
        ("wos-abc\ndef", "api_key contains whitespace"),
        ("wos-abc\N{NO-BREAK SPACE}def", "api_key contains whitespace"),
        ("wos-abc\x85def", "api_key contains a non-ASCII character"),
        ("wos-abc\x01def", "api_key contains a control character"),
    )

    def test_a_bad_key_names_the_same_fault_as_typescript_and_rust(self):
        for cls in ((Client, AsyncClient) if HAVE_HTTPX else (Client,)):
            for key, want in self.CASES:
                with self.subTest(cls=cls.__name__, key=key):
                    with self.assertRaises(ValueError) as cm:
                        cls(key)
                    self.assertIn(want, str(cm.exception))


class BlankBodyTest(Make, unittest.IsolatedAsyncioTestCase):

    def test_a_blank_answer_is_an_empty_body_not_invalid_json(self):
        _, mem = self.make([(200, {}, "  \r\n")], retries=0)
        with self.assertRaises(WosError) as cm:
            mem.stats("alice")
        self.assertTrue(str(cm.exception).startswith("[200] empty response body "), str(cm.exception))

    @needs_httpx
    async def test_async_a_blank_answer_is_an_empty_body(self):
        _, mem = self.amake([(200, {}, "  \r\n")], retries=0)
        async with mem:
            with self.assertRaises(WosError) as cm:
                await mem.stats("alice")
        self.assertTrue(str(cm.exception).startswith("[200] empty response body "), str(cm.exception))


class DeflateFirstChunkTest(unittest.IsolatedAsyncioTestCase):

    def answers(self):
        body = zlib.compress(b'{"total_memories": 2}')
        raw = zlib.compressobj(6, zlib.DEFLATED, -15)
        bare = raw.compress(b'{"total_memories": 3}') + raw.flush()
        return [chunked(200, {"Content-Encoding": "deflate"}, [body[:1], body[1:]]),
                chunked(200, {"Content-Encoding": "deflate"}, [bare[:1], bare[1:]])]

    def test_a_one_byte_first_chunk_of_a_deflate_answer_still_decodes(self):
        _, base = raw_server(self, self.answers())
        mem = Client(KEY, base_url=base, retries=0)
        self.addCleanup(mem.close)
        self.assertEqual([mem.stats("alice")["total_memories"] for _ in range(2)], [2, 3])

    @needs_httpx
    async def test_async_a_one_byte_first_chunk_of_a_deflate_answer_still_decodes(self):
        _, base = raw_server(self, self.answers())
        async with AsyncClient(KEY, base_url=base, retries=0) as mem:
            self.assertEqual([(await mem.stats("alice"))["total_memories"] for _ in range(2)], [2, 3])


def held_server(case, answer):
    """Sends `answer` and then holds the connection open, so only the client can end the call."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    done = threading.Event()

    def run():
        try:
            sock, _ = srv.accept()
        except OSError:
            return
        try:
            sock.settimeout(5.0)
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                data += chunk
            sock.sendall(answer)
            done.wait(30.0)
        except OSError:
            pass
        finally:
            sock.close()

    threading.Thread(target=run, daemon=True).start()
    case.addCleanup(srv.close)
    case.addCleanup(done.set)
    return f"http://127.0.0.1:{srv.getsockname()[1]}"


class ChunkFramingBoundTest(unittest.IsolatedAsyncioTestCase):

    HEAD = (b"HTTP/1.1 200 X\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n"
            b"Connection: close\r\n\r\n")
    FLOOD = b"A" * (1 << 20)
    CASES = (
        ("a size line that never ends", b""),
        ("a size below zero", b"-1\r\n"),
        ("a trailer line that never ends", b"0\r\nX-Trailer: "),
    )
    CLOSED = "(connection closed before the response completed)"

    def test_chunk_framing_that_hides_bytes_from_the_size_cap_is_refused_at_once(self):
        for name, prefix in self.CASES:
            with self.subTest(case=name):
                base = held_server(self, self.HEAD + prefix + self.FLOOD)
                mem = Client(KEY, base_url=base, retries=0, timeout=3.0)
                self.addCleanup(mem.close)
                with self.assertRaises(wontopos.APIConnectionError) as cm:
                    mem.stats("alice")
                self.assertTrue(str(cm.exception).endswith(self.CLOSED), str(cm.exception))

    @needs_httpx
    async def test_async_chunk_framing_that_hides_bytes_from_the_size_cap_is_refused_at_once(self):
        for name, prefix in self.CASES:
            with self.subTest(case=name):
                base = held_server(self, self.HEAD + prefix + self.FLOOD)
                async with AsyncClient(KEY, base_url=base, retries=0, timeout=3.0) as mem:
                    with self.assertRaises(wontopos.APIConnectionError) as cm:
                        await mem.stats("alice")
                self.assertTrue(str(cm.exception).endswith(self.CLOSED), str(cm.exception))

    def test_a_size_below_zero_is_refused_whatever_follows_it(self):
        for size in (b"-1", b"-5", b"-1;x=1"):
            with self.subTest(size=size):
                base = held_server(self, self.HEAD + size + b"\r\n\r\n5\r\nhello\r\n0\r\n\r\n")
                mem = Client(KEY, base_url=base, retries=0, timeout=3.0)
                self.addCleanup(mem.close)
                with self.assertRaises(wontopos.APIConnectionError) as cm:
                    mem.stats("alice")
                self.assertTrue(str(cm.exception).endswith(self.CLOSED), str(cm.exception))

    def test_the_reader_under_a_chunked_body_refuses_a_long_line_and_a_read_to_the_end(self):
        from urllib3.exceptions import ProtocolError

        limit = wontopos._CHUNK_LINE_MAX
        ok = wontopos._BoundedReader(io.BytesIO(b"1f;ext=1\r\n" + b"x" * 31 + b"\r\n"))
        self.assertEqual((ok.readline(), ok.read(31), ok.readline(limit + 1)), (b"1f;ext=1\r\n", b"x" * 31, b"\r\n"))
        self.assertEqual(wontopos._BoundedReader(io.BytesIO(b"y" * limit)).readline(), b"y" * limit)
        for size in (None, -1, limit + 1, limit * 4):
            with self.subTest(size=size):
                with self.assertRaises(ProtocolError):
                    wontopos._BoundedReader(io.BytesIO(b"y" * (limit + 1))).readline(size)
        for amt in (None, -1):
            with self.subTest(amt=amt):
                with self.assertRaises(ProtocolError):
                    wontopos._BoundedReader(io.BytesIO(b"y" * 8)).read(amt)

    def test_a_chunked_answer_with_extensions_and_a_trailer_still_decodes(self):
        body = (b"8;name=value\r\n{\"total_\r\n" b"d\r\nmemories\": 7}\r\n" b"0\r\nX-Checksum: abc\r\n\r\n")
        _, base = raw_server(self, [self.HEAD + body])
        mem = Client(KEY, base_url=base, retries=0)
        self.addCleanup(mem.close)
        self.assertEqual(mem.stats("alice")["total_memories"], 7)


class ProxyTlsTest(unittest.TestCase):

    def test_a_pool_through_a_proxy_keeps_the_clients_tls_context(self):
        mem = Client(KEY)
        self.addCleanup(mem.close)
        adapter = mem._session.get_adapter("https://api.wontopos.com")
        for proxy in ("http://127.0.0.1:9", "https://127.0.0.1:9"):
            with self.subTest(proxy=proxy):
                pool = adapter.proxy_manager_for(proxy).connection_from_host("api.wontopos.com", 443, scheme="https")
                self.assertIs(pool.conn_kw.get("ssl_context"), wontopos._tls_context())


class BackoffCapTest(unittest.TestCase):

    def test_a_long_retry_budget_keeps_waiting_about_8s(self):
        for attempt in (4, 1023, 1024, 10 ** 6):
            with self.subTest(attempt=attempt):
                self.assertTrue(8.0 <= wontopos._backoff(attempt) <= 8.25)
                self.assertTrue(8.0 <= wontopos._retry_wait(503, True, None, None, attempt) <= 8.25)


class EndlessLockWaitTest(Make, unittest.IsolatedAsyncioTestCase):

    BODY = '{"error":{"type":"conflict_error","message":"busy","retry_after_ms":' + "9" * 400 + "}}"

    def test_a_409_asking_for_an_endless_wait_is_a_conflict(self):
        srv, mem = self.make([(409, {}, self.BODY)])
        with self.assertRaises(wontopos.ConflictError) as cm:
            mem.add("x", "alice")
        self.assertEqual((cm.exception.message, len(srv.seen)), ("busy", 1))

    @needs_httpx
    async def test_async_a_409_asking_for_an_endless_wait_is_a_conflict(self):
        srv, mem = self.amake([(409, {}, self.BODY)])
        async with mem:
            with self.assertRaises(wontopos.ConflictError) as cm:
                await mem.add("x", "alice")
        self.assertEqual((cm.exception.message, len(srv.seen)), ("busy", 1))


class NonFiniteNumberTest(Make, unittest.IsolatedAsyncioTestCase):

    BAD = (float("nan"), float("inf"), float("-inf"))

    def test_a_number_json_cannot_carry_is_refused_before_sending(self):
        srv, mem = self.make([])
        for bad in self.BAD:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    mem.add("x", "alice", metadata={"category": bad})
                with self.assertRaises(ValueError):
                    mem.search("q", "alice", filters={"min_importance": bad})
        self.assertEqual(srv.seen, [])

    @needs_httpx
    async def test_async_a_number_json_cannot_carry_is_refused_before_sending(self):
        srv, mem = self.amake([])
        async with mem:
            for bad in self.BAD:
                with self.assertRaises(ValueError):
                    await mem.add("x", "alice", metadata={"category": bad})
                with self.assertRaises(ValueError):
                    await mem.search("q", "alice", filters={"min_importance": bad})
        self.assertEqual(srv.seen, [])


class NonFiniteAnswerTest(Make, unittest.IsolatedAsyncioTestCase):

    BODIES = ('{"total_memories": NaN}', '{"total_memories": Infinity}', '{"total_memories": -Infinity}',
              '{"total_memories": 1e999}')
    SAID = ("NaN is not JSON", "Infinity is not JSON", "-Infinity is not JSON",
            "1e999 is past the range of a JSON number")

    def test_a_number_json_cannot_carry_in_an_answer_is_refused(self):
        _, mem = self.make([(200, {}, body) for body in self.BODIES] + [(200, {}, '{"total_memories": 2.5}')], retries=0)
        for said in self.SAID:
            with self.subTest(said=said):
                with self.assertRaises(WosError) as cm:
                    mem.stats("alice")
                self.assertEqual(str(cm.exception), f"[200] invalid JSON in response: {said}")
        self.assertEqual(mem.stats("alice"), {"total_memories": 2.5})

    def test_an_integer_past_a_doubles_range_is_refused_and_the_refusal_stays_short(self):
        _, mem = self.make([(200, {}, '{"total_memories": ' + "9" * n + "}") for n in (400, 5000)], retries=0)
        with self.assertRaises(WosError) as cm:
            mem.stats("alice")
        self.assertEqual(str(cm.exception),
                         "[200] invalid JSON in response: " + "9" * 400 + " is past the range of a JSON number")
        with self.assertRaises(WosError) as cm:
            mem.stats("alice")
        self.assertLess(len(str(cm.exception)), 600)
        self.assertNotIn("9" * 513, str(cm.exception))

    @needs_httpx
    async def test_async_a_number_json_cannot_carry_in_an_answer_is_refused(self):
        _, mem = self.amake([(200, {}, body) for body in self.BODIES], retries=0)
        async with mem:
            for said in self.SAID:
                with self.assertRaises(WosError) as cm:
                    await mem.stats("alice")
                self.assertEqual(str(cm.exception), f"[200] invalid JSON in response: {said}")


class ProviderRetryHeaderTest(unittest.TestCase):

    def test_a_providers_retry_after_ms_of_thousands_of_digits_is_read_at_once(self):
        from wontopos import _callers

        started = time.perf_counter()
        for raw, want in (("9" * 16000 + "x", 1.0), ("1500", 1.5), ("2.5e3", 2.5), ("250.", 0.25), ("-1", 1.0)):
            with self.subTest(raw=raw[:12]):
                with self.assertRaises(wontopos.ModelCallError) as cm:
                    _callers._provider_answer(429, {"retry-after-ms": raw}, "{}", lambda *_: "rate_limit")
                self.assertEqual(cm.exception.retry_after, want)
        self.assertLess(time.perf_counter() - started, 1.0)


def held_in(value, secret, depth=0):
    if isinstance(value, str):
        return secret in value
    if isinstance(value, (bytes, bytearray)):
        return secret.encode() in value
    if depth < 6 and isinstance(value, dict):
        return any(held_in(k, secret, depth + 1) or held_in(v, secret, depth + 1) for k, v in value.items())
    if depth < 6 and isinstance(value, (list, tuple, set, frozenset)):
        return any(held_in(v, secret, depth + 1) for v in value)
    return secret in repr(value)


def frames_holding(exc, secret):
    held, tb = [], exc.__traceback__.tb_next
    while tb is not None:
        held += [f"{tb.tb_frame.f_code.co_name}: {name}" for name, value in list(tb.tb_frame.f_locals.items())
                 if held_in(value, secret)]
        tb = tb.tb_next
    return held


class ModelKeyOffTheTracebackTest(unittest.IsolatedAsyncioTestCase):

    MODEL_KEY = "sk-ant-api03-0123456789abcdefMODELKEY"
    BODY = b'{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}'
    REFUSED = (b"HTTP/1.1 401 X\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: "
               + str(len(BODY)).encode() + b"\r\n\r\n" + BODY)

    def caller(self, kind, base):
        if kind == "anthropic":
            return wontopos.anthropic_caller(self.MODEL_KEY, base_url=base)
        return wontopos.openai_caller(base_url=base, model="m", api_key=self.MODEL_KEY)

    def test_a_refused_model_call_leaves_the_key_in_no_frame_of_its_traceback(self):
        for kind in ("anthropic", "openai"):
            with self.subTest(kind=kind):
                seen, base = raw_server(self, [self.REFUSED])
                try:
                    self.caller(kind, base).call(wontopos.ModelCall("router", "m", ["be brief"], "hello", 16))
                except wontopos.ModelCallError as e:
                    self.assertEqual((e.status, len(seen)), (401, 1))
                    self.assertEqual(frames_holding(e, self.MODEL_KEY), [])
                else:
                    self.fail("the provider's 401 was not raised")

    @needs_httpx
    async def test_async_a_refused_model_call_leaves_the_key_in_no_frame_of_its_traceback(self):
        for kind in ("anthropic", "openai"):
            with self.subTest(kind=kind):
                seen, base = raw_server(self, [self.REFUSED])
                try:
                    await self.caller(kind, base)._acall(wontopos.ModelCall("router", "m", ["be brief"], "hello", 16))
                except wontopos.ModelCallError as e:
                    self.assertEqual((e.status, len(seen)), (401, 1))
                    self.assertEqual(frames_holding(e, self.MODEL_KEY), [])
                else:
                    self.fail("the provider's 401 was not raised")


class TzShownTest(unittest.TestCase):

    def test_an_out_of_range_tz_is_shown_as_javascript_shows_it(self):
        for cls in ((Client, AsyncClient) if HAVE_HTTPX else (Client,)):
            for tz, shown in ((float("inf"), "Infinity"), (float("-inf"), "-Infinity"), (1e300, "1e+300")):
                with self.subTest(cls=cls.__name__, tz=tz):
                    with self.assertRaises(ValueError) as cm:
                        cls(KEY, tz=tz)
                    self.assertTrue(str(cm.exception).endswith(f"got {shown}."), str(cm.exception))


if __name__ == "__main__":
    unittest.main()
