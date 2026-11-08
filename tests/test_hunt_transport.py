import json
import os
import socket
import sys
import threading
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
