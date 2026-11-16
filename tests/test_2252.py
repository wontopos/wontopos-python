import os
import socket
import sys
import threading
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "src"))
sys.path.insert(0, _HERE)

import wontopos
from wontopos import AsyncClient, Client, WosError

from test_client import HAVE_HTTPX, KEY

HEAD = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
        b"X-RateLimit-Limit: 150\r\nX-RateLimit-Remaining: 7\r\nX-RateLimit-Reset: 1720000000\r\n"
        b"Sunset: Tue, 01 Jun 2100 00:00:00 GMT\r\n"
        b"Content-Length: 100\r\nConnection: close\r\n\r\n{")


def cut_body_server(test):
    """One answer whose headers arrive and whose body stops after one byte."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(2)

    def run():
        try:
            sock, _ = srv.accept()
        except OSError:
            return
        try:
            sock.settimeout(2.0)
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                data += chunk
            sock.sendall(HEAD)
        except OSError:
            pass
        finally:
            sock.close()
            srv.close()

    threading.Thread(target=run, daemon=True).start()
    test.addCleanup(srv.close)
    return f"http://127.0.0.1:{srv.getsockname()[1]}"


class HeadersReadOnArrivalTest(unittest.IsolatedAsyncioTestCase):
    """The rate limit and a model's retirement are read when the headers arrive, as TypeScript and Rust read them."""

    def setUp(self):
        wontopos._warned_retirements.clear()
        self.addCleanup(wontopos._warned_retirements.clear)

    def test_a_body_that_never_finishes_still_leaves_the_rate_limit_and_the_retirement_warning(self):
        mem = Client(KEY, base_url=cut_body_server(self), retries=0, model="tablet-1")
        with self.assertLogs("wontopos", level="WARNING") as logs:
            with self.assertRaises(WosError):
                mem.stats("u")
        self.assertEqual(mem.rate_limit, {"limit": 150, "remaining": 7, "reset": 1720000000})
        self.assertTrue(any('model "tablet-1" retires on 2100-06-01' in line for line in logs.output), logs.output)

    async def test_async_a_body_that_never_finishes_still_leaves_the_rate_limit_and_the_retirement_warning(self):
        if not HAVE_HTTPX:
            self.skipTest("httpx not installed (pip install 'wontopos[async]')")
        async with AsyncClient(KEY, base_url=cut_body_server(self), retries=0, model="tablet-1") as mem:
            with self.assertLogs("wontopos", level="WARNING") as logs:
                with self.assertRaises(WosError):
                    await mem.stats("u")
            self.assertEqual(mem.rate_limit, {"limit": 150, "remaining": 7, "reset": 1720000000})
        self.assertTrue(any('model "tablet-1" retires on 2100-06-01' in line for line in logs.output), logs.output)


if __name__ == "__main__":
    unittest.main()
