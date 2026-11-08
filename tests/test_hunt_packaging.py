import os
import socket
import ssl
import sys
import threading
import tracemalloc
import unittest
import zlib
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import requests
import urllib3
import urllib3.connection as u3_connection
from urllib3.exceptions import MaxRetryError, SSLError
from urllib3.util.ssl_ import is_ipaddress
from urllib3.util.ssl_match_hostname import CertificateError

import wontopos
from wontopos import APIConnectionError, Client

KEY = "wos-test-xxxxxxxxxx"


def silent_https_base(case):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    held = []

    def run():
        while True:
            try:
                held.append(srv.accept()[0])
            except OSError:
                return

    threading.Thread(target=run, daemon=True).start()
    case.addCleanup(lambda: [s.close() for s in held])
    case.addCleanup(srv.close)
    return f"https://127.0.0.1:{srv.getsockname()[1]}"


class Urllib3OneIpHostTest(unittest.TestCase):

    def setUp(self):
        real_wrap = u3_connection.ssl_wrap_socket

        def wrap_as_urllib3_1(*args, **kwargs):
            if is_ipaddress(kwargs.get("server_hostname") or ""):
                kwargs["server_hostname"] = None
            return real_wrap(*args, **kwargs)

        saved_version, saved_ctx = urllib3.__version__, wontopos._TLS_CONTEXTS.pop("sync", None)
        urllib3.__version__, u3_connection.ssl_wrap_socket = "1.26.20", wrap_as_urllib3_1

        def restore():
            urllib3.__version__, u3_connection.ssl_wrap_socket = saved_version, real_wrap
            wontopos._TLS_CONTEXTS.pop("sync", None)
            if saved_ctx is not None:
                wontopos._TLS_CONTEXTS["sync"] = saved_ctx

        self.addCleanup(restore)

    def test_an_https_ip_host_is_reached_where_urllib3_1_sends_no_server_name(self):
        base = silent_https_base(self)
        with self.assertRaises(APIConnectionError) as cm:
            Client(KEY, base_url=base, timeout=10, deadline=0.5, retries=0).list_stores()
        self.assertIn("deadline of 0.5s exhausted", str(cm.exception))
        self.assertEqual(wontopos._tls_context().verify_mode, ssl.CERT_REQUIRED)


class Urllib3OneNameRefusalTest(unittest.TestCase):

    def test_a_name_urllib3_refuses_is_reported_as_a_refused_certificate(self):
        refused = CertificateError("hostname '127.0.0.1' doesn't match 'api.wontopos.com'")
        e = requests.exceptions.SSLError(MaxRetryError(None, "/api/v1/memory/collections", SSLError(refused)))
        self.assertEqual(wontopos._transport_reason(e),
                         "SSLError (TLS certificate: hostname '127.0.0.1' doesn't match 'api.wontopos.com')")


class InflateStepTest(unittest.TestCase):

    def test_a_bomb_costs_its_cap_and_not_twice_it_on_every_python(self):
        def bomb(wbits):
            packer = zlib.compressobj(9, zlib.DEFLATED, wbits)
            block = bytes(1 << 20)
            return b"".join(packer.compress(block) for _ in range(64)) + packer.flush()

        asked = []
        real = zlib.decompressobj

        class Spy:
            def __init__(self, *args):
                self._d = real(*args)

            def __getattr__(self, name):
                return getattr(self._d, name)

            def decompress(self, data, max_length=0):
                asked.append(max_length)
                return self._d.decompress(data, max_length)

        cap = 32 << 20
        for coding, wbits in (("gzip", 31), ("deflate", 15)):
            with self.subTest(coding=coding):
                asked.clear()
                data = bomb(wbits)
                out = bytearray()
                tracemalloc.start()
                try:
                    with mock.patch.object(zlib, "decompressobj", Spy), self.assertRaises(wontopos.WosError):
                        inflater = wontopos._Inflater(200, coding, cap, lambda: wontopos.WosError(200, "too large"))
                        inflater.feed(data, out)
                    peak = tracemalloc.get_traced_memory()[1]
                finally:
                    tracemalloc.stop()
                self.assertLessEqual(len(out), cap)
                self.assertLess(peak, cap + (8 << 20), f"peak {peak >> 20} MiB for a {cap >> 20} MiB cap")
                self.assertTrue(asked and max(asked) <= 1 << 20, f"zlib was asked for up to {max(asked or [0])} bytes at once")


if __name__ == "__main__":
    unittest.main()
