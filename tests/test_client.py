import contextlib
import inspect
import logging
import json
import numbers
import os
import pickle
import re
import sys
import socket
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import wontopos
from wontopos import (
    APIConnectionError,
    AsyncClient,
    Client,
    NotFoundError,
    RateLimitError,
    WosError,
    __version__,
)

try:
    import httpx  # noqa: F401
    HAVE_HTTPX = True
except ImportError:
    HAVE_HTTPX = False

needs_httpx = unittest.skipUnless(HAVE_HTTPX, "httpx not installed (pip install 'wontopos[async]')")


class ScriptedHandler(BaseHTTPRequestHandler):

    def _serve(self):
        srv = self.server
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        srv.seen.append(
            {"method": self.command, "path": self.path, "headers": dict(self.headers), "body": body}
        )
        status, headers, payload = (
            srv.script[len(srv.seen) - 1] if len(srv.seen) - 1 < len(srv.script) else (200, {}, "{}")
        )
        data = payload if isinstance(payload, bytes) else payload.encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = do_DELETE = _serve

    def log_message(self, *_):
        pass


def scripted_server(script):
    srv = HTTPServer(("127.0.0.1", 0), ScriptedHandler)
    srv.script = script
    srv.seen = []
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_port}"


def dropping_server(script):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    accepted = []

    def run():
        for entry in script:
            try:
                sock, _ = srv.accept()
            except OSError:
                break
            accepted.append(1)
            try:
                sock.settimeout(2.0)
                data = b""
                while b"\r\n\r\n" not in data:
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                if entry == "TRUNCATE":
                    sock.sendall(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                        b"Content-Length: 200\r\nConnection: close\r\n\r\n"
                        b"12345"
                    )
                elif isinstance(entry, tuple) and entry[1] == "TRUNCATE":
                    sock.sendall(
                        f"HTTP/1.1 {entry[0]} ERR\r\n".encode()
                        + b"Content-Type: application/json\r\nRetry-After: 0\r\n"
                        b"Content-Length: 200\r\nConnection: close\r\n\r\n"
                        b"12345"
                    )
                elif isinstance(entry, tuple):
                    code, ctype, payload = entry
                    raw = payload.encode()
                    sock.sendall(
                        f"HTTP/1.1 {code} OK\r\nContent-Type: {ctype}\r\n".encode()
                        + f"Content-Length: {len(raw)}\r\nConnection: close\r\n\r\n".encode()
                        + raw
                    )
                elif entry is not None:
                    body = entry.encode()
                    sock.sendall(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                        + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                        + body
                    )
            except OSError:
                pass
            finally:
                sock.close()
        srv.close()

    threading.Thread(target=run, daemon=True).start()
    return accepted, f"http://127.0.0.1:{srv.getsockname()[1]}"


def trickle_server(case, head, body=b"", interval=0.1, trickle_head=False):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    seen = []

    def handle(sock):
        try:
            sock.settimeout(5.0)
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = sock.recv(65536)
                if not chunk:
                    return
                data += chunk
            seen.append(1)
            out = head + body
            if not trickle_head:
                sock.sendall(head)
                out = body
            for i in range(len(out)):
                time.sleep(interval)
                sock.sendall(out[i:i + 1])
        except OSError:
            pass
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def run():
        while True:
            try:
                sock, _ = srv.accept()
            except OSError:
                return
            threading.Thread(target=handle, args=(sock,), daemon=True).start()

    threading.Thread(target=run, daemon=True).start()
    case.addCleanup(srv.close)
    return seen, f"http://127.0.0.1:{srv.getsockname()[1]}"


def staged_server(case, entries):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    seen = []
    stop = threading.Event()

    def handle(sock, entry):
        try:
            sock.settimeout(5.0)
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = sock.recv(65536)
                if not chunk:
                    return
                data += chunk
            head, _, rest = data.partition(b"\r\n\r\n")
            m = re.search(rb"(?i)\r\ncontent-length:\s*(\d+)", head)
            need = int(m.group(1)) if m else 0
            while len(rest) < need:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                rest += chunk
            seen.append(head.split(b"\r\n", 1)[0].decode())
            if entry is None:
                stop.wait(10)
                return
            status, headers, body, delay = entry
            raw = body.encode() if isinstance(body, str) else body
            out = f"HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n"
            out += f"Content-Length: {len(raw)}\r\nConnection: close\r\n"
            out += "".join(f"{k}: {v}\r\n" for k, v in headers.items())
            sock.sendall(out.encode() + b"\r\n")
            if delay and stop.wait(delay):
                return
            sock.sendall(raw)
        except OSError:
            pass
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def run():
        n = 0
        while True:
            try:
                sock, _ = srv.accept()
            except OSError:
                return
            entry = entries[min(n, len(entries) - 1)]
            n += 1
            threading.Thread(target=handle, args=(sock, entry), daemon=True).start()

    threading.Thread(target=run, daemon=True).start()
    case.addCleanup(srv.close)
    case.addCleanup(stop.set)
    return seen, f"http://127.0.0.1:{srv.getsockname()[1]}"


def closed_port_url():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"http://127.0.0.1:{port}"


class ClientTests(unittest.TestCase):
    def make(self, script, **kw):
        srv, base = scripted_server(script)
        self.addCleanup(srv.shutdown)
        return srv, Client("wos-test-xxxxxxxxxx", base_url=base, **kw)

    def test_retries_429_then_succeeds(self):
        srv, mem = self.make([
            (429, {"Retry-After": "0"}, '{"error": "rate limited"}'),
            (200, {}, '{"memories": []}'),
        ])
        self.assertEqual(mem.search("q", user_id="alice"), [])
        self.assertEqual(len(srv.seen), 2)

    def test_get_image_reaches_the_network_and_retries_a_429(self):
        srv, mem = self.make([
            (429, {"Retry-After": "0"}, '{"error": "rate limited"}'),
            (200, {"Content-Type": "image/webp"}, "PNGBYTES"),
        ])
        data, mime = mem.get_image(memory_id="11111111-1111-1111-1111-111111111111")
        self.assertEqual(data, b"PNGBYTES")
        self.assertIn("image/webp", mime)
        self.assertEqual(len(srv.seen), 2, "the 429 must have been retried")

    def test_retries_are_configurable_and_zero_disables(self):
        srv, mem = self.make([(429, {"Retry-After": "0"}, '{"error": "rate limited"}')], retries=0)
        with self.assertRaises(WosError) as cm:
            mem.search("q")
        self.assertEqual(cm.exception.status, 429)
        self.assertEqual(len(srv.seen), 1)

    def test_no_retry_on_400_and_parses_envelope(self):
        srv, mem = self.make([
            (400, {}, json.dumps({
                "type": "error",
                "error": {"type": "invalid_request_error", "message": "boom", "request_id": "req_123"},
            })),
        ])
        with self.assertRaises(WosError) as cm:
            mem.stats()
        self.assertEqual(cm.exception.status, 400)
        self.assertEqual(cm.exception.message, "boom")
        self.assertEqual(cm.exception.request_id, "req_123")
        self.assertIn("req_123", str(cm.exception))
        self.assertEqual(len(srv.seen), 1)

    def test_refuses_redirects(self):
        _, mem = self.make([(302, {"Location": "http://evil.example/"}, "")])
        with self.assertRaises(WosError) as cm:
            mem.stats()
        self.assertEqual(cm.exception.status, 302)
        self.assertIn("redirect", cm.exception.message)

    def test_delete_requires_memory_id(self):
        mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")
        with self.assertRaises(ValueError):
            mem.delete()
        with self.assertRaises(ValueError):
            mem.delete(memory_id=None)
        with self.assertRaises(ValueError):
            mem.delete_all("")
        with self.assertRaises(ValueError):
            mem.delete_store("")

    def test_image_and_lineage_calls_require_a_memory_id(self):
        mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")
        for call in (
            lambda: mem.get_image(memory_id=""),
            lambda: mem.get_image(memory_id="   "),
            lambda: mem.get_image(memory_id=None),
            lambda: mem.get_image(memory_id=7),
            lambda: mem.forget_image(memory_id=""),
            lambda: mem.forget_image(memory_id="   "),
            lambda: mem.forget_image(memory_id=None),
            lambda: mem.lineage(memory_id=""),
            lambda: mem.lineage(memory_id=None),
        ):
            with self.assertRaises(ValueError):
                call()

    def test_max_retries_alias_does_not_beat_an_explicit_retries(self):
        key = "wos-test-xxxxxxxxxx"
        self.assertEqual(Client(key, retries=2, max_retries=5)._retries, 2)
        self.assertEqual(Client(key, max_retries=5)._retries, 5)
        self.assertEqual(Client(key, max_retries=0)._retries, 0)
        self.assertEqual(Client(key, retries=4)._retries, 4)
        self.assertEqual(Client(key)._retries, 2)

    def test_repr_masks_the_key(self):
        mem = Client("wos-live-supersecretkeyvalue1234")
        self.assertNotIn("supersecretkeyvalue", repr(mem))
        self.assertIn("wos-", repr(mem))
        self.assertIn("1234", repr(mem))

    def test_headers_model_and_user_agent(self):
        srv, mem = self.make([(200, {}, "{}"), (200, {}, "{}")])
        mem.with_model("scroll-1").stats()
        mem.stats(model="scroll-1")
        self.assertEqual(srv.seen[0]["headers"].get("X-WOS-Model"), "scroll-1")
        self.assertEqual(srv.seen[1]["headers"].get("X-WOS-Model"), "scroll-1")
        ua = srv.seen[0]["headers"].get("User-Agent")
        self.assertTrue(ua.startswith(f"wontopos-python/{__version__} (python/"), ua)

    def test_search_opts_cannot_override_reserved_fields(self):
        srv, mem = self.make([(200, {}, '{"memories": []}')])
        with self.assertRaises(ValueError) as e:
            mem.search("real-query", user_id="alice", limit=7, max_results=999)
        self.assertIn("set by the call", str(e.exception))
        self.assertEqual(srv.seen, [], "a refused option must not reach the wire")

        mem.search("real-query", user_id="alice", limit=7, filters={"categories": ["x"]})
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body["user_id"], "alice")
        self.assertEqual(body["query"], "real-query")
        self.assertEqual(body["max_results"], 7)
        self.assertEqual(body["filters"], {"categories": ["x"]})

    def test_search_count_out_of_range_is_refused_not_adjusted(self):
        _, mem = self.make([])
        for bad in (0, 1, 4, 21, 50, 100):
            for call in (mem.search, mem.search_full, mem.search_self):
                with self.assertRaises(ValueError) as cm:
                    call("q", "alice", bad)
                self.assertIn("between 5 and 20", str(cm.exception))
                self.assertNotIsInstance(cm.exception, WosError)

    def test_search_full_keeps_what_search_merges_away(self):
        payload = (
            '{"memories": [{"id": "m1"}], "self_memories": [{"id": "s1"}], '
            '"images": [{"id": "i1"}], "verify_used": 2}'
        )
        srv, mem = self.make([(200, {}, payload), (200, {}, payload)])
        r = mem.search_full("q", user_id="alice", max_images=3, verify=2)
        self.assertEqual([m["id"] for m in r["images"]], ["i1"])
        self.assertEqual([m["id"] for m in r["self_memories"]], ["s1"])
        self.assertEqual([m["id"] for m in r["memories"]], ["m1"])
        self.assertEqual(r["verify_used"], 2)
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body["max_images"], 3)
        self.assertEqual(body["verify"], 2)
        self.assertEqual([m["id"] for m in mem.search("q", user_id="alice")], ["m1", "s1", "i1"])

    def test_search_full_normalizes_a_nulled_field(self):
        srv, mem = self.make([(200, {}, '{"memories": null, "images": null}')])
        r = mem.search_full("q", user_id="alice")
        self.assertEqual(r["memories"], [])
        self.assertEqual(r["images"], [])
        self.assertEqual(r["self_memories"], [])
        self.assertNotIn("verify_used", r)

    def test_search_self_returns_both_fields(self):
        srv, mem = self.make([(200, {}, '{"memories": [{"id": "m1"}], "self_memories": [{"id": "s1"}]}')])
        r = mem.search_self("q", user_id="alice")
        self.assertEqual([m["id"] for m in r["memories"]], ["m1"])
        self.assertEqual([m["id"] for m in r["self_memories"]], ["s1"])
        self.assertEqual(len(srv.seen), 1)
        self.assertEqual(srv.seen[0]["path"], "/api/v1/memory/search")

    def test_revisions_goes_to_the_won_surface(self):
        srv, mem = self.make([(200, {}, '{"revised": 3, "total": 40}')])
        r = mem.revisions(user_id="alice")
        self.assertEqual(r["revised"], 3)
        self.assertEqual(srv.seen[0]["path"], "/api/v1/won/revisions")


    def test_verify_and_max_images_are_named_arguments(self):
        srv, mem = self.make([(200, {}, '{"memories": []}')])
        mem.search("q", user_id="alice", limit=10, verify=2, max_images=5)
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body["verify"], 2)
        self.assertEqual(body["max_images"], 5)
        self.assertEqual(body["user_id"], "alice")

    def test_a_misspelled_option_is_refused_before_the_wire(self):
        srv, mem = self.make([(200, {}, '{"memories": []}')])
        with self.assertRaises(ValueError) as e:
            mem.search("q", user_id="alice", verfy=3)
        self.assertIn("verfy", str(e.exception))
        self.assertIn("verify", str(e.exception))
        self.assertEqual(srv.seen, [], "a refused option must not reach the wire")

    def test_extra_is_the_forward_compatible_way_through(self):
        srv, mem = self.make([(200, {}, '{"memories": []}')])
        mem.search("q", user_id="alice", extra={"future_option": 1})
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body.get("future_option"), 1)
        self.assertNotIn("extra", body)

    def test_omitting_them_sends_neither_key(self):
        srv, mem = self.make([(200, {}, '{"memories": []}')])
        mem.search("q", user_id="alice")
        body = json.loads(srv.seen[0]["body"])
        self.assertNotIn("verify", body)
        self.assertNotIn("max_images", body)

    def test_revisions_asks_for_counts_only_unless_a_page_is_requested(self):
        srv, mem = self.make([(200, {}, '{"revised": 3, "unrevised": 37, "total": 40}')])
        mem.revisions(user_id="alice")
        self.assertEqual(sorted(json.loads(srv.seen[0]["body"])), ["user_id"])

    def test_revisions_page_options_go_out_under_the_wire_names(self):
        srv, mem = self.make([(200, {}, '{"memories": []}')])
        mem.revisions(user_id="alice", include="unrevised", limit=20,
                      before="2026-08-20T01:00:00Z", skip_ids=["a-1"])
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body["include"], "unrevised")
        self.assertEqual(body["limit"], 20)
        self.assertEqual(body["before"], "2026-08-20T01:00:00Z")
        self.assertEqual(body["skip_ids"], ["a-1"])

    def test_search_self_null_or_missing_self_memories_is_empty_list(self):
        srv, mem = self.make([(200, {}, '{"memories": [{"id": "m1"}], "self_memories": null}')])
        r = mem.search_self("q", user_id="u")
        self.assertEqual(r["self_memories"], [])
        self.assertEqual([m["id"] for m in r["memories"]], ["m1"])

    def test_bad_elements_are_skipped_not_passed_through(self):
        bad = '{"memories": [null, 1, "x", [], {"id": "ok", "content": "c"}]}'
        _, mem = self.make([(200, {}, bad)])
        r = mem.search("q", user_id="u")
        self.assertEqual([m["id"] for m in r], ["ok"])
        _, mem = self.make([(200, {}, '{"memories": [null, {"id": "m1"}], "self_memories": [2, {"id": "s1"}]}')])
        r = mem.search_self("q", user_id="u")
        self.assertEqual([m["id"] for m in r["memories"]], ["m1"])
        self.assertEqual([m["id"] for m in r["self_memories"]], ["s1"])
        _, mem = self.make([(200, {}, '{"turns": [null, "x", {"role": "user"}]}')])
        self.assertEqual(mem.history(user_id="u"), [{"role": "user"}])

    def test_search_returns_both_fields(self):
        body = ('{"memories": [{"id": "m1", "content": "partner said this"}],'
                ' "self_memories": [{"id": "s1", "content": "I said this", "speaker": "me"},'
                '                   {"id": "m1", "content": "dup"}]}')
        _, mem = self.make([(200, {}, body)])
        out = mem.search("q", user_id="u")
        self.assertEqual([m["id"] for m in out], ["m1", "s1"], "both fields, id-deduplicated")
        self.assertEqual(out[1]["speaker"], "me", "the assistant's own words keep their speaker")

    def test_search_without_separate_self_memories_is_unchanged(self):
        _, mem = self.make([(200, {}, '{"memories": [{"id": "m1"}]}')])
        self.assertEqual([m["id"] for m in mem.search("q", user_id="u")], ["m1"])

    def test_get_and_delete_accept_the_id_alone(self):
        _, mem = self.make([(200, {}, '{"memory": {"id": "9b2d8c1e-0000-4000-8000-000000000000"}}')])
        got = mem.get("9b2d8c1e-0000-4000-8000-000000000000")
        self.assertEqual(got["id"], "9b2d8c1e-0000-4000-8000-000000000000")

    def test_delete_with_no_id_at_all_still_refuses(self):
        _, mem = self.make([(200, {}, "{}")])
        with self.assertRaises(ValueError):
            mem.delete()

    def test_star_import_exports_only_the_public_surface(self):
        ns = {}
        exec("from wontopos import *", ns)
        for leaked in ("os", "sys", "json", "re", "requests", "ssl", "random", "logging"):
            self.assertNotIn(leaked, ns, f"{leaked} leaked into a star-import")
        self.assertIn("Client", ns)

    def test_wrong_type_list_dict_fields_coerced_to_empty(self):
        srv, mem = self.make([
            (200, {}, '{"memories": "HACKED"}'),
            (200, {}, '{"turns": 42}'),
            (200, {}, '{"memory": "not-a-dict"}'),
            (200, {}, '{"memories": {"a": 1}, "self_memories": "x"}'),
        ])
        self.assertEqual(mem.search("q", "u"), [])
        self.assertEqual(mem.history("u"), [])
        self.assertEqual(mem.get("u", memory_id="x"), {})
        r = mem.search_self("q", "u")
        self.assertEqual(r, {"memories": [], "self_memories": []})

    def test_recall_and_engram_send_form_and_tz(self):
        srv, mem = self.make([(200, {}, '{"long_term":{"memories":[]}}'), (200, {}, '{"memories":[]}')])
        mem.recall("q", "u", form="archive", tz=9)
        b = json.loads(srv.seen[0]["body"])
        self.assertEqual(b["form"], "archive")
        self.assertEqual(b["tz"], 9)
        mem.engram("deep_recall", "q", "u", form="memoir")
        b2 = json.loads(srv.seen[1]["body"])
        self.assertEqual(b2["form"], "memoir")

    def test_post_write_not_retried_on_5xx(self):
        srv, mem = self.make([(502, {}, '{"error": "bad gateway"}')])
        with self.assertRaises(WosError) as cm:
            mem.create_store("u")
        self.assertEqual(cm.exception.status, 502)
        self.assertEqual(len(srv.seen), 1)
        self.assertIsNone(srv.seen[0]["headers"].get("Idempotency-Key"))

    def test_memory_write_retried_on_5xx_under_one_key(self):
        srv, mem = self.make([
            (502, {"Retry-After": "0"}, '{"error": "bad gateway"}'),
            (200, {}, '{"id": "m1", "status": "stored"}'),
        ])
        r = mem.add("hello", user_id="u")
        self.assertEqual(r["id"], "m1")
        self.assertEqual(len(srv.seen), 2)
        k = srv.seen[0]["headers"].get("Idempotency-Key")
        self.assertRegex(k, r"\Asdk-[0-9a-f]{32}\Z")
        self.assertEqual(srv.seen[1]["headers"].get("Idempotency-Key"), k,
                         "a retry must repeat the key, or it is a second write")
        self.assertEqual(srv.seen[1]["body"], srv.seen[0]["body"])

    def test_each_memory_write_gets_a_key_of_its_own(self):
        srv, mem = self.make([(200, {}, '{"id": "m", "status": "stored"}')] * 4)
        mem.add("same text", "u")
        mem.add("same text", "u")
        mem.add_turn("hi", "yo", "u")
        mem.update("m1", "new", "u")
        keys = [r["headers"].get("Idempotency-Key") for r in srv.seen]
        for k in keys:
            self.assertRegex(k, r"\Asdk-[0-9a-f]{32}\Z")
        self.assertEqual(len(set(keys)), 4)

    def test_a_replay_of_the_clients_own_retry_is_not_reported(self):
        srv, mem = self.make([
            (503, {"Retry-After": "0"}, "{}"),
            (200, {"Idempotent-Replayed": "true"}, '{"id": "m1", "status": "stored"}'),
        ])
        r = mem.add("x", "alice")
        self.assertEqual(len(srv.seen), 2)
        self.assertNotIn("replayed", r)

    def test_get_retried_on_503(self):
        srv, mem = self.make([
            (503, {"Retry-After": "0"}, "{}"),
            (200, {}, '{"models": []}'),
        ])
        mem.list_models()
        self.assertEqual(len(srv.seen), 2)

    def test_a_long_error_envelope_keeps_its_message_and_request_id(self):
        env = json.dumps({"type": "error", "error": {
            "type": "invalid_request_error", "message": "bad input",
            "request_id": "req_ABC123", "detail": "x" * 5000}})
        srv, mem = self.make([(400, {}, env)])
        with self.assertRaises(WosError) as cm:
            mem.stats()
        self.assertEqual(cm.exception.message, "bad input")
        self.assertEqual(cm.exception.request_id, "req_ABC123")

    def test_a_huge_error_string_is_still_capped(self):
        srv, mem = self.make([(400, {}, json.dumps({"error": "Z" * 9000}))])
        with self.assertRaises(WosError) as cm:
            mem.stats()
        self.assertLessEqual(len(cm.exception.message), 4200)
        self.assertTrue(cm.exception.message.endswith("…(truncated)"))

    def test_get_retried_on_504_and_408(self):
        for status in (504, 408):
            with self.subTest(status=status):
                srv, mem = self.make([
                    (status, {"Retry-After": "0"}, "{}"),
                    (200, {}, '{"models": []}'),
                ])
                mem.list_models()
                self.assertEqual(len(srv.seen), 2, f"{status} was not retried")

    def test_post_write_not_retried_on_504_or_408(self):
        for status in (504, 408):
            with self.subTest(status=status):
                srv, mem = self.make([(status, {}, '{"error": "gateway"}')])
                with self.assertRaises(WosError) as cm:
                    mem.add_speaker("Bob", "u")
                self.assertEqual(cm.exception.status, status)
                self.assertEqual(len(srv.seen), 1, f"{status} retried a write without a key")

    def test_backoff_parses_http_date_retry_after(self):
        from email.utils import formatdate
        future = formatdate(time.time() + 10, usegmt=True)
        self.assertTrue(1.0 <= wontopos._backoff(0, future) <= 30.0)
        past = formatdate(time.time() - 100, usegmt=True)
        self.assertEqual(wontopos._backoff(0, past), 0.0)

    def test_non_serializable_body_is_type_error_not_network(self):
        mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")
        with self.assertRaises(TypeError):
            mem.add("x", user_id="u", weird=object())

    def test_uppercase_http_scheme_warns(self):
        import warnings as _w
        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter("always")
            Client("wos-test-xxxxxxxxxx", base_url="HTTP://example.com")
        self.assertTrue(any("unencrypted" in str(c.message) for c in caught))

    def test_iter_memories_stops_on_repeated_cursor(self):
        srv, mem = self.make([
            (200, {}, '{"memories": [{"id": "1"}], "next_cursor": "C"}'),
            (200, {}, '{"memories": [{"id": "2"}], "next_cursor": "C"}'),
        ])
        out = []
        with self.assertRaises(RuntimeError) as cm:
            for m in mem.iter_memories(user_id="u", page_size=1):
                out.append(m["id"])
        self.assertIn("truncated", str(cm.exception))
        self.assertEqual(len(srv.seen), 2)
        self.assertEqual(out, ["1", "2"])

    def test_delete_all_rejects_blank_and_whitespace(self):
        mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")
        for uid in ["", " ", "\t", "\n", "   ", " "]:
            with self.assertRaises(ValueError):
                mem.delete_all(uid)

    def test_backoff_honors_retry_after_and_caps(self):
        self.assertEqual(wontopos._backoff(0, "2"), 2.0)
        self.assertEqual(wontopos._backoff(0, "999"), 30.0)
        self.assertTrue(4.0 <= wontopos._backoff(3) <= 4.25)
        self.assertTrue(8.0 <= wontopos._backoff(10) <= 8.25)

    def test_post_not_retried_on_midstream_drop(self):
        accepted, base = dropping_server([None, None])
        mem = Client("wos-test-xxxxxxxxxx", base_url=base, retries=1)
        with self.assertRaises(wontopos.APIConnectionError):
            mem.create_store("u")
        self.assertEqual(len(accepted), 1)

    def test_memory_write_retried_on_midstream_drop(self):
        accepted, base = dropping_server([None, '{"id": "m1", "status": "stored"}'])
        mem = Client("wos-test-xxxxxxxxxx", base_url=base, retries=1)
        self.assertEqual(mem.add("hello", user_id="u")["id"], "m1")
        self.assertEqual(len(accepted), 2)

    def test_get_retried_on_midstream_drop(self):
        accepted, base = dropping_server([None, '{"models": []}'])
        mem = Client("wos-test-xxxxxxxxxx", base_url=base, retries=1)
        self.assertEqual(mem.list_models(), [])
        self.assertEqual(len(accepted), 2)

    def test_never_sent_classifies_connect_failures(self):
        from urllib3.exceptions import MaxRetryError, NewConnectionError, ProtocolError

        refused = requests.exceptions.ConnectionError(
            MaxRetryError(None, "/", NewConnectionError(None, "connection refused"))
        )
        self.assertTrue(wontopos._never_sent(refused))
        self.assertTrue(wontopos._never_sent(requests.exceptions.ConnectTimeout()))
        midstream = requests.exceptions.ConnectionError(ProtocolError("Connection aborted."))
        self.assertFalse(wontopos._never_sent(midstream))

    def test_body_read_failure_is_api_connection_error(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.bind(("127.0.0.1", 0))
        srv.listen(2)

        def run():
            sock, _ = srv.accept()
            try:
                sock.settimeout(2.0)
                data = b""
                while b"\r\n\r\n" not in data:
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{")
            finally:
                sock.close()
                srv.close()

        threading.Thread(target=run, daemon=True).start()
        mem = Client("wos-test-xxxxxxxxxx", base_url=f"http://127.0.0.1:{srv.getsockname()[1]}", retries=0)
        with self.assertRaises(WosError):
            mem.stats()

    def test_timeout_nan_or_zero_means_the_default(self):
        for unset in (float("nan"), 0):
            self.assertEqual(Client("wos-test-xxxxxxxxxx", timeout=unset)._timeout, 30.0)

    def test_with_retries_and_with_timeout_clones(self):
        srv, mem = self.make([
            (429, {"Retry-After": "0"}, '{"error": "rate limited"}'),
            (200, {}, "{}"),
        ])
        with self.assertRaises(WosError) as cm:
            mem.with_retries(0).stats()
        self.assertEqual(cm.exception.status, 429)
        self.assertEqual(len(srv.seen), 1)
        mem.with_timeout(5).stats()
        self.assertEqual(len(srv.seen), 2)

    def test_close_is_idempotent(self):
        srv, mem = self.make([(200, {}, "{}")])
        mem.stats()
        mem.close()
        mem.close()

    def test_debug_logging_via_standard_logger(self):
        srv, mem = self.make([(200, {}, '{"memories": []}')])
        with self.assertLogs("wontopos", level="DEBUG") as captured:
            mem.search("super secret query text")
        joined = "\n".join(captured.output)
        self.assertIn("POST /api/v1/memory/search -> 200", joined)
        self.assertNotIn("super secret query text", joined)
        self.assertNotIn("wos-test", joined)

    def test_get_single_memory(self):
        srv, mem = self.make([
            (200, {}, '{"user_id": "u", "memory": {"id": "9b2d", "content": "tea", "is_superseded": false}}'),
        ])
        m = mem.get(user_id="u", memory_id="9b2d")
        self.assertEqual(m["content"], "tea")
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body["memory_id"], "9b2d")
        with self.assertRaises(ValueError):
            mem.get(user_id="u")

    def test_get_takes_a_flat_row(self):
        srv, mem = self.make([
            (200, {}, '{"id": "9b2d", "content": "tea", "is_superseded": false}'),
            (200, {}, '{"user_id": "u", "memory": null}'),
        ])
        m = mem.get(user_id="u", memory_id="9b2d")
        self.assertEqual((m["id"], m["content"]), ("9b2d", "tea"))
        self.assertEqual(mem.get(user_id="u", memory_id="9b2d"), {})

    def test_forked_child_drops_the_parents_pooled_connections(self):
        import wontopos
        srv, mem = self.make([(200, {}, '{"total_memories": 0}')])
        mem.stats("alice")
        pools = mem._session.get_adapter(f"http://127.0.0.1:{srv.server_port}").poolmanager.pools
        self.assertEqual(len(pools), 1)
        wontopos._drop_pools_after_fork()
        self.assertEqual(len(pools), 0)
        self.assertIn(mem.with_user("bob")._session, list(wontopos._sessions))

    def test_forked_child_drops_proxy_pools_too(self):
        import wontopos
        srv, mem = self.make([])
        adapter = mem._session.get_adapter("https://api.wontopos.com")
        pm = adapter.proxy_manager_for("http://127.0.0.1:9")
        pm.connection_from_host("api.wontopos.com", 443, scheme="https")
        self.assertEqual(len(pm.pools), 1)
        wontopos._drop_pools_after_fork()
        self.assertEqual(len(pm.pools), 0)

    def test_search_methods_send_form_and_tz(self):
        ok = (200, {}, '{"memories": []}')
        srv, mem = self.make([ok, ok, ok])
        mem.search("q", user_id="alice", form="memoir", tz=9)
        mem.search_self("q", user_id="alice", form="archive", tz=-5)
        mem.search_full("q", user_id="alice", form="memoir", tz=0)
        sent = [json.loads(r["body"]) for r in srv.seen]
        self.assertEqual([(b["form"], b["tz"]) for b in sent], [("memoir", 9), ("archive", -5), ("memoir", 0)])

    def test_add_refuses_a_store_name_as_metadata(self):
        srv, mem = self.make([])
        for kw in ({"userId": "tenant-42"}, {"store_id": "t"}, {"metadata": {"user_id": "t"}},
                   {"storeid": "t"}, {"UserId": "t"}, {"USER_ID": "t"},
                   {"idempotencyKey": "k"}, {"metadata": {"Idempotency-Key": "k"}},
                   {"metadata": {"speaker": "me", "Store-Id": "t"}}):
            with self.subTest(kw=kw):
                with self.assertRaises(ValueError):
                    mem.add("tenant-private fact", **kw)
        self.assertEqual(srv.seen, [])

    def test_ordinary_words_are_still_metadata(self):
        srv, mem = self.make([(200, {}, '{"id": "m1"}')] * 3)
        wontopos._reset_warning_state()
        self.addCleanup(wontopos._reset_warning_state)
        with self.assertLogs("wontopos", level="WARNING") as logs:
            mem.add("bought milk", store="Costco")
            mem.add("chat log", metadata={"model": "gpt-4o", "user": "bob", "uid": 5, "tenant": "t",
                                          "user_1": "a", "speaker": "me"})
            mem.add("x", Model="gpt-4o", Store="t")
        self.assertIn("'store' is not kept", " ".join(logs.output))
        sent = [json.loads(r["body"]) for r in srv.seen]
        self.assertEqual([b["user_id"] for b in sent], ["default"] * 3)
        self.assertEqual(sent[0]["metadata"], {"store": "Costco"})
        self.assertEqual(sent[1]["metadata"]["model"], "gpt-4o")
        self.assertEqual(sent[1]["metadata"]["speaker"], "me")
        self.assertEqual(sent[2]["metadata"], {"Model": "gpt-4o", "Store": "t"})
        self.assertEqual(srv.seen[2]["headers"].get("X-WOS-Model"), wontopos.DEFAULT_MODEL)

    def test_tenant_and_numbered_keys_are_ordinary_metadata(self):
        srv, mem = self.make([(200, {}, '{"id": "m1"}')] * 2)
        mem.add("acme note", tenant_id="acme")
        mem.add("second profile", user_id_2="bob")
        self.assertEqual(len(srv.seen), 2)

    def test_search_returns_image_rows_after_the_text_each_id_once(self):
        payload = (
            '{"memories": [{"id": "m1"}, {"id": "dup"}], "self_memories": [{"id": "s1"}],'
            ' "images": [{"id": "dup"}, {"id": "i1", "content": "", "image_ref": "r"}]}'
        )
        srv, mem = self.make([(200, {}, payload), (200, {}, payload)])
        self.assertEqual([m["id"] for m in mem.search("q", user_id="alice")], ["m1", "dup", "s1", "i1"])
        self.assertEqual(sorted(mem.search_self("q", user_id="alice")), ["memories", "self_memories"])

    def test_non_object_2xx_body_is_wos_error(self):
        for body in ("null", "12345", '"hi"', "[1,2,3]"):
            srv, mem = self.make([(200, {}, body)])
            with self.assertRaises(WosError):
                mem.search("q", "u")
            srv2, mem2 = self.make([(200, {}, body)])
            with self.assertRaises(WosError):
                mem2.get("u", "9b2d8c1e-1111-4222-8333-444455556666")


    def test_read_capped_incremental_cap(self):
        old = wontopos._MAX_RESPONSE_BYTES
        try:
            wontopos._MAX_RESPONSE_BYTES = 16
            srv, mem = self.make([(200, {}, '{"x":"' + "y" * 64 + '"}')])
            with self.assertRaises(WosError) as cm:
                mem.stats()
            self.assertIn("too large", cm.exception.message)
        finally:
            wontopos._MAX_RESPONSE_BYTES = old

    def test_error_message_is_capped(self):
        srv, mem = self.make([(400, {}, '{"error": "' + "Z" * 100000 + '"}')])
        with self.assertRaises(WosError) as cm:
            mem.stats()
        self.assertLessEqual(len(cm.exception.message), 4096 + 20)
        self.assertIn("truncated", cm.exception.message)



    def test_clones_share_the_session_pool(self):
        mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")
        for clone in (mem.with_model("scroll-1"), mem.with_user("bob"), mem.with_timeout(5), mem.with_retries(0)):
            self.assertIs(clone._session, mem._session)
            self.assertFalse(clone._owns_session)
        self.assertTrue(mem._owns_session)

    def test_clone_close_does_not_close_parent_session(self):
        srv, mem = self.make([(200, {}, "{}"), (200, {}, "{}")])
        mem.with_model("scroll-1").close()
        mem.stats()
        self.assertEqual(len(srv.seen), 1)

    def test_model_still_sent_after_pool_sharing(self):
        srv, mem = self.make([(200, {}, "{}"), (200, {}, "{}"), (200, {}, "{}")])
        mem.with_model("scroll-1").stats()
        mem.stats(model="tablet-1")
        mem.stats()
        self.assertEqual(srv.seen[0]["headers"].get("X-WOS-Model"), "scroll-1")
        self.assertEqual(srv.seen[1]["headers"].get("X-WOS-Model"), "tablet-1")
        self.assertEqual(srv.seen[2]["headers"].get("X-WOS-Model"), wontopos.DEFAULT_MODEL)

    def test_null_memories_and_turns_yield_empty_lists(self):
        srv, mem = self.make([
            (200, {}, '{"memories": null}'),
            (200, {}, '{"turns": null}'),
            (200, {}, '{"memories": null, "next_cursor": null}'),
        ])
        self.assertEqual(mem.search("q"), [])
        self.assertEqual(mem.history(), [])
        self.assertEqual(list(mem.iter_memories()), [])

    def test_key_hygiene(self):
        mem = Client(" wos-test-xxxxxxxxxx\n", base_url="http://127.0.0.1:9")
        self.assertIn("wos-", repr(mem))
        with self.assertRaises(ValueError):
            Client("wos-test xxxxxxxxxx")
        with self.assertRaises(ValueError):
            Client("   ")

    def test_model_name_validation(self):
        with self.assertRaises(ValueError):
            Client("wos-test-xxxxxxxxxx", model="tablet-1\r\nX-Evil: 1")
        mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")
        with self.assertRaises(ValueError):
            mem.stats(model="bad model")

    def test_from_env(self):
        old = os.environ.get("WONTOPOS_API_KEY")
        try:
            os.environ["WONTOPOS_API_KEY"] = "wos-test-envkey12345"
            mem = Client.from_env(user_id="alice")
            self.assertIn("alice", repr(mem))
            del os.environ["WONTOPOS_API_KEY"]
            os.environ.pop("WOS_API_KEY", None)
            with self.assertRaises(ValueError):
                Client.from_env()
        finally:
            if old is not None:
                os.environ["WONTOPOS_API_KEY"] = old

    def test_response_size_cap(self):
        old_max = wontopos._MAX_RESPONSE_BYTES
        try:
            wontopos._MAX_RESPONSE_BYTES = 16
            srv, mem = self.make([(200, {}, '{"memories": ["' + "x" * 64 + '"]}')])
            with self.assertRaises(WosError) as cm:
                mem.search("q")
            self.assertIn("too large", cm.exception.message)
        finally:
            wontopos._MAX_RESPONSE_BYTES = old_max

    def test_tls_context_floor(self):
        import ssl
        ctx = wontopos._tls_context()
        self.assertGreaterEqual(ctx.minimum_version, ssl.TLSVersion.TLSv1_2)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)

    def test_rate_limit_headers_exposed(self):
        srv, mem = self.make([
            (200, {"X-RateLimit-Limit": "150", "X-RateLimit-Remaining": "3", "X-RateLimit-Reset": "1720000000"}, "{}"),
        ])
        self.assertIsNone(mem.rate_limit)
        mem.list_stores()
        self.assertEqual(mem.rate_limit, {"limit": 150, "remaining": 3, "reset": 1720000000})

    def test_a_rate_limit_header_is_read_as_a_whole_number_as_every_client_reads_it(self):
        cases = [("60", 60), ("007", 7), ("9007199254740991", 9007199254740991), ("9007199254740992", None),
                 ("-3", None), ("1.5", None), ("1e3", None), ("0x10", None), ("+5", None), ("1_000", None),
                 ("12abc", None), ("", None)]
        srv, mem = self.make([(200, {"X-RateLimit-Limit": v, "X-RateLimit-Remaining": "1", "X-RateLimit-Reset": "2"},
                               "{}") for v, _ in cases])
        for v, want in cases:
            mem.stats("u")
            self.assertEqual(mem.rate_limit, {"limit": want, "remaining": 1, "reset": 2}, v)

    def test_billed_to_says_when_api_credit_paid(self):
        srv, mem = self.make([
            (200, {"X-WOS-Billed-To": "balance"}, "{}"),
            (200, {}, "{}"),
            (402, {}, '{"type":"error","error":{"type":"billing_error","message":"This week\'s subscription usage is used up.","reason":"weekly_limit_reached","resets_at":"2026-11-01T01:00:00.000Z"}}'),
        ])
        self.assertIsNone(mem.billed_to)
        mem.usage()
        self.assertEqual(mem.billed_to, "balance")
        mem.usage()
        self.assertIsNone(mem.billed_to)
        with self.assertRaises(wontopos.PaymentRequiredError) as cm:
            mem.usage()
        self.assertEqual(cm.exception.details["reason"], "weekly_limit_reached")
        self.assertEqual(cm.exception.details["resets_at"], "2026-11-01T01:00:00.000Z")


@unittest.skipUnless(HAVE_HTTPX, "httpx not installed (pip install 'wontopos[async]')")
class AsyncClientTests(unittest.IsolatedAsyncioTestCase):
    def make(self, script, **kw):
        srv, base = scripted_server(script)
        self.addCleanup(srv.shutdown)
        from wontopos import AsyncClient

        return srv, AsyncClient("wos-test-xxxxxxxxxx", base_url=base, **kw)

    async def test_billed_to_says_when_api_credit_paid(self):
        srv, mem = self.make([(200, {"X-WOS-Billed-To": "balance"}, "{}"), (200, {}, "{}")])
        async with mem:
            self.assertIsNone(mem.billed_to)
            await mem.usage()
            self.assertEqual(mem.billed_to, "balance")
            await mem.usage()
            self.assertIsNone(mem.billed_to)

    async def test_add_takes_ordinary_words_as_metadata_and_refuses_store_ids(self):
        srv, mem = self.make([(200, {}, '{"id": "m1"}')])
        async with mem:
            with self.assertRaises(ValueError):
                await mem.add("x", store_id="t")
            with self.assertLogs("wontopos", level="WARNING"):
                wontopos._reset_warning_state()
                await mem.add("bought milk", store="Costco", metadata={"model": "gpt-4o"})
        self.assertEqual(json.loads(srv.seen[0]["body"])["metadata"], {"model": "gpt-4o", "store": "Costco"})
        self.assertEqual(len(srv.seen), 1)

    async def test_get_accepts_the_id_alone_like_sync(self):
        srv, mem = self.make([(200, {}, '{"memory": {"id": "m1"}}')])
        async with mem:
            r = await mem.get("11111111-1111-1111-1111-111111111111")
        self.assertEqual(r["id"], "m1")
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body["memory_id"], "11111111-1111-1111-1111-111111111111")

    async def test_get_takes_a_flat_row_like_sync(self):
        srv, mem = self.make([(200, {}, '{"id": "m1", "content": "tea"}')])
        async with mem:
            r = await mem.get("11111111-1111-1111-1111-111111111111")
        self.assertEqual(r["content"], "tea")

    async def test_get_image_actually_reads_bytes_on_the_async_transport(self):
        srv, mem = self.make([(200, {}, "PNGBYTES")])
        async with mem:
            data, ctype = await mem.get_image("alice", "11111111-1111-1111-1111-111111111111")
        self.assertEqual(data, b"PNGBYTES")
        self.assertTrue(isinstance(ctype, str))

    async def test_get_image_retries_a_429_on_the_async_transport(self):
        srv, mem = self.make([
            (429, {"Retry-After": "0"}, '{"error": "rate limited"}'),
            (200, {"Content-Type": "image/webp"}, "PNGBYTES"),
        ])
        async with mem:
            data, mime = await mem.get_image("alice", "11111111-1111-1111-1111-111111111111")
        self.assertEqual(data, b"PNGBYTES")
        self.assertIn("image/webp", mime)
        self.assertEqual(len(srv.seen), 2, "the 429 must have been retried")

    async def test_retries_429_then_succeeds(self):
        srv, mem = self.make([
            (429, {"Retry-After": "0"}, '{"error": "rate limited"}'),
            (200, {}, '{"memories": []}'),
        ])
        async with mem:
            self.assertEqual(await mem.search("q", user_id="alice"), [])
        self.assertEqual(len(srv.seen), 2)

    async def test_envelope_and_request_id(self):
        srv, mem = self.make([
            (400, {}, json.dumps({"type": "error", "error": {"message": "boom", "request_id": "req_9"}})),
        ])
        async with mem:
            with self.assertRaises(WosError) as cm:
                await mem.stats()
        self.assertEqual(cm.exception.status, 400)
        self.assertEqual(cm.exception.request_id, "req_9")
        self.assertEqual(len(srv.seen), 1)

    async def test_refuses_redirects(self):
        _, mem = self.make([(302, {"Location": "http://evil.example/"}, "")])
        async with mem:
            with self.assertRaises(WosError) as cm:
                await mem.stats()
        self.assertEqual(cm.exception.status, 302)
        self.assertIn("redirect", cm.exception.message)

    async def test_delete_guard_and_repr(self):
        from wontopos import AsyncClient

        mem = AsyncClient("wos-live-supersecretkeyvalue1234", base_url="http://127.0.0.1:9")
        try:
            with self.assertRaises(ValueError):
                await mem.delete()
            self.assertNotIn("supersecretkeyvalue", repr(mem))
        finally:
            await mem.aclose()

    async def test_same_surface_as_sync(self):
        from wontopos import AsyncClient

        sync_api = {n for n in dir(Client) if not n.startswith("_")} - {"close"}
        async_api = {n for n in dir(AsyncClient) if not n.startswith("_")} - {"aclose"}
        self.assertEqual(sync_api, async_api)



class Base64ShapeTest(unittest.TestCase):

    def _norm(self, data):
        from wontopos import _normalize_image  # type: ignore[attr-defined]
        return _normalize_image({"data": data})["data"]

    def test_wrapped_base64_is_flattened(self):
        flat = "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo=" * 3
        wrapped = "\n".join(flat[i:i + 76] for i in range(0, len(flat), 76))
        self.assertEqual(self._norm(wrapped), flat, "line breaks survived into the payload")

    def test_leading_space_before_data_url(self):
        self.assertEqual(self._norm("  data:image/png;base64,QUJD"), "QUJD")

    def test_plain_base64_is_untouched(self):
        self.assertEqual(self._norm("QUJDRA=="), "QUJDRA==")



class RetriesAliasTest(unittest.TestCase):

    def test_alias_sets_retries(self):
        self.assertEqual(Client(api_key="k", max_retries=5)._retries, 5)

    def test_canonical_name_still_works(self):
        self.assertEqual(Client(api_key="k", retries=5)._retries, 5)

    def test_default_unchanged(self):
        self.assertEqual(Client(api_key="k")._retries, 2)

    def test_explicit_retries_wins_over_alias(self):
        self.assertEqual(Client(api_key="k", retries=1, max_retries=9)._retries, 1)

    def test_zero_disables_via_alias(self):
        self.assertEqual(Client(api_key="k", max_retries=0)._retries, 0)

    @unittest.skipUnless(HAVE_HTTPX, "httpx not installed (pip install 'wontopos[async]')")
    def test_async_client_takes_the_alias_too(self):
        self.assertEqual(AsyncClient(api_key="k", max_retries=3)._retries, 3)


class ListEngramsTest(unittest.TestCase):

    def make(self, script, **kw):
        srv, base = scripted_server(script)
        self.addCleanup(srv.shutdown)
        return srv, Client("wos-test-xxxxxxxxxx", base_url=base, **kw)

    def test_returns_catalog(self):
        srv, mem = self.make([(200, {}, json.dumps(
            {"engrams": [{"name": "deep_recall"}], "forms": [{"name": "memoir"}]}))])
        cat = mem.list_engrams()
        self.assertEqual(srv.seen[-1]["path"], "/api/v1/engram")
        self.assertEqual([e["name"] for e in cat["engrams"]], ["deep_recall"])
        self.assertEqual([f["name"] for f in cat["forms"]], ["memoir"])

    def test_empty_is_a_list_not_none(self):
        srv, mem = self.make([(200, {}, json.dumps({"note": "no engrams on this model"}))])
        cat = mem.list_engrams()
        self.assertEqual(cat["engrams"], [])
        self.assertEqual(cat["forms"], [])
        self.assertIn("no engrams", cat["note"])


class EmptyBodyTest(unittest.TestCase):

    def make(self, script):
        srv, base = scripted_server(script)
        self.addCleanup(srv.shutdown)
        return Client("wos-test-xxxxxxxxxx", base_url=base)

    def test_204_is_success_not_an_error(self):
        mem = self.make([(204, {}, "")])
        self.assertEqual(mem.delete("alice", "m1"), {})

    def test_empty_200_is_an_error_not_a_silent_ok(self):
        mem = self.make([(200, {}, "")])
        with self.assertRaises(WosError) as cm:
            mem.add("x", "alice")
        self.assertEqual(cm.exception.status, 200)
        self.assertIn("empty response body", str(cm.exception))

    def test_whitespace_only_body_counts_as_empty(self):
        mem = self.make([(200, {}, "   \n")])
        with self.assertRaises(WosError) as cm:
            mem.add("x", "alice")
        self.assertIn("empty response body", str(cm.exception))


class IdempotencyKeyTest(unittest.TestCase):

    def make(self, script):
        srv, base = scripted_server(script)
        self.addCleanup(srv.shutdown)
        return srv, Client("wos-test-xxxxxxxxxx", base_url=base)

    def test_key_rides_as_a_header(self):
        srv, mem = self.make([(200, {}, '{"id":"m1","status":"stored"}')])
        mem.add("x", "alice", idempotency_key="import:row-42")
        self.assertEqual(srv.seen[-1]["headers"].get("Idempotency-Key"), "import:row-42")

    def test_key_is_not_stored_as_metadata(self):
        srv, mem = self.make([(200, {}, "{}")])
        mem.add("x", "alice", idempotency_key="k1", mood="calm")
        body = json.loads(srv.seen[-1]["body"])
        self.assertEqual(body["metadata"], {"mood": "calm"})
        self.assertNotIn("idempotency_key", json.dumps(body))

    def test_every_write_can_carry_one(self):
        srv, mem = self.make([(200, {}, "{}")] * 5)
        mem.add("x", "alice", idempotency_key="k1")
        mem.add_turn("hi", "yo", "alice", idempotency_key="k2")
        mem.add_bulk("blob", "alice", "general", None, idempotency_key="k3")
        mem.update("m1", "new", "alice", idempotency_key="k4")
        mem.add("x", "alice")
        got = [r["headers"].get("Idempotency-Key") for r in srv.seen]
        self.assertEqual(got[:4], ["k1", "k2", "k3", "k4"])
        self.assertRegex(got[4], r"\Asdk-[0-9a-f]{32}\Z")

    def test_a_trailing_newline_is_refused_here_not_by_http_client(self):
        srv, mem = self.make([(200, {}, "{}")])
        for bad in ("k1\n", "k1\r", "k1\r\n", "\nk1"):
            with self.assertRaises(ValueError) as cm:
                mem.add("x", "alice", idempotency_key=bad)
            self.assertIn("invalid idempotency_key", str(cm.exception))
        self.assertEqual(srv.seen, [])

    def test_malformed_key_fails_before_the_request(self):
        srv, mem = self.make([(200, {}, "{}")])
        for bad in ("caf\u00e9 key", "a" * 129, "", "has space"):
            with self.assertRaises(ValueError):
                mem.add("x", "alice", idempotency_key=bad)
        self.assertEqual(srv.seen, [])


class StoreIdMustBeAStringTest(unittest.TestCase):

    def make(self, script):
        srv, base = scripted_server(script)
        self.addCleanup(srv.shutdown)
        return srv, Client("wos-test-xxxxxxxxxx", base_url=base, user_id="the-default-store")

    def test_a_non_string_id_never_reaches_the_wire(self):
        srv, mem = self.make([(200, {}, "{}")] * 4)
        for bad in (0, 42, 3.5, True, [], {}):
            with self.assertRaises(ValueError) as cm:
                mem.add("hello", bad)
            self.assertIn("non-blank string", str(cm.exception))
        self.assertEqual(srv.seen, [], "nothing may be sent for any of them")

    def test_omitting_the_id_still_uses_the_default(self):
        srv, mem = self.make([(200, {}, "{}")])
        mem.add("hello")
        self.assertEqual(json.loads(srv.seen[-1]["body"])["user_id"], "the-default-store")

    def test_a_real_id_still_goes_through(self):
        srv, mem = self.make([(200, {}, "{}")])
        mem.add("hello", "alice")
        self.assertEqual(json.loads(srv.seen[-1]["body"])["user_id"], "alice")

    def test_destructive_calls_refuse_a_non_string_too(self):
        mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")
        for bad in (0, 42, None, []):
            with self.assertRaises((ValueError, TypeError)):
                mem.delete_all(bad)
            with self.assertRaises((ValueError, TypeError)):
                mem.delete_store(bad)


class DeleteStoreWarnsOnCollapseTest(unittest.TestCase):

    def test_delete_store_warns_like_delete_all(self):
        wontopos._reset_warning_state()
        srv, base = scripted_server([(200, {}, "{}")] * 2)
        self.addCleanup(srv.shutdown)
        mem = Client("wos-test-xxxxxxxxxx", base_url=base)
        with self.assertLogs("wontopos", level="WARNING") as cm:
            mem.delete_store("Alice.Smith")
        self.assertTrue(any("Alice.Smith" in m for m in cm.output))
        wontopos._reset_warning_state()
        with self.assertLogs("wontopos", level="WARNING") as cm:
            mem.delete_all("Alice.Smith")
        self.assertTrue(any("Alice.Smith" in m for m in cm.output))


class SearchFiltersTest(unittest.TestCase):

    def test_filters_reach_the_api(self):
        srv, base = scripted_server([(200, {}, '{"memories":[]}')])
        self.addCleanup(srv.shutdown)
        mem = Client("wos-test-xxxxxxxxxx", base_url=base)
        mem.search("q", "alice", filters={"categories": ["work"], "event_from": "2026-01-01"})
        body = json.loads(srv.seen[-1]["body"])
        self.assertEqual(body["filters"], {"categories": ["work"], "event_from": "2026-01-01"})


class StoreIdCollisionTest(unittest.TestCase):

    def make(self):
        srv, base = scripted_server([(200, {}, "{}")] * 4)
        self.addCleanup(srv.shutdown)
        wontopos._reset_warning_state()
        return Client("wos-test-xxxxxxxxxx", base_url=base)

    def test_warns_once_when_the_id_changes(self):
        mem = self.make()
        with self.assertLogs("wontopos", level="WARNING") as cm:
            mem.add("x", "Alice.Smith")
        joined = " ".join(cm.output)
        self.assertIn("Alice.Smith", joined)
        self.assertIn("alice_smith", joined)
        self.assertIn("cannot be created beside it (409)", joined)

    def test_an_id_the_api_does_not_accept_gets_its_own_warning(self):
        for bad in ("bob.lee@example.com", "_alice", "x" * 65):
            mem = self.make()
            with self.assertLogs("wontopos", level="WARNING") as cm:
                mem.add("x", bad)
            joined = " ".join(cm.output)
            self.assertIn("is not a valid store id", joined)
            self.assertIn("refused (400)", joined)
            self.assertNotIn("409", joined)

    def test_silent_when_already_normalized(self):
        mem = self.make()
        logger = logging.getLogger("wontopos")
        with self.assertNoLogs(logger, level="WARNING") if hasattr(self, "assertNoLogs") else contextlib.nullcontext():
            mem.add("x", "alice_smith")


class FilterTypoTest(unittest.TestCase):

    def test_warns_on_unknown_key(self):
        srv, base = scripted_server([(200, {}, '{"memories":[]}')])
        self.addCleanup(srv.shutdown)
        wontopos._reset_warning_state()
        mem = Client("wos-test-xxxxxxxxxx", base_url=base)
        with self.assertLogs("wontopos", level="WARNING") as cm:
            mem.search("q", "alice", filters={"catagories": ["work"]})
        self.assertIn("catagories", " ".join(cm.output))
        self.assertIn("NO effect", " ".join(cm.output))


class MetadataKeywordTest(unittest.TestCase):

    def body(self, srv):
        return json.loads(srv.seen[-1]["body"])

    def test_metadata_kwarg_is_not_nested(self):
        srv, base = scripted_server([(200, {}, "{}")])
        self.addCleanup(srv.shutdown)
        mem = Client("wos-test-xxxxxxxxxx", base_url=base)
        mem.add("x", "alice", metadata={"speaker": "me"})
        self.assertEqual(self.body(srv)["metadata"], {"speaker": "me"})

    def test_loose_kwargs_still_work(self):
        srv, base = scripted_server([(200, {}, "{}")])
        self.addCleanup(srv.shutdown)
        mem = Client("wos-test-xxxxxxxxxx", base_url=base)
        mem.add("x", "alice", speaker="me", event_date="2026-03-14")
        self.assertEqual(self.body(srv)["metadata"], {"speaker": "me", "event_date": "2026-03-14"})

    def test_both_merge_and_the_loose_one_wins(self):
        srv, base = scripted_server([(200, {}, "{}")])
        self.addCleanup(srv.shutdown)
        mem = Client("wos-test-xxxxxxxxxx", base_url=base)
        mem.add("x", "alice", metadata={"speaker": "Bob", "category": "work"}, speaker="me")
        self.assertEqual(self.body(srv)["metadata"], {"speaker": "me", "category": "work"})


class ReplayedFlagTest(unittest.TestCase):

    def test_replayed_is_surfaced(self):
        srv, base = scripted_server([
            (200, {"Idempotent-Replayed": "true"}, '{"id":"m1","status":"stored"}'),
            (200, {}, '{"id":"m2","status":"stored"}'),
        ])
        self.addCleanup(srv.shutdown)
        mem = Client("wos-test-xxxxxxxxxx", base_url=base)
        self.assertIs(mem.add("x", "alice", idempotency_key="k1").get("replayed"), True)
        self.assertIsNone(mem.add("y", "alice").get("replayed"))


class CacheControlTest(unittest.TestCase):

    @staticmethod
    def body(srv):
        return json.loads(srv.seen[0]["body"])

    def test_cache_control_reaches_the_api(self):
        srv, base = scripted_server([(200, {}, '{"memories":[]}')])
        self.addCleanup(srv.shutdown)
        mem = Client("wos-test-xxxxxxxxxx", base_url=base)
        mem.search("q", "alice", cache_control={"ttl": "5m"})
        self.assertEqual(self.body(srv)["cache_control"], {"ttl": "5m"})

    def test_speaker_reaches_the_api(self):
        srv, base = scripted_server([(200, {}, '{"memories":[]}')])
        self.addCleanup(srv.shutdown)
        mem = Client("wos-test-xxxxxxxxxx", base_url=base)
        mem.search("q", "alice", speaker="me")
        self.assertEqual(self.body(srv)["speaker"], "me")

    def test_reserved_fields_still_win(self):
        srv, base = scripted_server([(200, {}, '{"memories":[]}')])
        self.addCleanup(srv.shutdown)
        mem = Client("wos-test-xxxxxxxxxx", base_url=base)
        untrusted = {"cache_control": {"ttl": "1h"}, "max_results": 999}
        with self.assertRaises(ValueError) as e:
            mem.search("real", "alice", **untrusted)
        self.assertIn("max_results", str(e.exception))

        mem.search("real", "alice", cache_control={"ttl": "1h"})
        sent = self.body(srv)
        self.assertEqual(sent["user_id"], "alice")
        self.assertEqual(sent["query"], "real")
        self.assertEqual(sent["max_results"], 10, "opts must not raise the limit via the wire name")
        self.assertEqual(sent["cache_control"], {"ttl": "1h"})


@unittest.skipUnless(HAVE_HTTPX, "httpx not installed")
class AsyncCacheControlTest(unittest.IsolatedAsyncioTestCase):

    async def test_cache_control_reaches_the_api(self):
        srv, base = scripted_server([(200, {}, '{"memories":[]}')])
        self.addCleanup(srv.shutdown)
        mem = AsyncClient("wos-test-xxxxxxxxxx", base_url=base)
        try:
            await mem.search("q", "alice", cache_control={"ttl": "5m"})
        finally:
            await mem.aclose()
        self.assertEqual(json.loads(srv.seen[0]["body"])["cache_control"], {"ttl": "5m"})


class VersionDriftGuardTest(unittest.TestCase):

    def test_version_matches_pyproject(self):
        import os
        import re

        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        toml = open(os.path.join(root, "pyproject.toml"), encoding="utf-8").read()
        m = re.search(r'^version\s*=\s*"([^"]+)"', toml, re.M)
        self.assertIsNotNone(m, "pyproject.toml must carry a version")
        self.assertEqual(
            __version__,
            m.group(1),
            "if __version__ and pyproject.toml disagree, the release misreports its own number",
        )


class StoreIdWarningBoundTest(unittest.TestCase):

    def setUp(self):
        wontopos._reset_warning_state()
        self.addCleanup(wontopos._reset_warning_state)
        self.mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")

    @contextlib.contextmanager
    def assertNoWarning(self):
        records = []

        class _Catch(logging.Handler):
            def emit(self, record):
                records.append(record)

        log = logging.getLogger("wontopos")
        h = _Catch()
        log.addHandler(h)
        try:
            yield
        finally:
            log.removeHandler(h)
        self.assertEqual(
            [r.getMessage() for r in records if r.levelno >= logging.WARNING], [],
            "a warning fired where none should",
        )

    def test_bounded_and_evicts_oldest(self):
        cap = wontopos._WARNED_STORE_IDS_MAX
        with self.assertLogs("wontopos", level="WARNING"):
            for i in range(cap * 3):
                self.mem._uid(f"user.{i}@example.com")
        self.assertLessEqual(
            len(wontopos._warned_store_ids), cap,
            "exceeding the cap means it grows per user",
        )
        with self.assertLogs("wontopos", level="WARNING"):
            self.mem._uid("user.0@example.com")
        with self.assertNoWarning():
            self.mem._uid("user.0@example.com")

    def test_a_late_first_collision_still_warns(self):
        cap = wontopos._WARNED_STORE_IDS_MAX
        with self.assertLogs("wontopos", level="WARNING"):
            for i in range(cap * 2):
                self.mem._uid(f"user.{i}@example.com")
        with self.assertLogs("wontopos", level="WARNING") as cm:
            self.mem._uid("zzz.late@example.com")
        self.assertTrue(any("zzz.late@example.com" in m for m in cm.output))

    def test_clean_ids_are_not_recorded(self):
        with self.assertNoWarning():
            for i in range(200):
                self.mem._uid(f"user_{i}")
        self.assertEqual(len(wontopos._warned_store_ids), 0, "an already-canonical id has no reason to consume the cap")


class StalledBodyIsNotRetriedTest(unittest.TestCase):

    def stalling_server(self, stall=4.0):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(16)
        requests_seen = []

        def handle(sock):
            try:
                while True:
                    data = sock.recv(65536)
                    if not data:
                        return
                    requests_seen.append(1)
                    sock.sendall(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                        b"Content-Length: 200\r\n\r\n"
                    )
                    time.sleep(stall)
            except OSError:
                pass
            finally:
                try:
                    sock.close()
                except OSError:
                    pass

        def run():
            while True:
                try:
                    sock, _ = srv.accept()
                except OSError:
                    return
                threading.Thread(target=handle, args=(sock,), daemon=True).start()

        threading.Thread(target=run, daemon=True).start()
        self.addCleanup(srv.close)
        return requests_seen, f"http://127.0.0.1:{srv.getsockname()[1]}"

    def test_a_read_timeout_in_the_body_is_not_retried(self):
        seen, base = self.stalling_server()
        mem = Client("wos-test-xxxxxxxxxx", user_id="u", base_url=base, retries=2, timeout=1.0)
        with self.assertRaises(APIConnectionError):
            mem.list_stores()
        time.sleep(0.3)
        self.assertEqual(len(seen), 1, "a stalled body was retried — count requests, not connections")


class BodyDropRetryTest(unittest.IsolatedAsyncioTestCase):

    def test_sync_retries_an_idempotent_get_after_the_body_drops(self):
        accepted, base = dropping_server(["TRUNCATE", '{"collections": []}'])
        mem = Client("wos-test-xxxxxxxxxx", user_id="u", base_url=base, retries=2)
        self.assertEqual(mem.list_stores(), [])
        self.assertEqual(len(accepted), 2, "the dropped body was not retried")

    def test_sync_does_not_retry_a_write_after_the_body_drops(self):
        accepted, base = dropping_server(["TRUNCATE", '{"status": "created"}'])
        mem = Client("wos-test-xxxxxxxxxx", user_id="u", base_url=base, retries=2)
        with self.assertRaises(APIConnectionError):
            mem.create_store()
        self.assertEqual(len(accepted), 1, "a write without a key was retried")

    def test_sync_retries_a_memory_write_after_the_body_drops(self):
        accepted, base = dropping_server(["TRUNCATE", '{"id": "m1"}'])
        mem = Client("wos-test-xxxxxxxxxx", user_id="u", base_url=base, retries=2)
        self.assertEqual(mem.add("hello")["id"], "m1")
        self.assertEqual(len(accepted), 2)

    @needs_httpx
    async def test_async_agrees_with_sync_on_both(self):
        accepted, base = dropping_server(["TRUNCATE", '{"collections": []}'])
        async with AsyncClient("wos-test-xxxxxxxxxx", user_id="u", base_url=base, retries=2) as mem:
            self.assertEqual(await mem.list_stores(), [])
        self.assertEqual(len(accepted), 2)

        accepted2, base2 = dropping_server(["TRUNCATE", '{"status": "created"}'])
        async with AsyncClient("wos-test-xxxxxxxxxx", user_id="u", base_url=base2, retries=2) as mem:
            with self.assertRaises(APIConnectionError):
                await mem.create_store()
        self.assertEqual(len(accepted2), 1)

        accepted3, base3 = dropping_server(["TRUNCATE", '{"id": "m1"}'])
        async with AsyncClient("wos-test-xxxxxxxxxx", user_id="u", base_url=base3, retries=2) as mem:
            self.assertEqual((await mem.add("hello"))["id"], "m1")
        self.assertEqual(len(accepted3), 2)


class SyncAsyncDropParityTest(unittest.IsolatedAsyncioTestCase):

    def test_sync_retries_idempotent_on_mid_stream_drop(self):
        accepted, base = dropping_server([None, '{"collections": []}'])
        mem = Client("wos-test-xxxxxxxxxx", base_url=base, retries=2)
        mem.list_stores()
        self.assertEqual(len(accepted), 2, "sync did not retry an idempotent GET")

    @needs_httpx
    async def test_async_retries_idempotent_on_mid_stream_drop(self):
        accepted, base = dropping_server([None, '{"collections": []}'])
        mem = AsyncClient("wos-test-xxxxxxxxxx", base_url=base, retries=2)
        try:
            await mem.list_stores()
        finally:
            await mem.aclose()
        self.assertEqual(len(accepted), 2, "if async does not retry the same GET, the two clients diverge")

    @needs_httpx
    async def test_async_does_not_retry_a_write_on_mid_stream_drop(self):
        accepted, base = dropping_server([None, '{"status": "created"}'])
        mem = AsyncClient("wos-test-xxxxxxxxxx", base_url=base, retries=2)
        try:
            with self.assertRaises(APIConnectionError):
                await mem.create_store("alice")
        finally:
            await mem.aclose()
        self.assertEqual(len(accepted), 1, "retrying a POST without a key can apply it twice")

    @needs_httpx
    async def test_async_retries_a_memory_write_on_mid_stream_drop(self):
        accepted, base = dropping_server([None, '{"id": "m1"}'])
        mem = AsyncClient("wos-test-xxxxxxxxxx", base_url=base, retries=2)
        try:
            self.assertEqual((await mem.add("x", "alice"))["id"], "m1")
        finally:
            await mem.aclose()
        self.assertEqual(len(accepted), 2)


class ErrorStatusOutranksTheDeadBodyTest(unittest.IsolatedAsyncioTestCase):

    IMAGE_ID = "11111111-1111-1111-1111-111111111111"

    def test_sync_reports_the_status(self):
        _, base = dropping_server([(404, "TRUNCATE")])
        mem = Client("wos-test-xxxxxxxxxx", base_url=base, retries=0)
        with self.assertRaises(NotFoundError):
            mem.get_image(memory_id=self.IMAGE_ID)

    @needs_httpx
    async def test_async_reports_the_same_status(self):
        _, base = dropping_server([(404, "TRUNCATE")])
        mem = AsyncClient("wos-test-xxxxxxxxxx", base_url=base, retries=0)
        try:
            with self.assertRaises(NotFoundError):
                await mem.get_image(memory_id=self.IMAGE_ID)
        finally:
            await mem.aclose()

    @needs_httpx
    async def test_async_still_retries_a_429_whose_body_dies(self):
        accepted, base = dropping_server(
            [(429, "TRUNCATE"), (200, "image/webp", "PNGBYTES")]
        )
        mem = AsyncClient("wos-test-xxxxxxxxxx", base_url=base, retries=2)
        try:
            data, mime = await mem.get_image(memory_id=self.IMAGE_ID)
        finally:
            await mem.aclose()
        self.assertEqual(data, b"PNGBYTES")
        self.assertIn("image/webp", mime)
        self.assertEqual(len(accepted), 2, "a 429 read as a connection error is never retried")


class RecallContractTests(unittest.TestCase):

    def make(self, script, **kw):
        srv, base = scripted_server(script)
        self.addCleanup(srv.shutdown)
        return srv, Client("wos-test-xxxxxxxxxx", base_url=base, **kw)

    def test_recall_refuses_a_count_outside_5_20_without_sending(self):
        srv, mem = self.make([(200, {}, "{}")])
        for bad in (0, 1, 4, 21, 500):
            with self.assertRaises(ValueError) as cm:
                mem.recall("q", "alice", limit=bad)
            self.assertIn("between 5 and 20", str(cm.exception))
        self.assertEqual(len(srv.seen), 0, "nothing may reach the wire")

    def test_recall_refuses_a_context_limit_outside_0_20_and_allows_the_range(self):
        srv, mem = self.make([(200, {}, "{}")])
        for bad in (-1, 21, 100):
            with self.assertRaises(ValueError):
                mem.recall("q", "alice", context_limit=bad)
        mem.recall("q", "alice", limit=20, context_limit=0)
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body["context_limit"], 0)
        self.assertEqual(body["limit"], 20)

    def test_status_zero_belongs_to_the_connection_error_alone(self):
        import re
        src = open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "src", "wontopos", "__init__.py"), encoding="utf-8").read()
        self.assertEqual(re.findall(r"WosError\(\s*0\s*,", src), [],
                         "these construct a WosError with status 0")

    def test_replayed_never_overwrites_what_the_service_sent(self):
        srv, mem = self.make([
            (200, {"Idempotent-Replayed": "true"}, '{"id": "m1", "replayed": false}'),
            (200, {"Idempotent-Replayed": "true"}, '{"id": "m2"}'),
        ])
        said = mem.add("x", user_id="alice", idempotency_key="k1")
        self.assertIs(said["replayed"], False, "the service said false; we must not flip it")
        silent = mem.add("x", user_id="alice", idempotency_key="k2")
        self.assertIs(silent["replayed"], True, "the service said nothing; the header answers")


class DeadlineTests(unittest.TestCase):

    def make(self, script, **kw):
        srv, base = scripted_server(script)
        self.addCleanup(srv.shutdown)
        return srv, Client("wos-test-xxxxxxxxxx", base_url=base, **kw)

    def test_deadline_bounds_the_whole_call_not_one_attempt(self):
        srv, mem = self.make([
            (429, {"Retry-After": "5"}, '{"error": "rate limited"}'),
            (200, {}, "{}"),
        ], deadline=0.2)
        t0 = time.monotonic()
        with self.assertRaises(RateLimitError):
            mem.search("q", user_id="alice")
        spent = time.monotonic() - t0
        self.assertLess(spent, 0.15, f"spent {spent:.2f}s against a 0.2s budget")

    def test_a_clone_keeps_the_deadline(self):
        srv, mem = self.make([(429, {"Retry-After": "5"}, "{}")], deadline=0.2)
        with self.assertRaises(RateLimitError):
            mem.with_model("tablet-2").search("q", user_id="alice")

    def test_a_non_positive_deadline_means_no_budget(self):
        for unset in (0, -1):
            self.assertIsNone(Client("wos-test-xxxxxxxxxx", deadline=unset)._deadline)


class PooledConnectionWatchdogTest(unittest.TestCase):

    def test_a_finished_attempt_leaves_the_pooled_connection_alone(self):
        from http.server import ThreadingHTTPServer
        from unittest import mock
        import urllib3.response

        seen, b_arrived = [], threading.Event()

        class KeepAlive(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _serve(self):
                n = int(self.headers.get("Content-Length") or 0)
                if n:
                    self.rfile.read(n)
                seen.append((self.command, self.client_address[1]))
                if self.command == "POST":
                    b_arrived.set()
                    time.sleep(0.8)
                    data = b'{"id": "m1", "status": "stored"}'
                else:
                    data = b'{"models": []}'
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except OSError:
                    pass

            do_GET = do_POST = _serve

            def log_message(self, *_):
                pass

        srv = ThreadingHTTPServer(("127.0.0.1", 0), KeepAlive)
        srv.daemon_threads = True
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        mem = Client(KEY, base_url=f"http://127.0.0.1:{srv.server_port}", timeout=5)
        self.addCleanup(mem.close)
        fast = mem.with_deadline(0.3)
        released, go, out = threading.Event(), threading.Event(), {}
        release = urllib3.response.HTTPResponse.release_conn

        def held_after_release(resp):
            had = resp._connection is not None
            release(resp)
            if had and threading.current_thread().name == "A" and not released.is_set():
                released.set()
                go.wait(5)

        def run(name, fn):
            try:
                out[name] = fn()
            except Exception as e:
                out[name] = e

        with mock.patch.object(urllib3.response.HTTPResponse, "release_conn", held_after_release):
            t0 = time.monotonic()
            a = threading.Thread(target=run, args=("A", fast.list_models), name="A")
            b = threading.Thread(target=lambda: released.wait(5) and run("B", lambda: mem.add("x", "alice")))
            a.start()
            b.start()
            self.assertTrue(b_arrived.wait(5))
            time.sleep(max(0.0, t0 + 0.45 - time.monotonic()))
            go.set()
            a.join(5)
            b.join(5)

        self.assertEqual([c for c, _ in seen], ["GET", "POST"])
        self.assertEqual(seen[0][1], seen[1][1])
        self.assertEqual(out.get("B"), {"id": "m1", "status": "stored"})
        self.assertEqual(out.get("A"), [])



class BuilderStoreIdGuardTest(unittest.TestCase):

    BAD = ("", " ", "\t", None, 0, 42, 3.5, [], {})
    CLASSES = (Client, AsyncClient) if HAVE_HTTPX else (Client,)

    def test_with_user_refuses_every_unusable_id(self):
        for cls in self.CLASSES:
            bound = cls("wos-test-xxxxxxxxxx", user_id="tenant-A")
            for bad in self.BAD:
                with self.assertRaises(ValueError, msg=f"{cls.__name__}.with_user({bad!r})") as cm:
                    bound.with_user(bad)
                self.assertIn("non-blank string", str(cm.exception))

    def test_constructor_refuses_every_unusable_id(self):
        for cls in self.CLASSES:
            for bad in self.BAD:
                with self.assertRaises(ValueError, msg=f"{cls.__name__}(user_id={bad!r})") as cm:
                    cls("wos-test-xxxxxxxxxx", user_id=bad)
                self.assertIn("non-blank string", str(cm.exception))

    def test_the_zero_setup_path_and_other_clones_still_work(self):
        for cls in self.CLASSES:
            self.assertEqual(cls("wos-test-xxxxxxxxxx")._user_id, "default")
            bound = cls("wos-test-xxxxxxxxxx", user_id="tenant-A")
            self.assertEqual(bound.with_model("tablet-2")._user_id, "tenant-A")
            self.assertEqual(bound.with_user("tenant-B")._user_id, "tenant-B")


class ListEngramsKeepsUnknownFieldsTest(unittest.TestCase):

    def make(self, body):
        srv, base = scripted_server([(200, {}, body)])
        self.addCleanup(srv.shutdown)
        return Client("wos-test-xxxxxxxxxx", user_id="u", base_url=base)

    def test_a_field_added_later_still_arrives(self):
        mem = self.make(
            '{"engrams": [{"name": "deep_recall"}], "forms": [{"name": "memoir"}], '
            '"note": null, "a_field_added_later": {"n": 7}}'
        )
        r = mem.list_engrams()
        self.assertEqual(r["a_field_added_later"], {"n": 7}, "a field added later was dropped")
        self.assertEqual(r["engrams"][0]["name"], "deep_recall")
        self.assertIsNone(r["note"])

    def test_non_objects_and_a_non_string_note_are_dropped(self):
        mem = self.make(
            '{"engrams": [null, "x", {"name": "deep_recall"}], "forms": [1], "note": 42}'
        )
        r = mem.list_engrams()
        self.assertEqual(r["engrams"], [{"name": "deep_recall"}])
        self.assertEqual(r["forms"], [])
        self.assertIsNone(r["note"])


class MalformedErrorBodyTest(unittest.TestCase):

    def test_a_non_string_message_still_builds_a_wos_error(self):
        for body in ('{"error": {"message": 42}}', '{"message": null}',
                     '{"error": {"message": ["a", "b"]}}', '{"error": {"message": {"k": 1}}}'):
            e = wontopos._parse_error(400, body)
            self.assertIsInstance(e, WosError, f"body {body} did not build a WosError")
            self.assertEqual(e.status, 400)
            self.assertIsInstance(str(e), str)

    def test_a_long_non_string_message_is_still_capped(self):
        body = '{"error": {"message": ' + str(list(range(20000))) + '}}'
        e = wontopos._parse_error(400, body)
        self.assertLessEqual(len(str(e)), wontopos._MAX_ERR_MSG + 40)


class KeyCharsetTest(unittest.TestCase):

    def test_a_non_latin1_key_is_refused_with_a_useful_message(self):
        for bad in ("wos\u2013live\u2013" + "a" * 30, "wos-live-\u00e9" + "a" * 30, "wos-live-\uff0d" + "a" * 30):
            with self.assertRaises(ValueError) as cm:
                Client(bad)
            self.assertIn("api_key", str(cm.exception))
            self.assertIn("non-ASCII", str(cm.exception))

    def test_an_ordinary_key_still_works(self):
        self.assertTrue(Client("wos-live-" + "k" * 40))


class ListShapeTest(unittest.TestCase):

    def test_a_dict_where_a_list_was_promised_becomes_an_empty_list(self):
        for path_body, call in (
            ('{"models": {"tablet-2": {"id": "tablet-2"}}}', lambda m: m.list_models()),
            ('{"collections": {"a": 1}}', lambda m: m.list_stores()),
            ('{"engrams": "oops"}', lambda m: m.list_engrams()["engrams"]),
        ):
            srv, base = scripted_server([(200, {}, path_body)])
            self.addCleanup(srv.shutdown)
            got = call(Client("wos-test-xxxxxxxxxx", base_url=base))
            self.assertEqual(got, [], path_body)
            for _ in got:
                pass


KEY = "wos-test-xxxxxxxxxx"
IMAGE_ID = "11111111-1111-1111-1111-111111111111"


def envelope(etype, message, request_id, **extra):
    return json.dumps({"type": "error", "error": {
        "type": etype, "message": message, "request_id": request_id, **extra}})


LOCK_409 = envelope("conflict_error", "Another write to store 'alice' is already in flight. "
                    "Nothing was stored or changed.", "req_lock", retry_after_ms=100)
COLLIDE_409 = envelope("conflict_error", "Store id 'team_a' collides with existing store 'team-a'.",
                       "req_collide", conflicts_with="team-a")
PLAIN_409 = envelope("conflict_error", "conflict", "req_plain")
RATE_429 = envelope("rate_limit_error", "slow down", "req_429")


@contextlib.contextmanager
def no_wontopos_warning(case):
    records = []

    class _Catch(logging.Handler):
        def emit(self, record):
            records.append(record)

    log = logging.getLogger("wontopos")
    h = _Catch()
    log.addHandler(h)
    try:
        yield
    finally:
        log.removeHandler(h)
    case.assertEqual([r.getMessage() for r in records if r.levelno >= logging.WARNING], [])


def peak_rss_mb():
    import resource
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1 << 20) if sys.platform == "darwin" else peak / 1024


def zeros_gzip(n_bytes):
    import zlib
    c = zlib.compressobj(9, zlib.DEFLATED, 31)
    block = bytes(1 << 20)
    parts = [c.compress(block) for _ in range(n_bytes >> 20)]
    parts.append(c.flush())
    return b"".join(parts)


class SyncMake:
    def make(self, script, **kw):
        srv, base = scripted_server(script)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return srv, Client(KEY, base_url=base, **kw)


class AsyncMake:
    def amake(self, script, **kw):
        if not HAVE_HTTPX:
            self.skipTest("httpx not installed (pip install 'wontopos[async]')")
        srv, base = scripted_server(script)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return srv, AsyncClient(KEY, base_url=base, **kw)


class ConflictRetryTest(SyncMake, unittest.TestCase):

    def test_a_write_lock_409_is_retried_even_on_a_write(self):
        srv, mem = self.make([(409, {}, LOCK_409), (200, {}, '{"id": "m1", "status": "stored"}')])
        self.assertEqual(mem.add("x", "alice")["id"], "m1")
        self.assertEqual(len(srv.seen), 2)

    def test_the_wait_is_at_least_retry_after_ms(self):
        slow = envelope("conflict_error", "in flight", "req_lock", retry_after_ms=900)
        srv, mem = self.make([(409, {}, slow), (200, {}, '{"id": "m1"}')])
        t0 = time.monotonic()
        mem.add("x", "alice")
        self.assertGreaterEqual(time.monotonic() - t0, 0.85)

    def test_a_store_id_collision_is_never_retried(self):
        srv, mem = self.make([(409, {}, COLLIDE_409), (200, {}, "{}")], retries=2)
        with self.assertRaises(wontopos.ConflictError) as cm:
            mem.create_store("team_a")
        self.assertEqual(cm.exception.conflicts_with, "team-a")
        self.assertEqual(cm.exception.type, "conflict_error")
        self.assertEqual(cm.exception.request_id, "req_collide")
        self.assertEqual(len(srv.seen), 1)

    def test_a_409_without_retry_after_ms_is_final(self):
        srv, mem = self.make([(409, {}, PLAIN_409), (200, {}, "{}")])
        with self.assertRaises(wontopos.ConflictError) as cm:
            mem.add("x", "alice")
        self.assertIsNone(cm.exception.conflicts_with)
        self.assertEqual(len(srv.seen), 1)

    def test_retries_zero_does_not_retry_the_lock(self):
        srv, mem = self.make([(409, {}, LOCK_409), (200, {}, "{}")], retries=0)
        with self.assertRaises(wontopos.ConflictError):
            mem.add("x", "alice")
        self.assertEqual(len(srv.seen), 1)

    def test_a_lock_wait_past_the_deadline_raises_the_409(self):
        slow = envelope("conflict_error", "in flight", "req_lock", retry_after_ms=5000)
        srv, mem = self.make([(409, {}, slow), (200, {}, "{}")], deadline=0.4)
        t0 = time.monotonic()
        with self.assertRaises(wontopos.ConflictError) as cm:
            mem.add("x", "alice")
        self.assertLess(time.monotonic() - t0, 0.3)
        self.assertEqual(cm.exception.request_id, "req_lock")
        self.assertEqual(len(srv.seen), 1)

    def test_get_image_retries_the_lock_too(self):
        srv, mem = self.make([(409, {}, LOCK_409), (200, {"Content-Type": "image/webp"}, "PNGBYTES")])
        data, _ = mem.get_image(memory_id=IMAGE_ID)
        self.assertEqual(data, b"PNGBYTES")
        self.assertEqual(len(srv.seen), 2)


@needs_httpx
class AsyncConflictRetryTest(AsyncMake, unittest.IsolatedAsyncioTestCase):
    async def test_a_write_lock_409_is_retried_even_on_a_write(self):
        srv, mem = self.amake([(409, {}, LOCK_409), (200, {}, '{"id": "m1"}')])
        async with mem:
            self.assertEqual((await mem.add("x", "alice"))["id"], "m1")
        self.assertEqual(len(srv.seen), 2)

    async def test_a_store_id_collision_is_never_retried(self):
        srv, mem = self.amake([(409, {}, COLLIDE_409), (200, {}, "{}")])
        async with mem:
            with self.assertRaises(wontopos.ConflictError) as cm:
                await mem.create_store("team_a")
        self.assertEqual(cm.exception.conflicts_with, "team-a")
        self.assertEqual(len(srv.seen), 1)

    async def test_get_image_retries_the_lock_too(self):
        srv, mem = self.amake([(409, {}, LOCK_409), (200, {"Content-Type": "image/webp"}, "PNGBYTES")])
        async with mem:
            data, _ = await mem.get_image(memory_id=IMAGE_ID)
        self.assertEqual(data, b"PNGBYTES")
        self.assertEqual(len(srv.seen), 2)


class DeadlineReportsTheResponseTest(SyncMake, unittest.TestCase):

    def test_a_429_that_cannot_wait_raises_rate_limit_error(self):
        srv, mem = self.make([(429, {"Retry-After": "5"}, RATE_429), (200, {}, "{}")], deadline=0.4)
        t0 = time.monotonic()
        with self.assertRaises(RateLimitError) as cm:
            mem.add("x", "alice")
        self.assertLess(time.monotonic() - t0, 0.3)
        self.assertEqual(cm.exception.status, 429)
        self.assertEqual(cm.exception.request_id, "req_429")
        self.assertEqual(len(srv.seen), 1)

    def test_a_503_on_a_read_that_cannot_wait_raises_server_error(self):
        busy = envelope("overloaded_error", "busy", "req_503")
        srv, mem = self.make([(503, {"Retry-After": "5"}, busy), (200, {}, '{"models": []}')], deadline=0.4)
        with self.assertRaises(wontopos.ServerError) as cm:
            mem.list_models()
        self.assertEqual((cm.exception.status, cm.exception.request_id), (503, "req_503"))

    def test_get_image_reports_the_429(self):
        srv, mem = self.make([(429, {"Retry-After": "5"}, RATE_429)], deadline=0.4)
        with self.assertRaises(RateLimitError):
            mem.get_image(memory_id=IMAGE_ID)

    def test_a_budget_spent_with_no_response_is_still_status_0(self):
        mem = Client(KEY, base_url=closed_port_url(), retries=5, deadline=0.3)
        with self.assertRaises(APIConnectionError) as cm:
            mem.list_stores()
        self.assertEqual(cm.exception.status, 0)
        self.assertIn("deadline of 0.3s exhausted", str(cm.exception))

    STALL_AFTER_429 = [(429, {"Retry-After": "0"}, RATE_429, 0), None]

    def test_a_read_cut_by_the_deadline_reports_the_answer_before_it(self):
        seen, base = staged_server(self, self.STALL_AFTER_429)
        mem = Client(KEY, base_url=base, timeout=10, deadline=0.8)
        t0 = time.monotonic()
        with self.assertRaises(RateLimitError) as cm:
            mem.list_stores()
        self.assertLess(time.monotonic() - t0, 1.6)
        self.assertEqual((cm.exception.status, cm.exception.request_id), (429, "req_429"))
        self.assertEqual(len(seen), 2)

    def test_get_image_cut_by_the_deadline_reports_the_answer_before_it(self):
        seen, base = staged_server(self, self.STALL_AFTER_429)
        mem = Client(KEY, base_url=base, timeout=10, deadline=0.8)
        with self.assertRaises(RateLimitError):
            mem.get_image(memory_id=IMAGE_ID)
        self.assertEqual(len(seen), 2)

    def test_a_search_cut_by_the_deadline_reports_the_answer_before_it(self):
        seen, base = staged_server(self, self.STALL_AFTER_429)
        mem = Client(KEY, base_url=base, timeout=10, deadline=0.8)
        with self.assertRaises(RateLimitError):
            mem.search("q", "alice")
        self.assertEqual(len(seen), 2)

    def test_a_write_cut_by_the_deadline_is_status_0(self):
        seen, base = staged_server(self, self.STALL_AFTER_429)
        mem = Client(KEY, base_url=base, timeout=10, deadline=0.8)
        with self.assertRaises(APIConnectionError) as cm:
            mem.add("x", "alice")
        self.assertIn("deadline of 0.8s exhausted", str(cm.exception))
        self.assertEqual(len(seen), 2)

    def test_a_timeout_that_is_not_the_deadline_is_status_0(self):
        seen, base = staged_server(self, self.STALL_AFTER_429)
        mem = Client(KEY, base_url=base, timeout=0.5, deadline=30)
        with self.assertRaises(APIConnectionError) as cm:
            mem.list_stores()
        self.assertIn("request timed out after 0.5s", str(cm.exception))

    def test_a_deadline_spent_during_the_wait_reports_the_answer(self):
        call = wontopos._Call("POST", "/api/v1/memory/add", 2, 0.05)
        self.assertEqual(call.after_status(429, {"Retry-After": "0"}, RATE_429.encode()), 0.0)
        time.sleep(0.1)
        with self.assertRaises(RateLimitError) as cm:
            call.clock(10)
        self.assertEqual(cm.exception.request_id, "req_429")
        call = wontopos._Call("POST", "/api/v1/memory/store", 2, 0.2)
        self.assertEqual(call.after_status(429, {"Retry-After": "0"}, RATE_429.encode()), 0.0)
        clock = call.clock(10)
        time.sleep(0.25)
        with self.assertRaises(RateLimitError):
            call.after_failure(wontopos._Failure("ConnectTimeout (timed out)", never_sent=True), clock)
        bare = wontopos._Call("GET", "/x", 2, 0.01)
        time.sleep(0.05)
        with self.assertRaises(APIConnectionError):
            bare.clock(10)

    NEVER_SENT = wontopos._Failure("ConnectTimeout (timed out)", never_sent=True)

    def test_the_deadline_ends_a_last_attempt_that_was_never_sent(self):
        for retries in (1, 2):
            with self.subTest(retries=retries):
                call = wontopos._Call("GET", "/api/v1/x", retries, 0.3)
                self.assertEqual(call.after_status(429, {"Retry-After": "0"}, RATE_429.encode()), 0.0)
                call.attempt = 1
                clock = call.clock(10)
                time.sleep(0.35)
                with self.assertRaises(RateLimitError):
                    call.after_failure(self.NEVER_SENT, clock)
        call = wontopos._Call("POST", "/api/v1/x", 0, 0.1)
        clock = call.clock(10)
        time.sleep(0.15)
        with self.assertRaises(APIConnectionError) as cm:
            call.after_failure(self.NEVER_SENT, clock)
        self.assertEqual(cm.exception.message, "deadline of 0.1s exhausted")

    def test_attempts_that_run_out_before_the_deadline_report_the_last_failure(self):
        call = wontopos._Call("GET", "/api/v1/x", 1, 30)
        call.after_status(429, {"Retry-After": "0"}, RATE_429.encode())
        call.attempt = 1
        clock = call.clock(10)
        with self.assertRaises(APIConnectionError) as cm:
            call.after_failure(wontopos._Failure("ConnectError (connection refused)", never_sent=True), clock)
        self.assertEqual(cm.exception.message, "network error: GET /api/v1/x: ConnectError (connection refused)")

    def test_a_dropped_read_with_no_time_to_retry_reports_the_answer_before_it(self):
        call = wontopos._Call("GET", "/api/v1/x", 2, 0.3)
        call.after_status(429, {"Retry-After": "0"}, RATE_429.encode())
        clock = call.clock(10)
        dropped = wontopos._Failure("ReadError (connection closed before the response completed)",
                                    dropped=True)
        with self.assertRaises(RateLimitError):
            call.after_failure(dropped, clock)
        call = wontopos._Call("POST", "/api/v1/x", 2, 0.3)
        call.after_status(429, {"Retry-After": "0"}, RATE_429.encode())
        with self.assertRaises(APIConnectionError) as cm:
            call.after_failure(dropped, call.clock(10))
        self.assertIn("network error: POST /api/v1/x: ReadError", cm.exception.message)


@needs_httpx
class AsyncDeadlineReportsTheResponseTest(AsyncMake, unittest.IsolatedAsyncioTestCase):
    async def test_a_429_that_cannot_wait_raises_rate_limit_error(self):
        srv, mem = self.amake([(429, {"Retry-After": "5"}, RATE_429), (200, {}, "{}")], deadline=0.4)
        async with mem:
            with self.assertRaises(RateLimitError) as cm:
                await mem.add("x", "alice")
        self.assertEqual(cm.exception.request_id, "req_429")
        self.assertEqual(len(srv.seen), 1)

    async def test_get_image_reports_the_429(self):
        srv, mem = self.amake([(429, {"Retry-After": "5"}, RATE_429)], deadline=0.4)
        async with mem:
            with self.assertRaises(RateLimitError):
                await mem.get_image(memory_id=IMAGE_ID)

    async def test_a_read_cut_by_the_deadline_reports_the_answer_before_it(self):
        seen, base = staged_server(self, DeadlineReportsTheResponseTest.STALL_AFTER_429)
        async with AsyncClient(KEY, base_url=base, timeout=10, deadline=0.8) as mem:
            t0 = time.monotonic()
            with self.assertRaises(RateLimitError) as cm:
                await mem.list_stores()
            self.assertLess(time.monotonic() - t0, 1.6)
        self.assertEqual(cm.exception.request_id, "req_429")
        self.assertEqual(len(seen), 2)

    async def test_get_image_cut_by_the_deadline_reports_the_answer_before_it(self):
        seen, base = staged_server(self, DeadlineReportsTheResponseTest.STALL_AFTER_429)
        async with AsyncClient(KEY, base_url=base, timeout=10, deadline=0.8) as mem:
            with self.assertRaises(RateLimitError):
                await mem.get_image(memory_id=IMAGE_ID)

    async def test_a_write_cut_by_the_deadline_is_status_0(self):
        seen, base = staged_server(self, DeadlineReportsTheResponseTest.STALL_AFTER_429)
        async with AsyncClient(KEY, base_url=base, timeout=10, deadline=0.8) as mem:
            with self.assertRaises(APIConnectionError) as cm:
                await mem.add("x", "alice")
        self.assertIn("deadline of 0.8s exhausted", str(cm.exception))
        self.assertEqual(len(seen), 2)


class SlowErrorBodyTest(unittest.IsolatedAsyncioTestCase):

    SLOW_429 = [(429, {"Retry-After": "0"}, RATE_429, 3), (200, {}, '{"id": "m1"}', 0)]
    SLOW_404 = [(404, {}, envelope("not_found_error", "Not found.", "r"), 3)]

    def test_sync_retries_the_429(self):
        seen, base = staged_server(self, self.SLOW_429)
        mem = Client(KEY, base_url=base, timeout=0.5)
        self.assertEqual(mem.add("x", "alice")["id"], "m1")
        self.assertEqual(len(seen), 2)

    @needs_httpx
    async def test_async_retries_the_429(self):
        seen, base = staged_server(self, self.SLOW_429)
        async with AsyncClient(KEY, base_url=base, timeout=0.5) as mem:
            self.assertEqual((await mem.add("x", "alice"))["id"], "m1")
        self.assertEqual(len(seen), 2)

    STALLED_429 = [(429, {"Retry-After": "0"}, RATE_429, 8), (200, {}, '{"id": "m1"}', 0)]

    def test_sync_retries_a_stalled_429_without_waiting_for_its_body(self):
        seen, base = staged_server(self, self.STALLED_429)
        mem = Client(KEY, base_url=base, timeout=10)
        t0 = time.monotonic()
        self.assertEqual(mem.add("x", "alice")["id"], "m1")
        self.assertLess(time.monotonic() - t0, 3)
        self.assertEqual(len(seen), 2)

    @needs_httpx
    async def test_async_retries_a_stalled_429_without_waiting_for_its_body(self):
        seen, base = staged_server(self, self.STALLED_429)
        async with AsyncClient(KEY, base_url=base, timeout=10) as mem:
            t0 = time.monotonic()
            self.assertEqual((await mem.add("x", "alice"))["id"], "m1")
            self.assertLess(time.monotonic() - t0, 3)
        self.assertEqual(len(seen), 2)

    FINAL_429S = ([(429, {"Retry-After": "60"}, RATE_429, 1.5)], [(429, {"Retry-After": "0"}, RATE_429, 1.5)])

    def test_sync_waits_for_the_body_of_a_429_that_is_not_retried(self):
        for entries, retries in zip(self.FINAL_429S, (2, 0)):
            seen, base = staged_server(self, entries)
            with self.assertRaises(RateLimitError) as cm:
                Client(KEY, base_url=base, timeout=10, retries=retries).stats("alice")
            self.assertEqual(cm.exception.request_id, "req_429")

    @needs_httpx
    async def test_async_waits_for_the_body_of_a_429_that_is_not_retried(self):
        for entries, retries in zip(self.FINAL_429S, (2, 0)):
            seen, base = staged_server(self, entries)
            async with AsyncClient(KEY, base_url=base, timeout=10, retries=retries) as mem:
                with self.assertRaises(RateLimitError) as cm:
                    await mem.stats("alice")
            self.assertEqual(cm.exception.request_id, "req_429")

    def test_sync_404(self):
        seen, base = staged_server(self, self.SLOW_404)
        mem = Client(KEY, base_url=base, timeout=0.5)
        with self.assertRaises(NotFoundError) as cm:
            mem.get("alice", IMAGE_ID)
        self.assertEqual(cm.exception.message,
                         "HTTP 404 (the error body could not be read: request timed out after 0.5s)")

    @needs_httpx
    async def test_async_404(self):
        seen, base = staged_server(self, self.SLOW_404)
        async with AsyncClient(KEY, base_url=base, timeout=0.5) as mem:
            with self.assertRaises(NotFoundError) as cm:
                await mem.get("alice", IMAGE_ID)
        self.assertEqual(cm.exception.message,
                         "HTTP 404 (the error body could not be read: request timed out after 0.5s)")


class RetryAfterTest(SyncMake, unittest.TestCase):

    def test_a_retry_after_beyond_the_cap_raises_at_once(self):
        srv, mem = self.make([(429, {"Retry-After": "3540"}, RATE_429), (200, {}, "{}")], retries=2)
        t0 = time.monotonic()
        with self.assertRaises(RateLimitError) as cm:
            mem.usage(7)
        self.assertLess(time.monotonic() - t0, 1.0)
        self.assertEqual(cm.exception.retry_after, 3540.0)
        self.assertEqual(len(srv.seen), 1)

    def test_an_http_date_beyond_the_cap_raises_at_once(self):
        from email.utils import formatdate
        later = formatdate(time.time() + 3600, usegmt=True)
        srv, mem = self.make([(429, {"Retry-After": later}, RATE_429), (200, {}, "{}")])
        with self.assertRaises(RateLimitError) as cm:
            mem.stats("alice")
        self.assertGreater(cm.exception.retry_after, 3500)
        self.assertEqual(len(srv.seen), 1)

    def test_the_final_error_carries_retry_after(self):
        srv, mem = self.make([(429, {"Retry-After": "7"}, RATE_429)], retries=0)
        with self.assertRaises(RateLimitError) as cm:
            mem.stats("alice")
        self.assertEqual(cm.exception.retry_after, 7.0)
        srv, mem = self.make([(429, {}, RATE_429)], retries=0)
        with self.assertRaises(RateLimitError) as cm:
            mem.stats("alice")
        self.assertIsNone(cm.exception.retry_after)

    def test_get_image_raises_at_once_too(self):
        srv, mem = self.make([(429, {"Retry-After": "600"}, RATE_429)])
        with self.assertRaises(RateLimitError) as cm:
            mem.get_image(memory_id=IMAGE_ID)
        self.assertEqual(cm.exception.retry_after, 600.0)
        self.assertEqual(len(srv.seen), 1)

    def test_malformed_values_fall_back_to_backoff(self):
        for v in ("-5", "5, 10", "0x2", "garbage", "", " ", "5.5", "1e3", "inf", "nan", "+5", "٥"):
            with self.subTest(value=v):
                self.assertIsNone(wontopos._parse_retry_after(v))
                self.assertTrue(0.5 <= wontopos._backoff(0, v) <= 0.75, wontopos._backoff(0, v))

    def test_delta_seconds_and_http_dates_are_read(self):
        from email.utils import formatdate
        self.assertEqual(wontopos._parse_retry_after("12"), 12.0)
        self.assertEqual(wontopos._parse_retry_after("0"), 0.0)
        self.assertEqual(wontopos._parse_retry_after(formatdate(time.time() - 100, usegmt=True)), 0.0)
        self.assertTrue(8 <= wontopos._parse_retry_after(formatdate(time.time() + 10, usegmt=True)) <= 10)

    def test_a_very_long_retry_after_is_over_the_cap(self):
        self.assertEqual(wontopos._parse_retry_after("9" * 5000), float(2**31))
        self.assertEqual(wontopos._parse_retry_after("0" * 20 + "7"), 7.0)
        srv, mem = self.make([(429, {"Retry-After": "9" * 5000}, RATE_429), (200, {}, "{}")])
        with self.assertRaises(RateLimitError) as cm:
            mem.add("x", "alice")
        self.assertEqual(cm.exception.retry_after, float(2**31))
        self.assertEqual(len(srv.seen), 1)

    def test_a_negative_retry_after_does_not_retry_at_once(self):
        srv, mem = self.make([(429, {"Retry-After": "-5"}, RATE_429), (200, {}, "{}")])
        t0 = time.monotonic()
        mem.stats("alice")
        self.assertGreaterEqual(time.monotonic() - t0, 0.45)
        self.assertEqual(len(srv.seen), 2)


@needs_httpx
class AsyncRetryAfterTest(AsyncMake, unittest.IsolatedAsyncioTestCase):
    async def test_a_retry_after_beyond_the_cap_raises_at_once(self):
        srv, mem = self.amake([(429, {"Retry-After": "3540"}, RATE_429), (200, {}, "{}")])
        async with mem:
            with self.assertRaises(RateLimitError) as cm:
                await mem.stats("alice")
        self.assertEqual(cm.exception.retry_after, 3540.0)
        self.assertEqual(len(srv.seen), 1)

    async def test_get_image_raises_at_once_too(self):
        srv, mem = self.amake([(429, {"Retry-After": "600"}, RATE_429)])
        async with mem:
            with self.assertRaises(RateLimitError):
                await mem.get_image(memory_id=IMAGE_ID)
        self.assertEqual(len(srv.seen), 1)

    async def test_a_very_long_retry_after_is_over_the_cap(self):
        srv, mem = self.amake([(429, {"Retry-After": "9" * 5000}, RATE_429), (200, {}, "{}")])
        async with mem:
            with self.assertRaises(RateLimitError) as cm:
                await mem.add("x", "alice")
        self.assertEqual(cm.exception.retry_after, float(2**31))
        self.assertEqual(len(srv.seen), 1)


class WallClockTimeoutTest(unittest.IsolatedAsyncioTestCase):

    HEAD = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 40\r\n\r\n"
    IMG_HEAD = b"HTTP/1.1 200 OK\r\nContent-Type: image/webp\r\nContent-Length: 40\r\n\r\n"
    SMALL_HEAD = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n"

    def assertQuick(self, t0, limit):
        spent = time.monotonic() - t0
        self.assertLess(spent, limit + 0.8, f"took {spent:.1f}s against a {limit}s limit")

    def test_sync_body(self):
        seen, base = trickle_server(self, self.HEAD, b" " * 40)
        mem = Client(KEY, base_url=base, timeout=0.5, retries=0)
        t0 = time.monotonic()
        with self.assertRaises(APIConnectionError) as cm:
            mem.stats("alice")
        self.assertQuick(t0, 0.5)
        self.assertIn("request timed out after 0.5s", str(cm.exception))

    def test_sync_headers(self):
        seen, base = trickle_server(self, self.SMALL_HEAD, b"{}", trickle_head=True)
        mem = Client(KEY, base_url=base, timeout=0.5, retries=2)
        t0 = time.monotonic()
        with self.assertRaises(APIConnectionError) as cm:
            mem.list_stores()
        self.assertQuick(t0, 0.5)
        self.assertIn("request timed out after 0.5s", str(cm.exception))
        self.assertEqual(len(seen), 1, "a timeout is final")

    def test_sync_deadline(self):
        seen, base = trickle_server(self, self.HEAD, b" " * 40)
        mem = Client(KEY, base_url=base, timeout=10, deadline=0.6, retries=2)
        t0 = time.monotonic()
        with self.assertRaises(APIConnectionError) as cm:
            mem.list_stores()
        self.assertQuick(t0, 0.6)
        self.assertIn("deadline of 0.6s exhausted", str(cm.exception))

    def test_sync_image(self):
        seen, base = trickle_server(self, self.IMG_HEAD, b"x" * 40)
        mem = Client(KEY, base_url=base, timeout=0.5, retries=0)
        t0 = time.monotonic()
        with self.assertRaises(APIConnectionError):
            mem.get_image(memory_id=IMAGE_ID)
        self.assertQuick(t0, 0.5)

    @needs_httpx
    async def test_async_body(self):
        seen, base = trickle_server(self, self.HEAD, b" " * 40)
        async with AsyncClient(KEY, base_url=base, timeout=0.5, retries=0) as mem:
            t0 = time.monotonic()
            with self.assertRaises(APIConnectionError) as cm:
                await mem.stats("alice")
        self.assertQuick(t0, 0.5)
        self.assertIn("request timed out after 0.5s", str(cm.exception))

    @needs_httpx
    async def test_async_headers(self):
        seen, base = trickle_server(self, self.SMALL_HEAD, b"{}", trickle_head=True)
        async with AsyncClient(KEY, base_url=base, timeout=0.5, retries=2) as mem:
            t0 = time.monotonic()
            with self.assertRaises(APIConnectionError):
                await mem.list_stores()
        self.assertQuick(t0, 0.5)
        self.assertEqual(len(seen), 1)

    @needs_httpx
    async def test_async_deadline(self):
        seen, base = trickle_server(self, self.HEAD, b" " * 40)
        async with AsyncClient(KEY, base_url=base, timeout=10, deadline=0.6) as mem:
            t0 = time.monotonic()
            with self.assertRaises(APIConnectionError) as cm:
                await mem.list_stores()
        self.assertQuick(t0, 0.6)
        self.assertIn("deadline of 0.6s exhausted", str(cm.exception))

    @needs_httpx
    async def test_async_image(self):
        seen, base = trickle_server(self, self.IMG_HEAD, b"x" * 40)
        async with AsyncClient(KEY, base_url=base, timeout=0.5, retries=0) as mem:
            t0 = time.monotonic()
            with self.assertRaises(APIConnectionError):
                await mem.get_image(memory_id=IMAGE_ID)
        self.assertQuick(t0, 0.5)


class ErrorFieldsTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):

    UNSUPPORTED = envelope("api_error", "tablet-1 does not serve images", "req_501",
                           model="tablet-1", endpoint="images")

    def test_type_and_details(self):
        _, mem = self.make([(501, {}, self.UNSUPPORTED)])
        with self.assertRaises(wontopos.ServerError) as cm:
            mem.list_images()
        e = cm.exception
        self.assertEqual(e.type, "api_error")
        self.assertEqual(e.details, {"model": "tablet-1", "endpoint": "images"})
        self.assertEqual(e.request_id, "req_501")

    def test_no_extras_means_none(self):
        _, mem = self.make([(400, {}, envelope("invalid_request_error", "bad", "r1")),
                            (400, {}, '{"error": "plain"}')])
        with self.assertRaises(wontopos.BadRequestError) as cm:
            mem.stats("alice")
        self.assertEqual(cm.exception.type, "invalid_request_error")
        self.assertIsNone(cm.exception.details)
        with self.assertRaises(wontopos.BadRequestError) as cm:
            mem.stats("alice")
        self.assertIsNone(cm.exception.type)
        self.assertIsNone(cm.exception.details)

    async def test_async_type_and_details(self):
        _, mem = self.amake([(501, {}, self.UNSUPPORTED)])
        async with mem:
            with self.assertRaises(wontopos.ServerError) as cm:
                await mem.list_images()
        self.assertEqual(cm.exception.details, {"model": "tablet-1", "endpoint": "images"})
        self.assertEqual(cm.exception.type, "api_error")

    GONE = envelope("gone_error", "Scroll 1 now exists only in memory.", "req_410", code=0)

    def test_410_is_gone_says_what_to_do_and_is_not_retried(self):
        srv, mem = self.make([(410, {}, self.GONE)] * 3)
        with self.assertRaises(wontopos.GoneError) as cm:
            mem.search("q", "alice")
        e = cm.exception
        self.assertIsInstance(e, wontopos.WosError)
        self.assertEqual((e.status, e.type, e.request_id), (410, "gone_error", "req_410"))
        self.assertIn("Scroll 1 now exists only in memory.", e.message)
        self.assertIn("list_models()", e.message)
        self.assertEqual(len(srv.seen), 1, "retrying a retired model can never succeed")

    async def test_async_410_is_gone(self):
        srv, mem = self.amake([(410, {}, self.GONE)] * 3)
        async with mem:
            with self.assertRaises(wontopos.GoneError) as cm:
                await mem.search("q", "alice")
        self.assertIn("list_models()", cm.exception.message)
        self.assertEqual(len(srv.seen), 1)

    EXPIRED = envelope("gone_error", "This sleep ran out: read it again.", "req_exp", code="sleep_expired")
    SELF_CONFLICT = envelope("conflict_error", "the file changed since it was read", "req_409",
                             code="self_version_conflict")

    def test_a_sleep_expired_410_keeps_its_own_message_and_code_and_a_409_its_code(self):
        writes = [{"slot": 1, "base_version": 0, "body": "## A\n- b.\n"}]
        srv, mem = self.make([(410, {}, self.EXPIRED), (409, {}, self.SELF_CONFLICT)])
        with self.assertRaises(wontopos.WosError) as cm:
            mem.write_sleep_files("s-1", writes, "alice")
        e = cm.exception
        self.assertIs(type(e), wontopos.WosError, "an expired sleep plan is not a retired model")
        self.assertEqual((e.status, e.code, e.message), (410, "sleep_expired", "This sleep ran out: read it again."))
        self.assertNotIn("retired", str(e))
        self.assertEqual(pickle.loads(pickle.dumps(e)).code, "sleep_expired")
        self.assertIs(type(pickle.loads(pickle.dumps(e))), wontopos.WosError)
        with self.assertRaises(wontopos.ConflictError) as cm:
            mem.write_sleep_files("s-1", writes, "alice")
        self.assertEqual((cm.exception.status, cm.exception.code), (409, "self_version_conflict"))
        self.assertEqual(len(srv.seen), 2)
        srv, mem = self.make([(410, {}, self.GONE)])
        with self.assertRaises(wontopos.GoneError) as cm:
            mem.sleep_done("s-1", "alice")
        self.assertIn("This model is retired; list_models()", cm.exception.message)
        self.assertIsNone(cm.exception.code)

    async def test_async_a_sleep_expired_410_and_a_409_keep_their_codes(self):
        srv, mem = self.amake([(410, {}, self.EXPIRED), (409, {}, self.SELF_CONFLICT)])
        async with mem:
            with self.assertRaises(wontopos.WosError) as cm:
                await mem.sleep_done("s-1", "alice")
            self.assertIs(type(cm.exception), wontopos.WosError)
            self.assertEqual((cm.exception.code, cm.exception.message),
                             ("sleep_expired", "This sleep ran out: read it again."))
            with self.assertRaises(wontopos.ConflictError) as cm:
                await mem.write_sleep_notes(0, {}, "alice")
            self.assertEqual(cm.exception.code, "self_version_conflict")
        self.assertEqual(len(srv.seen), 2)

    def test_revisions_include_is_checked_before_sending(self):
        srv, mem = self.make([])
        with self.assertRaisesRegex(ValueError, 'include must be "revised" or "unrevised"'):
            mem.revisions("alice", include="both")
        self.assertEqual(srv.seen, [])

    def test_none_options_are_left_out_not_sent_as_null(self):
        srv, mem = self.make([(200, {}, '{"id":"m1","status":"stored"}'),
                              (200, {}, '{"memories":[]}'),
                              (200, {}, '{"status":"stored"}')])
        mem.add("x", "alice", metadata={"speaker": "me"}, speaker=None, category="work")
        mem.search("q", "alice", speaker=None, cache_control=None)
        mem.add_bulk("long text", "alice", category=None)
        add, search, bulk = (json.loads(r["body"]) for r in srv.seen)
        self.assertEqual(add["metadata"], {"speaker": "me", "category": "work"})
        self.assertNotIn("speaker", search)
        self.assertNotIn("cache_control", search)
        self.assertEqual(bulk["category"], "general")

    def test_a_store_keyword_of_none_is_still_refused(self):
        srv, mem = self.make([])
        for key in ("userId", "store_id", "idempotencyKey"):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "is not a metadata field"):
                mem.add("x", "alice", **{key: None})
        self.assertEqual(srv.seen, [])

    def test_an_integer_of_another_type_is_accepted_and_sent_as_an_int(self):
        class OtherInt:
            def __init__(self, v): self.v = v
            def __int__(self): return self.v
            def __index__(self): return self.v
            def __lt__(self, o): return self.v < o
            def __le__(self, o): return self.v <= o
            def __gt__(self, o): return self.v > o
            def __ge__(self, o): return self.v >= o
        numbers.Integral.register(OtherInt)
        srv, mem = self.make([(200, {}, '{"memories":[]}')])
        mem.search("q", "alice", limit=OtherInt(7))
        self.assertEqual(json.loads(srv.seen[0]["body"])["max_results"], 7)
        with self.assertRaises(ValueError):
            mem.search("q", "alice", limit=True)

    def test_list_speakers_always_has_a_list(self):
        srv, mem = self.make([(200, {}, '{"speakers":null}')])
        self.assertEqual(mem.list_speakers("alice")["speakers"], [])

    def test_413_and_422_are_bad_requests(self):
        for status in (413, 422):
            with self.subTest(status=status):
                _, mem = self.make([(status, {}, envelope("invalid_request_error", "no", "r"))])
                with self.assertRaises(wontopos.BadRequestError):
                    mem.add("x", "alice")

    async def test_async_413_and_422_are_bad_requests(self):
        for status in (413, 422):
            _, mem = self.amake([(status, {}, envelope("invalid_request_error", "no", "r"))])
            async with mem:
                with self.assertRaises(wontopos.BadRequestError):
                    await mem.add("x", "alice")

    def test_an_object_message_falls_back_to_the_type(self):
        e = wontopos._parse_error(404, json.dumps({"error": {"type": "not_found_error", "message": {"k": 1}}}))
        self.assertEqual(e.message, "not_found_error")

    def test_get_image_errors_carry_the_request_id(self):
        _, mem = self.make([(404, {}, envelope("not_found_error", "Not found.", "req_img"))])
        with self.assertRaises(NotFoundError) as cm:
            mem.get_image(memory_id=IMAGE_ID)
        self.assertEqual(cm.exception.request_id, "req_img")

    async def test_async_get_image_errors_carry_the_request_id(self):
        _, mem = self.amake([(404, {}, envelope("not_found_error", "Not found.", "req_img"))])
        async with mem:
            with self.assertRaises(NotFoundError) as cm:
                await mem.get_image(memory_id=IMAGE_ID)
        self.assertEqual(cm.exception.request_id, "req_img")

    HOSTILE = json.dumps({"error": {"type": "api_error", "message": "boom\u001b[31mRED\r\nInjected: line\u007f",
                                    "request_id": "req\n1"}})

    def assertClean(self, e):
        for text in (e.message, str(e), e.request_id or ""):
            self.assertIsNone(re.search(r"[\x00-\x1f\x7f]", text), repr(text))
        self.assertIn("boom", e.message)
        self.assertIn("Injected: line", e.message)

    def test_control_characters_never_reach_the_message(self):
        _, mem = self.make([(500, {}, self.HOSTILE), (400, {}, '{"error": "a\\u0000b"}'),
                            (400, {}, "raw\x07text"), (500, {}, self.HOSTILE)], retries=0)
        with self.assertRaises(WosError) as cm:
            mem.add("x", "alice")
        self.assertClean(cm.exception)
        with self.assertRaises(WosError) as cm:
            mem.add("x", "alice")
        self.assertEqual(cm.exception.message, "ab")
        with self.assertRaises(WosError) as cm:
            mem.add("x", "alice")
        self.assertEqual(cm.exception.message, "rawtext")
        with self.assertRaises(WosError) as cm:
            mem.get_image(memory_id=IMAGE_ID)
        self.assertClean(cm.exception)

    async def test_async_control_characters_never_reach_the_message(self):
        _, mem = self.amake([(500, {}, self.HOSTILE)], retries=0)
        async with mem:
            with self.assertRaises(WosError) as cm:
                await mem.add("x", "alice")
        self.assertClean(cm.exception)

    def test_an_empty_error_body_names_the_status(self):
        _, mem = self.make([(502, {}, ""), (500, {}, "  \r\n")])
        for status in (502, 500):
            with self.assertRaises(WosError) as cm:
                mem.create_store("alice")
            self.assertEqual(cm.exception.message, f"HTTP {status}")
            self.assertEqual(str(cm.exception), f"[{status}] HTTP {status}")

    async def test_async_an_empty_error_body_names_the_status(self):
        _, mem = self.amake([(502, {}, "")])
        async with mem:
            with self.assertRaises(wontopos.ServerError) as cm:
                await mem.create_store("alice")
        self.assertEqual(cm.exception.message, "HTTP 502")

    def test_a_blank_error_field_is_skipped_and_a_top_level_message_is_read(self):
        cases = [
            ('{"error":{"type":"invalid_request_error","message":"","request_id":"req_e"}}',
             "[400] invalid_request_error (request_id: req_e)"),
            ('{"error":""}', "[400] HTTP 400"),
            ('{"error":{"type":"t","message":"  ","request_id":"  "}}', "[400] t"),
            ('{"error":"  "}', "[400] HTTP 400"),
            ("  ", "[400] HTTP 400"),
            ('{"message":"m"}', "[400] m"),
            ('{"message":""}', '[400] {"message":""}'),
        ]
        _, mem = self.make([(400, {}, body) for body, _ in cases], retries=0)
        for body, want in cases:
            with self.subTest(body=body):
                with self.assertRaises(WosError) as cm:
                    mem.add("x", "alice")
                self.assertEqual(str(cm.exception), want)

    def test_details_lose_control_characters(self):
        body = envelope("conflict_error", "taken", "r", conflicts_with="team\u001b[2J-a",
                        extra={"k\n": ["a\u0007b", 1, None]})
        _, mem = self.make([(409, {}, body)])
        with self.assertRaises(wontopos.ConflictError) as cm:
            mem.create_store("team_a")
        self.assertEqual(cm.exception.conflicts_with, "team[2J-a")
        self.assertEqual(cm.exception.details["extra"], {"k": ["ab", 1, None]})

    def test_oversized_details_drop_the_biggest_fields(self):
        e = wontopos._parse_error(409, envelope("conflict_error", "taken", "r", a="x" * 4000,
                                                b="y" * 4000, c="z" * 4000, conflicts_with="team-a"))
        self.assertEqual(list(e.details), ["a", "b", "conflicts_with"])
        self.assertEqual(e.conflicts_with, "team-a")
        e = wontopos._parse_error(500, envelope("api_error", "boom", "r", blob="x" * 9000))
        self.assertTrue(e.details["blob"].endswith("…(truncated)"))


class TlsTrustTest(unittest.TestCase):

    def test_separate_contexts_with_the_floor(self):
        import ssl
        sync_ctx, async_ctx = wontopos._tls_context(), wontopos._async_tls_context()
        self.assertIsNot(sync_ctx, async_ctx)
        for ctx in (sync_ctx, async_ctx):
            self.assertGreaterEqual(ctx.minimum_version, ssl.TLSVersion.TLSv1_2)
            self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
            self.assertTrue(ctx.check_hostname)

    def test_certifi_roots_without_an_os_store(self):
        import subprocess
        import tempfile
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        code = ("import wontopos; print(wontopos._tls_context().cert_store_stats()['x509_ca'], "
                "wontopos._async_tls_context().cert_store_stats()['x509_ca'])")
        with tempfile.TemporaryDirectory() as empty:
            env = dict(os.environ, SSL_CERT_FILE=os.path.join(empty, "none.pem"), SSL_CERT_DIR=empty,
                       PYTHONPATH=os.path.join(root, "src"), OBJC_DISABLE_INITIALIZE_FORK_SAFETY="YES",
                       NO_PROXY="*")
            out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True,
                                 text=True, timeout=120)
        self.assertEqual(out.returncode, 0, out.stderr)
        sync_n, async_n = (int(x) for x in out.stdout.split())
        self.assertGreater(sync_n, 0)
        self.assertGreater(async_n, 0)


class BaseUrlWhitespaceTest(unittest.TestCase):
    def test_surrounding_whitespace_is_trimmed_and_inner_is_refused(self):
        for cls in ((Client, AsyncClient) if HAVE_HTTPX else (Client,)):
            self.assertEqual(cls(KEY, base_url=" https://api.example.com/\n")._base, "https://api.example.com")
            with self.assertRaises(ValueError):
                cls(KEY, base_url="https://api.example.com\t/x")


class TimeoutValuesTest(unittest.TestCase):

    CLASSES = (Client, AsyncClient) if HAVE_HTTPX else (Client,)

    def test_values_that_are_not_positive_numbers_mean_the_default(self):
        for cls in self.CLASSES:
            for unset in (0, -1, float("nan"), float("-inf"), True, "60", (3, 30)):
                with self.subTest(cls=cls.__name__, value=unset):
                    self.assertEqual(cls(KEY, timeout=unset)._timeout, 30.0)
                    self.assertIsNone(cls(KEY, deadline=unset)._deadline)
            self.assertEqual(cls(KEY, timeout=float("inf"))._timeout, 2147483)

    def test_clones_fall_back_too(self):
        mem = Client(KEY)
        self.assertEqual(mem.with_timeout(0)._timeout, 30.0)
        self.assertIsNone(mem.with_deadline(float("nan"))._deadline)

    def test_huge_values_are_clamped(self):
        for cls in self.CLASSES:
            mem = cls(KEY, timeout=1e20, deadline=1e20)
            self.assertEqual(mem._timeout, 2147483)
            self.assertEqual(mem._deadline, 2147483)

    def test_a_huge_timeout_still_makes_calls(self):
        srv, base = scripted_server([(200, {}, '{"collections": []}')])
        self.addCleanup(srv.shutdown)
        self.assertEqual(Client(KEY, base_url=base, timeout=1e20).list_stores(), [])

    def test_none_keeps_the_defaults(self):
        mem = Client(KEY, timeout=None, deadline=None)
        self.assertEqual(mem._timeout, 30.0)
        self.assertIsNone(mem._deadline)


class StoreTimeoutTest(unittest.TestCase):

    CLASSES = (Client, AsyncClient) if HAVE_HTTPX else (Client,)
    EXPECTED = [
        ("POST /api/v1/memory/store", 200.0),
        ("POST /api/v1/memory/store-turn", 200.0),
        ("POST /api/v1/memory/bulk-store", 200.0),
        ("POST /api/v1/memory/supersede", 30.0),
        ("POST /api/v1/memory/search", 30.0),
    ]

    def test_defaults(self):
        for cls in self.CLASSES:
            with self.subTest(cls=cls.__name__):
                mem = cls(KEY)
                self.assertEqual((mem._timeout, mem._store_timeout), (30.0, 200.0))

    def test_a_timeout_the_caller_sets_applies_to_every_call_and_clones_keep_it(self):
        for cls in self.CLASSES:
            with self.subTest(cls=cls.__name__):
                mem = cls(KEY, timeout=10)
                self.assertEqual((mem._timeout, mem._store_timeout), (10, 10))
                self.assertEqual(mem.with_model("tablet-1")._store_timeout, 10)
                self.assertEqual(cls(KEY).with_timeout(5)._store_timeout, 5)
                self.assertEqual(cls(KEY).with_user("bob")._store_timeout, 200.0)
                self.assertEqual(cls(KEY, timeout=0)._store_timeout, 200.0)

    def spy(self):
        from unittest import mock
        waits = []
        clock = wontopos._Call.clock

        def record(call, timeout):
            waits.append((call.where, timeout))
            return clock(call, timeout)

        return waits, mock.patch.object(wontopos._Call, "clock", record)

    def script(self):
        srv, base = scripted_server([(200, {}, "{}")] * 4 + [(200, {}, '{"results": []}')])
        self.addCleanup(srv.shutdown)
        return base

    def test_each_call_waits_its_own_default(self):
        mem = Client(KEY, base_url=self.script())
        self.addCleanup(mem.close)
        waits, patch = self.spy()
        with patch:
            mem.add("x", "alice")
            mem.add_turn("hi", "yo", "alice")
            mem.add_bulk("blob", "alice", "general", None)
            mem.update("m1", "new", "alice")
            mem.search("q", "alice")
        self.assertEqual(waits, self.EXPECTED)

    @needs_httpx
    def test_each_async_call_waits_its_own_default(self):
        import asyncio
        base = self.script()
        waits, patch = self.spy()

        async def main():
            async with AsyncClient(KEY, base_url=base) as mem:
                await mem.add("x", "alice")
                await mem.add_turn("hi", "yo", "alice")
                await mem.add_bulk("blob", "alice", "general", None)
                await mem.update("m1", "new", "alice")
                await mem.search("q", "alice")

        with patch:
            asyncio.run(main())
        self.assertEqual(waits, self.EXPECTED)


@needs_httpx
class AsyncLifecycleTest(unittest.TestCase):

    def test_a_second_event_loop_is_refused_before_sending(self):
        import asyncio
        srv, base = scripted_server([(200, {}, '{"total_memories": 0}')] * 3)
        self.addCleanup(srv.shutdown)
        mem = AsyncClient(KEY, base_url=base)
        asyncio.run(mem.stats("alice"))
        with self.assertRaises(APIConnectionError) as cm:
            asyncio.run(mem.stats("alice"))
        self.assertIn("asyncio.run", str(cm.exception))
        self.assertEqual(len(srv.seen), 1)

    def test_after_aclose_every_call_is_refused_before_sending(self):
        import asyncio
        srv, base = scripted_server([(200, {}, "{}")] * 3)
        self.addCleanup(srv.shutdown)

        async def main():
            mem = AsyncClient(KEY, base_url=base)
            clone = mem.with_user("bob")
            await mem.aclose()
            with self.assertRaises(APIConnectionError) as cm:
                await mem.stats("alice")
            self.assertIn("aclose", str(cm.exception))
            with self.assertRaises(APIConnectionError):
                await clone.stats()

        asyncio.run(main())
        self.assertEqual(srv.seen, [])

    def test_a_non_finite_number_is_the_same_error_in_both_clients(self):
        import asyncio
        srv, base = scripted_server([(200, {}, "{}")] * 4)
        self.addCleanup(srv.shutdown)
        with self.assertRaises(ValueError):
            Client(KEY, base_url=base).add("x", "alice", category=float("nan"))

        async def main():
            async with AsyncClient(KEY, base_url=base) as mem:
                with self.assertRaises(ValueError):
                    await mem.add("x", "alice", category=float("nan"))
                with self.assertRaises(TypeError):
                    await mem.add("x", "alice", category=object())

        asyncio.run(main())
        self.assertEqual(srv.seen, [])


class TransportErrorHygieneTest(unittest.IsolatedAsyncioTestCase):

    def assertHygienic(self, e, where):
        text = str(e)
        self.assertIn(where, text)
        for leak in ("127.0.0.1", "customer-4471", "?", "HTTPConnectionPool"):
            self.assertNotIn(leak, text)
        self.assertIsNone(e.__cause__)
        self.assertIsNone(e.__context__)

    def test_sync(self):
        mem = Client(KEY, base_url=closed_port_url(), retries=0)
        with self.assertRaises(APIConnectionError) as cm:
            mem.list_speakers("customer-4471-jane.doe")
        self.assertHygienic(cm.exception, "GET /api/v1/memory/speakers")
        with self.assertRaises(APIConnectionError) as cm:
            mem.usage(7)
        self.assertHygienic(cm.exception, "GET /api/v1/won/usage")
        self.assertIn("refused", str(cm.exception))

    @needs_httpx
    async def test_async(self):
        async with AsyncClient(KEY, base_url=closed_port_url(), retries=0) as mem:
            with self.assertRaises(APIConnectionError) as cm:
                await mem.list_speakers("customer-4471-jane.doe")
            self.assertHygienic(cm.exception, "GET /api/v1/memory/speakers")
            with self.assertRaises(APIConnectionError) as cm:
                await mem.get_image(memory_id=IMAGE_ID)
            self.assertHygienic(cm.exception, "POST /api/v1/memory/image")

    @needs_httpx
    def test_an_async_client_refuses_to_be_pickled(self):
        import pickle
        mem = AsyncClient(KEY)
        for obj in (mem, mem.with_user("bob")):
            with self.assertRaises(TypeError) as cm:
                pickle.dumps(obj)
            self.assertIn("per process", str(cm.exception))


class ClientPickleTest(unittest.TestCase):

    def test_a_client_round_trips_into_a_working_one(self):
        import pickle
        srv, base = scripted_server([(200, {}, '{"total_memories": 3}')] * 2)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        mem = Client(KEY, base_url=base, user_id="alice", timeout=7, retries=1, deadline=9.0,
                     model="tablet-1")
        self.addCleanup(mem.close)
        data = pickle.dumps(mem)
        self.assertIn(KEY.encode(), data)
        back = pickle.loads(data)
        self.addCleanup(back.close)
        self.assertIs(type(back), Client)
        self.assertEqual((back._user_id, back._timeout, back._retries, back._deadline, back._model),
                         ("alice", 7, 1, 9.0, "tablet-1"))
        self.assertIsNot(back._session, mem._session)
        self.assertIn(back._session, list(wontopos._sessions))
        self.assertIs(back._session.get_adapter(base).poolmanager.pool_classes_by_scheme,
                      wontopos._WATCHED_POOLS)
        self.assertEqual(back.stats()["total_memories"], 3)
        self.assertEqual(srv.seen[0]["headers"]["X-API-Key"], KEY)
        self.assertEqual(srv.seen[0]["headers"]["X-WOS-Model"], "tablet-1")
        self.assertNotIn(KEY, repr(back))
        clone = pickle.loads(pickle.dumps(mem.with_user("bob")))
        self.assertEqual(clone.stats()["total_memories"], 3)


class CopyTest(unittest.IsolatedAsyncioTestCase):

    def test_sync_copies_work_and_share_the_session(self):
        import copy
        import pickle
        from unittest import mock
        srv, base = scripted_server([(200, {}, '{"models": []}')] * 3)
        self.addCleanup(srv.shutdown)
        mem = Client(KEY, base_url=base, user_id="alice", deadline=9.0)
        self.addCleanup(mem.close)
        copies = (copy.copy(mem), copy.deepcopy(mem), copy.deepcopy({"tools": [mem]})["tools"][0])
        with mock.patch.object(mem._session, "close") as closed:
            for dup in copies:
                self.assertIs(type(dup), Client)
                self.assertIs(dup._session, mem._session)
                self.assertEqual((dup._user_id, dup._deadline), ("alice", 9.0))
                self.assertNotIn(KEY, repr(dup))
                dup.close()
                self.assertEqual(dup.list_models(), [])
            closed.assert_not_called()
            mem.close()
            closed.assert_called_once()
        self.assertEqual(len(srv.seen), 3)
        for obj in (mem, *copies):
            back = pickle.loads(pickle.dumps(obj))
            self.assertIs(type(back), Client)
            self.assertEqual(back._user_id, "alice")

    def test_a_subclass_stays_itself_and_deepcopy_is_deep(self):
        import copy

        class Tagged(Client):
            pass

        mem = Tagged(KEY)
        self.addCleanup(mem.close)
        mem.tags = ["a"]
        shallow, deep = copy.copy(mem), copy.deepcopy(mem)
        self.assertIs(type(shallow), Tagged)
        self.assertIs(type(deep), Tagged)
        self.assertIs(shallow.tags, mem.tags)
        self.assertEqual(deep.tags, ["a"])
        self.assertIsNot(deep.tags, mem.tags)
        self.assertIs(deep._session, mem._session)

    @unittest.skipUnless(HAVE_HTTPX, "httpx not installed")
    async def test_async_copies_work_and_share_the_transport(self):
        import copy
        import pickle
        srv, base = scripted_server([(200, {}, '{"models": []}')] * 3)
        self.addCleanup(srv.shutdown)
        mem = AsyncClient(KEY, base_url=base, user_id="alice")
        copies = (copy.copy(mem), copy.deepcopy(mem))
        for dup in copies:
            self.assertIs(type(dup), AsyncClient)
            self.assertIs(dup._transport, mem._transport)
            await dup.aclose()
            self.assertEqual(await dup.list_models(), [])
        self.assertEqual(await mem.list_models(), [])
        for obj in (mem, *copies):
            with self.assertRaises(TypeError):
                pickle.dumps(obj)
        await mem.aclose()
        with self.assertRaises(APIConnectionError):
            await copies[0].list_models()


class BaseUrlTest(unittest.TestCase):

    CLASSES = (Client, AsyncClient) if HAVE_HTTPX else (Client,)

    def test_inner_whitespace_and_backslashes_are_refused(self):
        for cls in self.CLASSES:
            for bad in ("https://api.won topos.com", "http://203.0.113.7:8080\\@localhost",
                        "https://api.wontopos.com/a\tb"):
                with self.subTest(cls=cls.__name__, url=bad):
                    with self.assertRaises(ValueError):
                        cls(KEY, base_url=bad)
            for padded in (" https://api.wontopos.com", "https://api.wontopos.com\n",
                           "https://api.wontopos.com\t"):
                self.assertEqual(cls(KEY, base_url=padded)._base, "https://api.wontopos.com")

    def test_a_url_that_does_not_parse_is_refused(self):
        for cls in self.CLASSES:
            for bad in ("http://[bad", "http://", "https://:443", "api.wontopos.com",
                        "ftp://files.example.com", "http://host:99999", "mailto:a@b",
                        "http://user:s3cret@[bad", "http:/example.net:9", "http:example.net",
                        "HTTPS:api.wontopos.com", "https:///api.wontopos.com", "http:127.0.0.1:9"):
                with self.subTest(cls=cls.__name__, url=bad):
                    t0 = time.monotonic()
                    with self.assertRaises(ValueError) as cm:
                        cls(KEY, base_url=bad)
                    self.assertLess(time.monotonic() - t0, 0.5)
                    self.assertIn("base_url is not a URL", str(cm.exception))
                    self.assertNotIn("s3cret", str(cm.exception))

    def test_no_message_or_repr_shows_the_url_password(self):
        for cls in self.CLASSES:
            for bad in ("https://u:hunter2@proxy.example.com/a\nb", "u:hunter2@proxy.example.com",
                        "http://u:hunter2@proxy.example.com/x y", "https://u:hunt@er2@proxy.example.com/\tx"):
                with self.subTest(cls=cls.__name__, url=bad):
                    with self.assertRaises(ValueError) as cm:
                        cls(KEY, base_url=bad)
                    text = str(cm.exception)
                    self.assertNotIn("er2", text)
                    self.assertNotIn("—", text)
            mem = cls(KEY, base_url="https://u:hunter2@proxy.example.com")
            self.assertNotIn("hunter2", repr(mem))
            self.assertIn("https://***@proxy.example.com", repr(mem))
        self.assertEqual(wontopos._mask_userinfo("https://api.wontopos.com/v1"), "https://api.wontopos.com/v1")
        self.assertEqual(wontopos._mask_userinfo("https://host/path@x"), "https://host/path@x")

    def test_the_warning_follows_the_transport_host(self):
        import warnings as _w
        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter("always")
            wontopos._warn_if_plain_http("http://203.0.113.7:8080\\@localhost")
        self.assertTrue(any("unencrypted" in str(c.message) for c in caught))

    def test_loopback_stays_quiet(self):
        import warnings as _w
        for url in ("http://127.0.0.1:8080", "http://localhost:1", "http://[::1]:8080"):
            with _w.catch_warnings(record=True) as caught:
                _w.simplefilter("always")
                Client(KEY, base_url=url)
                if HAVE_HTTPX:
                    AsyncClient(KEY, base_url=url)
            self.assertEqual([str(c.message) for c in caught if "unencrypted" in str(c.message)], [], url)

    def test_every_loopback_or_unspecified_address_is_local_as_rust_and_typescript_read_it(self):
        import warnings as _w
        quiet = ("http://127.0.0.2:8080", "http://127.255.255.254", "http://127.1:9", "http://[::]:8080",
                 "http://[0:0:0:0:0:0:0:1]:9", "http://0.0.0.0:9", "http://LOCALHOST")
        loud = ("http://[::ffff:127.0.0.1]:9", "http://128.0.0.1", "http://127.0.0.1.example.com",
                "http://203.0.113.7", "http://[::2]", "http://localhost.example.com")
        for url in quiet + loud:
            for cls in self.CLASSES:
                with self.subTest(cls=cls.__name__, url=url), _w.catch_warnings(record=True) as caught:
                    _w.simplefilter("always")
                    cls(KEY, base_url=url)
                    said = [str(c.message) for c in caught if "unencrypted" in str(c.message)]
                    self.assertEqual(len(said), 0 if url in quiet else 1)


class MetadataKeysTest(SyncMake, unittest.TestCase):

    def setUp(self):
        wontopos._reset_warning_state()
        self.addCleanup(wontopos._reset_warning_state)

    def test_an_unknown_key_warns_once(self):
        srv, mem = self.make([(200, {}, "{}")] * 3)
        with self.assertLogs("wontopos", level="WARNING") as cm:
            mem.add("x", "alice", mood="calm")
        self.assertIn("mood", " ".join(cm.output))
        self.assertNotIn("—", " ".join(cm.output))
        with no_wontopos_warning(self):
            mem.add("x", "alice", mood="calm")
            mem.add("x", "alice", speaker="me", event_date="2026-03-14", category="work",
                    conversation_id="c1")


class DocsTest(unittest.TestCase):

    STALE = (
        r'add\(""\s*,\s*image', r"content may be empty", r"IS the memory", r"Scroll 1\.2\+",
        r"Tablet 2 and newer", r"Tablet 2\+", r"store and search with the same",
        r"max_bytes", r"\bused to\b", r"\b2\.2\.\d+", r"[Mm]easured",
    )

    def test_no_stale_claims(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        src = open(os.path.join(root, "src", "wontopos", "__init__.py"), encoding="utf-8").read()
        src = re.sub(r'(?m)^__version__ = .*$', "", src)
        found = [p for p in self.STALE if re.search(p, src)]
        self.assertEqual(found, [])

    def test_list_models_documents_capabilities(self):
        for fn in (Client.list_models, AsyncClient.list_models):
            self.assertIn("capabilities", fn.__doc__)

    def test_delete_all_says_it_clears_the_time_zone_and_speakers(self):
        for fn in (Client.delete_all, AsyncClient.delete_all):
            doc = " ".join(fn.__doc__.split())
            self.assertIn("clears the store's time zone and speakers", doc)
            self.assertIn("set_settings", doc)
            self.assertIn("add_speaker", doc)


class PageSizeTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):

    def test_image_speaker_and_revision_pages_are_5_to_20(self):
        srv, mem = self.make([(200, {}, "{}")] * 8)
        for bad in (0, 4, 21, 100, True, 5.0, "10", float("nan")):
            for call in (lambda v: mem.list_images(limit=v), lambda v: mem.by_speaker("me", limit=v),
                         lambda v: mem.revisions(include="revised", limit=v),
                         lambda v: mem.export_images(page_size=v),
                         lambda v: list(mem.iter_images(page_size=v))):
                with self.subTest(value=bad):
                    with self.assertRaises(ValueError) as cm:
                        call(bad)
                    self.assertIn("5 and 20", str(cm.exception))
        self.assertEqual(srv.seen, [])
        mem.list_images(limit=5)
        mem.by_speaker("me", limit=20)
        self.assertEqual(len(srv.seen), 2)

    def test_max_images_is_0_to_5(self):
        srv, mem = self.make([(200, {}, '{"memories": []}')] * 6)
        for bad in (-1, 6, True, 2.0):
            with self.assertRaises(ValueError) as cm:
                mem.search("q", "alice", max_images=bad)
            self.assertIn("0 and 5", str(cm.exception))
            with self.assertRaises(ValueError):
                mem.search("q", "alice", extra={"max_images": bad})
        self.assertEqual(srv.seen, [])
        mem.search("q", "alice", max_images=0)
        mem.search("q", "alice", max_images=5)
        self.assertEqual(len(srv.seen), 2)

    def test_list_memories_is_1_to_500(self):
        srv, mem = self.make([(200, {}, '{"memories": [], "next_cursor": null}')] * 4)
        for bad in (0, -1, 501, 10 ** 9, True, 2.5, float("nan")):
            with self.assertRaises(ValueError) as cm:
                mem.list_memories("alice", limit=bad)
            self.assertIn("1 and 500", str(cm.exception))
        with self.assertRaises(ValueError):
            list(mem.iter_memories("alice", page_size=0))
        self.assertEqual(srv.seen, [])
        mem.list_memories("alice", limit=1)
        mem.list_memories("alice", limit=500)
        self.assertEqual(len(srv.seen), 2)

    EMPTY_PAGE = (200, {}, '{"memories": [], "next_cursor": null}')

    def test_none_means_the_default_page_size(self):
        srv, mem = self.make([self.EMPTY_PAGE] * 3)
        mem.list_memories("alice", limit=None)
        list(mem.iter_memories("alice", page_size=None))
        mem.export_memories("alice")
        self.assertEqual([json.loads(r["body"])["limit"] for r in srv.seen], [100, 100, 100])

    def test_a_none_search_limit_means_the_default(self):
        srv, mem = self.make([(200, {}, '{"memories": []}')] * 3)
        mem.search("q", "alice", None)
        mem.search_self("q", "alice", None)
        mem.search_full("q", "alice", None)
        self.assertEqual([json.loads(r["body"])["max_results"] for r in srv.seen], [10, 10, 10])

    async def test_async_none_means_the_default_page_size(self):
        srv, mem = self.amake([self.EMPTY_PAGE] * 2)
        async with mem:
            await mem.list_memories("alice", limit=None)
            self.assertEqual([m async for m in mem.iter_memories("alice", page_size=None)], [])
        self.assertEqual([json.loads(r["body"])["limit"] for r in srv.seen], [100, 100])

    async def test_async_checks_the_same(self):
        srv, mem = self.amake([(200, {}, "{}")] * 2)
        async with mem:
            with self.assertRaises(ValueError):
                await mem.list_images(limit=100)
            with self.assertRaises(ValueError):
                await mem.export_images(page_size=2)
            with self.assertRaises(ValueError):
                await mem.list_memories(limit=0)
            with self.assertRaises(ValueError):
                await mem.search("q", "alice", max_images=6)
        self.assertEqual(srv.seen, [])


class ListRecordsTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    MODELS = '{"models": [{"id": "tablet-2"}, null, 3, "x"]}'
    STORES = '{"collections": [{"user_id": "a"}, {"user_id": "b"}, null]}'

    def test_non_objects_are_dropped(self):
        _, mem = self.make([(200, {}, self.MODELS), (200, {}, self.STORES)])
        self.assertEqual(mem.list_models(), [{"id": "tablet-2"}])
        self.assertEqual(mem.list_stores(), [{"user_id": "a"}, {"user_id": "b"}])

    async def test_async_non_objects_are_dropped(self):
        _, mem = self.amake([(200, {}, self.MODELS), (200, {}, self.STORES)])
        async with mem:
            self.assertEqual(await mem.list_models(), [{"id": "tablet-2"}])
            self.assertEqual(await mem.list_stores(), [{"user_id": "a"}, {"user_id": "b"}])


ADMIN_KEY = "wos-admin-" + "k" * 147
ADMIN_ONLY = "Only an admin key lists stores; this is a standard API key."
ADMIN_SAID = ("This is an admin key: it lists stores and reads usage and revision counts, and opens no "
              "memory. Use a standard API key for this call.")


class AdminKeyTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    PING = '{"ok": true, "scope": "admin"}'
    LISTS = [
        (200, {}, '{"collections": [], "count": 0, "admin_only": true}'),
        (200, {}, '{"collections": [{"user_id": "a"}], "admin_only": false}'),
        (200, {}, '{"collections": [{"user_id": "a"}, null]}'),
    ]

    def test_ping_and_key_scope_ask_the_ping_endpoint(self):
        srv, mem = self.make([(200, {}, self.PING), (200, {}, self.PING), (200, {}, '{"ok": true}')])
        self.assertIs(mem.ping(), True)
        self.assertEqual(mem.key_scope(), "admin")
        with self.assertRaises(WosError) as cm:
            mem.key_scope()
        self.assertEqual((cm.exception.status, cm.exception.message), (200, "the answer carried no key scope"))
        self.assertEqual([(s["method"], s["path"]) for s in srv.seen], [("GET", "/api/v1/memory/ping")] * 3)

    async def test_async_ping_and_key_scope(self):
        srv, mem = self.amake([(200, {}, self.PING), (200, {}, '{"ok": true, "scope": "read"}')])
        async with mem:
            self.assertIs(await mem.ping(), True)
            self.assertEqual(await mem.key_scope(), "read")
        self.assertEqual([s["path"] for s in srv.seen], ["/api/v1/memory/ping"] * 2)

    def test_a_standard_key_cannot_list_stores_where_only_an_admin_key_does(self):
        srv, mem = self.make(self.LISTS)
        with self.assertRaises(wontopos.PermissionDeniedError) as cm:
            mem.list_stores()
        e = cm.exception
        self.assertEqual((e.status, e.message, str(e)), (403, ADMIN_ONLY, f"[403] {ADMIN_ONLY}"))
        self.assertEqual(mem.list_stores(), [{"user_id": "a"}])
        self.assertEqual(mem.list_stores(), [{"user_id": "a"}])
        self.assertEqual(len(srv.seen), 3, "the refusal is not retried")

    async def test_async_a_standard_key_cannot_list_stores_where_only_an_admin_key_does(self):
        _, mem = self.amake(self.LISTS)
        async with mem:
            with self.assertRaises(wontopos.PermissionDeniedError) as cm:
                await mem.list_stores()
            self.assertEqual(str(cm.exception), f"[403] {ADMIN_ONLY}")
            self.assertEqual(await mem.list_stores(), [{"user_id": "a"}])

    def test_usage_carries_stores_admin_only_and_raises_nothing(self):
        _, mem = self.make([(200, {}, '{"stores": [], "stores_admin_only": true, "balance_cents": 5}')])
        self.assertEqual(mem.usage(), {"stores": [], "stores_admin_only": True, "balance_cents": 5})

    def test_an_admin_key_is_sent_as_given_and_its_403_reads_as_the_service_said(self):
        self.assertEqual(len(ADMIN_KEY), 157)
        srv, base = scripted_server([(403, {}, envelope("permission_error", ADMIN_SAID, "req_adm"))])
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        with self.assertRaises(wontopos.PermissionDeniedError) as cm:
            Client(ADMIN_KEY, base_url=base).search("q", "alice")
        self.assertEqual((cm.exception.status, cm.exception.message), (403, ADMIN_SAID))
        self.assertEqual(srv.seen[0]["headers"]["X-API-Key"], ADMIN_KEY)


class CursorRepeatTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):

    LOOP = [(200, {}, '{"memories": [{"id": "1"}], "next_cursor": "A"}'),
            (200, {}, '{"memories": [{"id": "2"}], "next_cursor": "A"}')]
    END = [(200, {}, '{"memories": [{"id": "1"}], "next_cursor": "A"}'),
           (200, {}, '{"memories": [], "next_cursor": "A"}')]
    IMG_LOOP = [(200, {}, '{"images": [{"id": "1"}], "has_more": true, "next_before": "T", "next_skip_ids": ["1"]}'),
                (200, {}, '{"images": [{"id": "2"}], "has_more": true, "next_before": "T", "next_skip_ids": ["1"]}')]

    def test_memories(self):
        _, mem = self.make(self.LOOP)
        with self.assertRaises(RuntimeError) as cm:
            mem.export_memories("alice")
        self.assertIn("truncated", str(cm.exception))
        _, mem = self.make(self.END)
        self.assertEqual([m["id"] for m in mem.export_memories("alice")], ["1"])

    def test_images(self):
        _, mem = self.make(self.IMG_LOOP)
        with self.assertRaises(RuntimeError) as cm:
            mem.export_images("alice")
        self.assertIn("truncated", str(cm.exception))

    async def test_async(self):
        _, mem = self.amake(self.LOOP)
        async with mem:
            with self.assertRaises(RuntimeError):
                await mem.export_memories("alice")
        _, mem = self.amake(self.IMG_LOOP)
        async with mem:
            with self.assertRaises(RuntimeError):
                await mem.export_images("alice")
        _, mem = self.amake(self.END)
        async with mem:
            self.assertEqual([m["id"] for m in await mem.export_memories("alice")], ["1"])


class ErrorPickleTest(unittest.TestCase):

    def test_every_class_round_trips(self):
        import pickle
        classes = (WosError, APIConnectionError, wontopos.BadRequestError, wontopos.AuthenticationError,
                   wontopos.PaymentRequiredError, wontopos.PermissionDeniedError, NotFoundError,
                   wontopos.ConflictError, RateLimitError, wontopos.ServerError)
        for cls in classes:
            with self.subTest(cls=cls.__name__):
                e = cls(404, "gone", request_id="req_1", type="not_found_error", details={"field": "x"})
                back = pickle.loads(pickle.dumps(e))
                self.assertIs(type(back), cls)
                self.assertEqual((back.status, back.message, back.request_id, back.type, back.details),
                                 (404, "gone", "req_1", "not_found_error", {"field": "x"}))
                self.assertEqual(str(back), str(e))

    def test_class_specific_fields_survive(self):
        import pickle
        rl = RateLimitError(429, "slow", request_id="r")
        rl.retry_after = 12.0
        self.assertEqual(pickle.loads(pickle.dumps(rl)).retry_after, 12.0)
        c = wontopos.ConflictError(409, "taken", details={"conflicts_with": "team-a"})
        self.assertEqual(pickle.loads(pickle.dumps(c)).conflicts_with, "team-a")


@unittest.skipUnless(HAVE_HTTPX, "httpx not installed")
class AsyncDecompressionTest(AsyncMake, unittest.IsolatedAsyncioTestCase):

    async def test_stacked_gzip_is_refused_without_inflating_it(self):
        import gzip
        bomb = gzip.compress(zeros_gzip(256 << 20))
        _, mem = self.amake([(200, {"Content-Encoding": "gzip, gzip"}, bomb)], retries=0)
        before = peak_rss_mb()
        async with mem:
            with self.assertRaises(WosError) as cm:
                await mem.stats("alice")
        grew = peak_rss_mb() - before
        self.assertLess(grew, 64, f"peak RSS grew {grew:.0f}MB")
        self.assertIn("encoding", str(cm.exception))

    async def test_one_gzip_layer_stops_at_the_cap(self):
        old = wontopos._MAX_RESPONSE_BYTES
        wontopos._MAX_RESPONSE_BYTES = 1 << 20
        try:
            body = zeros_gzip(128 << 20)
            _, mem = self.amake([(200, {"Content-Encoding": "gzip"}, body)], retries=0)
            before = peak_rss_mb()
            async with mem:
                with self.assertRaises(WosError) as cm:
                    await mem.stats("alice")
            grew = peak_rss_mb() - before
        finally:
            wontopos._MAX_RESPONSE_BYTES = old
        self.assertIn("too large", str(cm.exception))
        self.assertLess(grew, 32, f"peak RSS grew {grew:.0f}MB")

    async def test_gzip_and_deflate_bodies_decode(self):
        import gzip
        import zlib
        raw = zlib.compressobj(6, zlib.DEFLATED, -15)
        raw_deflate = raw.compress(b'{"total_memories": 3}') + raw.flush()
        srv, mem = self.amake([
            (200, {"Content-Encoding": "gzip"}, gzip.compress(b'{"total_memories": 1}')),
            (200, {"Content-Encoding": "deflate"}, zlib.compress(b'{"total_memories": 2}')),
            (200, {"Content-Encoding": "deflate"}, raw_deflate),
            (200, {"Content-Encoding": "identity"}, '{"total_memories": 4}'),
        ])
        async with mem:
            got = [(await mem.stats("alice"))["total_memories"] for _ in range(4)]
        self.assertEqual(got, [1, 2, 3, 4])
        self.assertEqual(srv.seen[0]["headers"].get("Accept-Encoding"), "gzip")

    async def test_an_unknown_encoding_is_refused(self):
        _, mem = self.amake([(200, {"Content-Encoding": "br"}, "xxxx")], retries=0)
        async with mem:
            with self.assertRaises(WosError) as cm:
                await mem.stats("alice")
        self.assertIn("encoding", str(cm.exception))


class DeepJsonTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    DEEP = "[" * 200000 + "]" * 200000

    def test_sync(self):
        _, mem = self.make([(200, {}, self.DEEP)])
        with self.assertRaises(WosError):
            mem.stats("alice")

    async def test_async(self):
        _, mem = self.amake([(200, {}, self.DEEP)])
        async with mem:
            with self.assertRaises(WosError):
                await mem.stats("alice")


class EmptyMemoryIdTest(unittest.IsolatedAsyncioTestCase):

    TENANT = "9b2d8c1e-0000-4000-8000-000000000000"

    def test_sync(self):
        srv, base = scripted_server([(200, {}, "{}")] * 2)
        self.addCleanup(srv.shutdown)
        mem = Client(KEY, base_url=base, user_id="shared-default")
        for call in (mem.delete, mem.get, mem.get_image, mem.forget_image, mem.lineage):
            with self.subTest(call=call.__name__):
                with self.assertRaises(ValueError):
                    call(self.TENANT, "")
        self.assertEqual(srv.seen, [])
        mem.delete(self.TENANT)
        self.assertEqual(json.loads(srv.seen[0]["body"])["memory_id"], self.TENANT)

    @needs_httpx
    async def test_async(self):
        srv, base = scripted_server([(200, {}, "{}")])
        self.addCleanup(srv.shutdown)
        async with AsyncClient(KEY, base_url=base, user_id="shared-default") as mem:
            for call in (mem.delete, mem.get, mem.get_image, mem.forget_image, mem.lineage):
                with self.assertRaises(ValueError):
                    await call(self.TENANT, "")
        self.assertEqual(srv.seen, [])


class PackagingTest(unittest.TestCase):
    def test_async_names_the_supported_httpx_range(self):
        import types
        from unittest import mock
        with mock.patch.dict(sys.modules, {"httpx": types.ModuleType("httpx")}):
            with self.assertRaises(ImportError) as cm:
                AsyncClient(KEY)
        self.assertIn("httpx>=0.27,<1", str(cm.exception))

    def test_dependency_floors(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        toml = open(os.path.join(root, "pyproject.toml"), encoding="utf-8").read()
        self.assertIn('"requests>=2.32.4"', toml)
        self.assertIn('"httpx>=0.27,<1"', toml)
        self.assertIn('license = "MIT"', toml)
        self.assertNotIn("License ::", toml)


class DeleteRetryTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):

    NOT_FOUND = envelope("not_found_error", "Store not found.", "req_404")
    SUFFIX = "(an earlier attempt may already have deleted it)"

    def test_after_a_502(self):
        srv, mem = self.make([(502, {"Retry-After": "0"}, "{}"), (404, {}, self.NOT_FOUND)])
        with self.assertRaises(NotFoundError) as cm:
            mem.delete_store("tenant_a")
        self.assertTrue(cm.exception.message.endswith(self.SUFFIX), cm.exception.message)
        self.assertEqual(cm.exception.request_id, "req_404")
        self.assertEqual(len(srv.seen), 2)

    def test_after_a_dropped_connection(self):
        _, base = dropping_server([None, (404, "application/json", self.NOT_FOUND)])
        mem = Client(KEY, base_url=base, retries=1)
        with self.assertRaises(NotFoundError) as cm:
            mem.remove_speaker("Bob", "alice")
        self.assertTrue(cm.exception.message.endswith(self.SUFFIX))

    def test_a_plain_404_or_one_after_a_429_says_nothing_more(self):
        for script in ([(404, {}, self.NOT_FOUND)],
                       [(429, {"Retry-After": "0"}, RATE_429), (404, {}, self.NOT_FOUND)]):
            _, mem = self.make(script)
            with self.assertRaises(NotFoundError) as cm:
                mem.delete_store("tenant_a")
            self.assertEqual(cm.exception.message, "Store not found.")

    async def test_async_after_a_503(self):
        srv, mem = self.amake([(503, {"Retry-After": "0"}, "{}"), (404, {}, self.NOT_FOUND)])
        async with mem:
            with self.assertRaises(NotFoundError) as cm:
                await mem.forget_image("alice", IMAGE_ID)
        self.assertTrue(cm.exception.message.endswith(self.SUFFIX))

    def test_a_preview_deletes_nothing_so_says_nothing_more(self):
        srv, mem = self.make([(503, {"Retry-After": "0"}, "{}"), (404, {}, self.NOT_FOUND)])
        with self.assertRaises(NotFoundError) as cm:
            mem.forget_image("alice", IMAGE_ID, preview=True)
        self.assertEqual(cm.exception.message, "Store not found.")
        self.assertEqual(len(srv.seen), 2)

    async def test_async_a_preview_says_nothing_more(self):
        srv, mem = self.amake([(503, {"Retry-After": "0"}, "{}"), (404, {}, self.NOT_FOUND)])
        async with mem:
            with self.assertRaises(NotFoundError) as cm:
                await mem.forget_image("alice", IMAGE_ID, preview=True)
        self.assertEqual(cm.exception.message, "Store not found.")


def silent_server(case):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    held = []

    def run():
        while True:
            try:
                sock, _ = srv.accept()
            except OSError:
                return
            held.append(sock)

    threading.Thread(target=run, daemon=True).start()
    case.addCleanup(lambda: [s.close() for s in held])
    case.addCleanup(srv.close)
    return held, f"https://127.0.0.1:{srv.getsockname()[1]}"


@needs_httpx
class AsyncConnectHangTest(unittest.IsolatedAsyncioTestCase):

    async def test_a_hung_handshake_is_retried_even_for_a_write(self):
        conns, base = silent_server(self)
        async with AsyncClient(KEY, base_url=base, timeout=0.5, retries=1) as mem:
            with self.assertRaises(APIConnectionError) as cm:
                await mem.add("x", "alice")
        self.assertEqual(len(conns), 2)
        self.assertIn("network error: POST /api/v1/memory/store", str(cm.exception))

    async def test_a_hang_cut_by_the_deadline_reports_the_deadline(self):
        for retries in (2, 0):
            with self.subTest(retries=retries):
                conns, base = silent_server(self)
                async with AsyncClient(KEY, base_url=base, timeout=10, deadline=0.5,
                                       retries=retries) as mem:
                    with self.assertRaises(APIConnectionError) as cm:
                        await mem.list_stores()
                self.assertIn("deadline of 0.5s exhausted", str(cm.exception))
                self.assertEqual(len(conns), 1)
                with self.assertRaises(APIConnectionError) as cm:
                    Client(KEY, base_url=base, timeout=10, deadline=0.5, retries=retries).list_stores()
                self.assertIn("deadline of 0.5s exhausted", str(cm.exception))

    async def test_a_request_that_went_out_is_not_retried(self):
        seen, base = staged_server(self, [None])
        async with AsyncClient(KEY, base_url=base, timeout=0.5, retries=2) as mem:
            with self.assertRaises(APIConnectionError) as cm:
                await mem.add("x", "alice")
            self.assertIn("request timed out after 0.5s", str(cm.exception))
            with self.assertRaises(APIConnectionError):
                await mem.list_stores()
        self.assertEqual(len(seen), 2)


@needs_httpx
class AsyncPoolWaitTest(unittest.IsolatedAsyncioTestCase):

    def test_an_httpx_pool_timeout_was_never_sent(self):
        import httpx
        f = AsyncClient(KEY)._failure(httpx.PoolTimeout("timed out"))
        self.assertEqual((f.never_sent, f.timed_out, f.reason), (True, False, "PoolTimeout (timed out)"))

    async def wait_for_the_pool(self, mem):
        import asyncio
        holder = asyncio.create_task(mem.with_timeout(5).list_stores())
        await asyncio.sleep(0.3)
        try:
            for _ in range(4):
                with self.assertRaises(APIConnectionError) as cm:
                    await mem.with_timeout(0.3).with_retries(0).add("x", "alice")
                self.assertIn("network error: POST /api/v1/memory/store: PoolTimeout (timed out)",
                              str(cm.exception))
            t0 = time.monotonic()
            with self.assertRaises(APIConnectionError):
                await mem.with_timeout(0.3).with_retries(1).add("x", "alice")
            self.assertGreater(time.monotonic() - t0, 1.0)
        finally:
            holder.cancel()
            with contextlib.suppress(BaseException):
                await holder

    async def test_an_owned_client(self):
        from unittest import mock
        seen, base = staged_server(self, [None])
        one = lambda hx, key: hx.AsyncClient(limits=hx.Limits(max_connections=1))  # noqa: E731
        with mock.patch.object(wontopos, "_new_async_http", one):
            mem = AsyncClient(KEY, base_url=base)
        async with mem:
            await self.wait_for_the_pool(mem)
        self.assertEqual(len(seen), 1)

    async def test_an_httpx_client_passed_in(self):
        import httpx
        seen, base = staged_server(self, [None])
        async with httpx.AsyncClient(limits=httpx.Limits(max_connections=1)) as http:
            await self.wait_for_the_pool(AsyncClient(KEY, base_url=base, _http=http))
        self.assertEqual(len(seen), 1)


@needs_httpx
class AsyncForkTest(unittest.TestCase):

    def serve(self, n):
        srv, base = scripted_server([(200, {}, '{"total_memories": 0}')] * n)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return srv, base

    def test_an_unused_client_works_in_the_child(self):
        import asyncio
        srv, base = self.serve(1)
        mem = AsyncClient(KEY, base_url=base)
        inherited = mem._transport.http
        mem._transport.pid = -1

        async def child():
            await mem.stats("alice")
            await mem.aclose()

        asyncio.run(child())
        self.assertEqual(len(srv.seen), 1)
        self.assertIsNot(mem._transport.http, inherited)
        self.assertEqual(mem._transport.pid, os.getpid())

    def test_a_used_client_rebuilds_and_leaves_the_old_one_alone(self):
        import asyncio
        from unittest import mock
        srv, base = self.serve(2)
        mem = AsyncClient(KEY, base_url=base)
        asyncio.run(mem.stats("alice"))
        inherited = mem._transport.http
        mem._transport.pid = -1
        touched = mock.MagicMock(side_effect=AssertionError("the inherited client was used"))

        async def child():
            await mem.stats("alice")
            await mem.aclose()

        with mock.patch.object(inherited, "aclose", touched), \
                mock.patch.object(inherited, "stream", touched), \
                mock.patch.object(inherited, "send", touched):
            asyncio.run(child())
        touched.assert_not_called()
        self.assertEqual(len(srv.seen), 2)
        rebuilt = srv.seen[1]["headers"]
        self.assertEqual((rebuilt["X-API-Key"], rebuilt["Accept-Encoding"]), (KEY, "gzip"))
        self.assertTrue(rebuilt["User-Agent"].startswith("wontopos-python/"))
        self.assertFalse(mem._transport.http.follow_redirects)

    def test_clones_share_the_rebuilt_client(self):
        import asyncio
        import copy
        srv, base = self.serve(3)
        mem = AsyncClient(KEY, base_url=base)
        clone, dup = mem.with_user("bob"), copy.copy(mem)
        inherited = mem._transport.http
        mem._transport.pid = -1

        async def child():
            await clone.stats()
            rebuilt = mem._transport.http
            await dup.stats()
            await mem.stats("alice")
            self.assertIs(mem._transport.http, rebuilt)
            self.assertIs(clone._transport.http, rebuilt)
            self.assertIs(dup._transport.http, rebuilt)
            await mem.aclose()

        asyncio.run(child())
        self.assertIsNot(mem._transport.http, inherited)
        self.assertEqual(len(srv.seen), 3)

    def test_aclose_in_the_child_leaves_the_inherited_client_alone(self):
        import asyncio
        from unittest import mock
        mem = AsyncClient(KEY, base_url="http://127.0.0.1:9")
        mem._transport.pid = -1
        touched = mock.MagicMock(side_effect=AssertionError("the inherited client was closed"))
        with mock.patch.object(mem._transport.http, "aclose", touched):
            asyncio.run(mem.aclose())
        touched.assert_not_called()
        with self.assertRaises(APIConnectionError) as cm:
            asyncio.run(mem.stats("alice"))
        self.assertIn("aclose", str(cm.exception))

    def test_a_client_closed_before_fork_stays_closed(self):
        import asyncio
        mem = AsyncClient(KEY, base_url="http://127.0.0.1:9")
        asyncio.run(mem.aclose())
        mem._transport.pid = -1
        with self.assertRaises(APIConnectionError) as cm:
            asyncio.run(mem.stats("alice"))
        self.assertIn("aclose", str(cm.exception))

    def test_an_httpx_client_passed_in_is_still_refused(self):
        import asyncio
        srv, base = self.serve(1)
        mem = AsyncClient(KEY, base_url=base, _http=httpx.AsyncClient())
        mem._transport.pid = -1
        with self.assertRaises(APIConnectionError) as cm:
            asyncio.run(mem.stats("alice"))
        self.assertIn("fork", str(cm.exception))
        self.assertEqual(srv.seen, [])


class NoAsyncioLoopTest(AsyncMake, unittest.IsolatedAsyncioTestCase):

    async def test_the_loop_helper_skips_the_binding(self):
        from unittest import mock
        _, mem = self.amake([])
        with mock.patch("asyncio.get_running_loop", side_effect=RuntimeError("no running event loop")):
            http = mem._http_for_this_loop()
        self.assertIs(http, mem._transport.http)
        self.assertIsNone(mem._transport.loop)
        await mem.aclose()

    async def test_calls_retry_and_time_out_without_asyncio(self):
        from unittest import mock
        with mock.patch.object(wontopos, "_asyncio_loop", return_value=None):
            srv, mem = self.amake([(429, {"Retry-After": "0"}, RATE_429), (200, {}, '{"total_memories": 2}')])
            async with mem:
                self.assertEqual((await mem.stats("alice"))["total_memories"], 2)
            self.assertEqual(len(srv.seen), 2)
            self.assertIsNone(mem._transport.loop)
            _, base = staged_server(self, SlowErrorBodyTest.STALLED_429)
            async with AsyncClient(KEY, base_url=base, timeout=10) as mem:
                t0 = time.monotonic()
                self.assertEqual((await mem.add("x", "alice"))["id"], "m1")
            self.assertLess(time.monotonic() - t0, 3)
            _, base = trickle_server(self, WallClockTimeoutTest.HEAD, b" " * 40)
            async with AsyncClient(KEY, base_url=base, timeout=0.5, retries=0) as mem:
                t0 = time.monotonic()
                with self.assertRaises(APIConnectionError) as cm:
                    await mem.stats("alice")
            self.assertLess(time.monotonic() - t0, 1.3)
            self.assertIn("request timed out after 0.5s", str(cm.exception))
            conns, base = silent_server(self)
            async with AsyncClient(KEY, base_url=base, timeout=0.5, retries=1) as mem:
                with self.assertRaises(APIConnectionError):
                    await mem.add("x", "alice")
            self.assertEqual(len(conns), 2)


class ReadPostRetryTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):

    BUSY = (503, {"Retry-After": "0"}, b"{}")

    def test_sync(self):
        for status in (502, 503, 504):
            srv, mem = self.make([(status, {"Retry-After": "0"}, b"{}"), (200, {}, b'{"memories":[]}')])
            mem.search("q", "alice")
            self.assertEqual(len(srv.seen), 2, status)
        srv, mem = self.make([self.BUSY, (200, {}, b'{"status":"created"}')])
        with self.assertRaises(wontopos.ServerError):
            mem.create_store("alice")
        self.assertEqual(len(srv.seen), 1)

    async def test_async(self):
        srv, mem = self.amake([self.BUSY, (200, {}, b'{"long_term":{"memories":[]}}')])
        async with mem:
            await mem.recall("q", "alice")
        self.assertEqual(len(srv.seen), 2)


class UndecodableBodyTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):

    BAD_GZIP = (200, {"Content-Encoding": "gzip"}, b"this is not gzip")
    BAD_DEFLATE = (200, {"Content-Encoding": "deflate"}, b"\xff\xff not deflate either")

    def test_sync(self):
        srv, mem = self.make([self.BAD_GZIP] * 2, retries=1)
        with self.assertRaises(APIConnectionError) as cm:
            mem.list_models()
        self.assertEqual(cm.exception.status, 0)
        self.assertEqual(len(srv.seen), 2)
        srv, mem = self.make([self.BAD_GZIP] * 2, retries=1)
        with self.assertRaises(APIConnectionError):
            mem.stats("alice")
        self.assertEqual(len(srv.seen), 2)
        srv, mem = self.make([self.BAD_GZIP] * 2, retries=1)
        with self.assertRaises(APIConnectionError):
            mem.create_store("alice")
        self.assertEqual(len(srv.seen), 1)
        srv, mem = self.make([self.BAD_GZIP] * 2, retries=1)
        with self.assertRaises(APIConnectionError):
            mem.add("x", "alice")
        self.assertEqual(len(srv.seen), 2)

    async def test_async(self):
        for bad in (self.BAD_GZIP, self.BAD_DEFLATE):
            with self.subTest(coding=bad[1]["Content-Encoding"]):
                srv, mem = self.amake([bad] * 2, retries=1)
                async with mem:
                    with self.assertRaises(APIConnectionError) as cm:
                        await mem.list_models()
                self.assertEqual(cm.exception.status, 0)
                self.assertIn("GET /api/v1/models", str(cm.exception))
                self.assertEqual(len(srv.seen), 2)
                srv, mem = self.amake([bad] * 2, retries=1)
                async with mem:
                    with self.assertRaises(APIConnectionError):
                        await mem.stats("alice")
                self.assertEqual(len(srv.seen), 2)
                srv, mem = self.amake([bad] * 2, retries=1)
                async with mem:
                    with self.assertRaises(APIConnectionError):
                        await mem.create_store("alice")
                self.assertEqual(len(srv.seen), 1)
                srv, mem = self.amake([bad] * 2, retries=1)
                async with mem:
                    with self.assertRaises(APIConnectionError):
                        await mem.add("x", "alice")
                self.assertEqual(len(srv.seen), 2)

    async def test_async_error_status_still_stands(self):
        _, mem = self.amake([(404, {"Content-Encoding": "gzip"}, b"not gzip")])
        async with mem:
            with self.assertRaises(NotFoundError):
                await mem.stats("alice")


class RetirementWarningTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):

    SUNSET = {"Sunset": "Sat, 07 Nov 2026 00:00:00 GMT"}

    def setUp(self):
        wontopos._reset_warning_state()
        self.addCleanup(wontopos._reset_warning_state)

    def test_once_per_model_and_day(self):
        srv, mem = self.make([(200, self.SUNSET, '{"memories": []}')] * 2
                             + [(200, {"Sunset": "Sun, 08 Nov 2026 00:00:00 GMT"}, '{"memories": []}')],
                             model="scroll-1")
        with self.assertLogs("wontopos", level="WARNING") as logs:
            mem.search("q", "alice")
            mem.search("q", "alice")
        said = [r.getMessage() for r in logs.records]
        self.assertEqual(len(said), 1, said)
        self.assertIn('"scroll-1" retires on 2026-11-07', said[0])
        self.assertIn("410", said[0])
        with self.assertLogs("wontopos", level="WARNING") as logs:
            mem.search("q", "alice")
        self.assertEqual(len(logs.records), 1, "a new day is news")

    def test_no_header_or_not_a_date_says_nothing(self):
        srv, mem = self.make([(200, {}, '{"memories": []}'), (200, {"Sunset": "soon"}, '{"memories": []}')])
        with no_wontopos_warning(self):
            mem.search("q", "alice")
            mem.search("q", "alice")

    def test_a_per_call_model_is_the_one_named(self):
        srv, mem = self.make([(200, self.SUNSET, '{"memories": []}')])
        with self.assertLogs("wontopos", level="WARNING") as logs:
            mem.search("q", "alice", model="scroll-1")
        self.assertIn('"scroll-1"', logs.records[0].getMessage())

    async def test_async(self):
        srv, mem = self.amake([(200, self.SUNSET, b'{"memories": []}')], model="scroll-1")
        async with mem:
            with self.assertLogs("wontopos", level="WARNING") as logs:
                await mem.search("q", "alice")
        self.assertIn('"scroll-1" retires on 2026-11-07', logs.records[0].getMessage())


class RecallShapeTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):

    BODY = json.dumps({
        "short_term": {"turns": [{"role": "user", "content": "hi"}, "not a turn", None], "count": 3},
        "long_term": {"memories": [{"id": "m1", "content": "tea"}, 7], "count": 2},
        "context": {"around_top_memory": ["[before] a", 3, {"x": 1}, "[match] b"], "count": 4},
        "instruction": "kept",
    })

    def check(self, r):
        self.assertEqual(r["short_term"]["turns"], [{"role": "user", "content": "hi"}])
        self.assertEqual(r["long_term"]["memories"], [{"id": "m1", "content": "tea"}])
        self.assertEqual(r["context"]["around_top_memory"], ["[before] a", "[match] b"])
        self.assertEqual(r["short_term"]["count"], 3)
        self.assertEqual(r["instruction"], "kept")

    def test_sync(self):
        _, mem = self.make([(200, {}, self.BODY), (200, {}, "{}")])
        self.check(mem.recall("q", "alice"))
        empty = mem.recall("q", "alice")
        self.assertEqual([empty["short_term"]["turns"], empty["long_term"]["memories"],
                          empty["context"]["around_top_memory"]], [[], [], []])

    async def test_async(self):
        _, mem = self.amake([(200, {}, self.BODY)])
        async with mem:
            self.check(await mem.recall("q", "alice"))


class PageShapeTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):

    SCRIPT = [
        (200, {}, "{}"), (200, {}, "{}"), (200, {}, "{}"), (200, {}, "{}"),
        (200, {}, '{"memories": [{"id": "a"}, 7, null], "count": 3, "next_cursor": "C"}'),
        (200, {}, '{"memories": [{"id": "b"}], "next_cursor": ""}'),
        (200, {}, '{"images": [{"id": "i"}, "x"], "count": 9}'),
        (200, {}, '{"speaker": "Dana", "memories": [null, {"id": "b"}]}'),
        (200, {}, '{"chain": [{"memory_id": "c"}, [1]]}'),
        (200, {}, json.dumps({
            "short_term": {"turns": [{"role": "user", "content": "hi"}]},
            "long_term": {"memories": [{"id": "m1"}, {"id": "m2"}], "count": 7},
            "context": {},
            "rooms": {"rooms": [{"name": "m0", "found": 1}], "memories": [{"id": "r1"}, "x"]},
            "images": {"count": 0},
        })),
        (200, {}, '{"rooms": null, "rooms_off": "no clock"}'),
    ]

    async def check(self, mem):
        empty = {"memories": [], "count": 0, "next_cursor": None}
        self.assertEqual(await done(mem.list_memories("u")), empty)
        self.assertEqual(await done(mem.list_images("u")), {"images": []})
        self.assertEqual(await done(mem.by_speaker("Dana", "u")), {"memories": []})
        self.assertEqual(await done(mem.lineage("u", "m1")), {"chain": []})
        self.assertEqual(await done(mem.list_memories("u")), {"memories": [{"id": "a"}], "count": 3, "next_cursor": "C"})
        last = await done(mem.list_memories("u", cursor="C"))
        self.assertEqual(last, {"memories": [{"id": "b"}], "count": 1, "next_cursor": None})
        self.assertEqual(await done(mem.list_images("u")), {"images": [{"id": "i"}], "count": 9})
        self.assertEqual(await done(mem.by_speaker("Dana", "u")), {"speaker": "Dana", "memories": [{"id": "b"}]})
        self.assertEqual(await done(mem.lineage("u", "c")), {"chain": [{"memory_id": "c"}]})
        r = await done(mem.recall("q", "u"))
        self.assertEqual([r["short_term"]["count"], r["long_term"]["count"], r["context"]["count"]], [1, 7, 0])
        self.assertEqual(r["rooms"], {"rooms": [{"name": "m0", "found": 1}], "memories": [{"id": "r1"}], "count": 1})
        self.assertEqual(r["images"], {"memories": [], "count": 0})
        off = await done(mem.recall("q", "u"))
        self.assertIsNone(off["rooms"])
        self.assertNotIn("images", off)
        self.assertEqual([off["short_term"]["count"], off["long_term"]["count"], off["context"]["count"]], [0, 0, 0])

    async def test_sync(self):
        _, mem = self.make(self.SCRIPT)
        await self.check(mem)

    async def test_async(self):
        _, mem = self.amake(self.SCRIPT)
        async with mem:
            await self.check(mem)


async def done(x):
    return await x if inspect.isawaitable(x) else x


def headers_of(seen):
    return {k.lower(): v for k, v in seen["headers"].items()}


class CoreParityCases:

    async def test_what_a_call_is_given_is_trimmed_as_javascript_trims_it(self):
        srv, mem = self.new()
        await done(mem.get("u", "﻿m1"))
        await done(mem.delete("u", "m1\u0085"))
        await done(mem.lineage("u", "﻿m1　"))
        await done(mem.seen("﻿2026-10-19T09:00:00Z", "u"))
        await done(mem.set_settings("u", time_zone="Asia/Seoul﻿"))
        await done(mem.work_deeper("﻿w-1\x1c", "u"))
        await done(mem.add("x", "u", image={"data": "﻿aG﻿k=\u0085"}))
        await done(mem.delete_all("\x1c"))
        got = [json.loads(s["body"]) for s in srv.seen]
        self.assertEqual([g["memory_id"] for g in got[:3]], ["m1", "m1\u0085", "m1"])
        self.assertEqual((got[3]["at"], got[4]["time_zone"], got[5]["work_id"]),
                         ("2026-10-19T09:00:00Z", "Asia/Seoul", "w-1\x1c"))
        self.assertEqual((got[6]["image"]["data"], got[7]["user_id"]), ("aGk=\u0085", "\x1c"))
        for call, message in (
            (lambda: mem.get("u", "﻿"), "memory_id is required"),
            (lambda: mem.seen("﻿", "u"), "at is required"),
            (lambda: mem.stats("﻿"), "user_id must be a non-blank string"),
            (lambda: mem.delete_store(" ﻿"), "user_id is required"),
            (lambda: mem.forget_speaker("Dana", "﻿"), "user_id is required"),
        ):
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                await done(call())
        self.assertEqual(len(srv.seen), 8)

    async def test_a_key_and_a_base_url_are_trimmed_as_javascript_trims_them(self):
        srv, _ = self.new()
        base = f"http://127.0.0.1:{srv.server_port}"
        await done(self.fresh("﻿" + KEY + " ", "﻿" + base + "　").stats("u"))
        self.assertEqual(headers_of(srv.seen[0])["x-api-key"], KEY)
        for key in (KEY + "\u0085", "\x1c" + KEY):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "api_key contains"):
                self.fresh(key, base)
        with self.assertRaisesRegex(ValueError, "base_url"):
            self.fresh(KEY, base + "\u0085")
        self.assertEqual(len(srv.seen), 1)

    async def test_an_answer_that_starts_with_a_byte_order_mark_is_read_as_typescript_reads_it(self):
        bom = "﻿"
        srv, mem = self.new([(200, {}, bom + '{"total_memories": 3}'), (409, {}, bom + LOCK_409),
                             (200, {}, bom + '{"id": "m1"}'), (410, {}, bom + ErrorFieldsTest.EXPIRED)])
        self.assertEqual((await done(mem.stats("u")))["total_memories"], 3)
        self.assertEqual((await done(mem.add("x", "u")))["id"], "m1")
        with self.assertRaises(WosError) as cm:
            await done(mem.sleep_done("s-1", "u"))
        self.assertIs(type(cm.exception), WosError)
        self.assertEqual((cm.exception.code, cm.exception.request_id), ("sleep_expired", "req_exp"))
        self.assertEqual(len(srv.seen), 4)

    async def test_a_model_of_none_is_the_default_model_as_typescript_reads_null(self):
        srv, mem = self.new(model=None)
        await done(mem.stats("u"))
        await done(mem.with_model(None).stats("u"))
        await done(mem.with_model("").stats("u"))
        self.assertEqual([headers_of(s).get("x-wos-model") for s in srv.seen],
                         [wontopos.DEFAULT_MODEL, wontopos.DEFAULT_MODEL, None])

    async def test_a_redirect_is_refused_before_anything_is_read_from_it(self):
        moved = {"Location": "https://example.com/", "X-RateLimit-Limit": "60", "X-RateLimit-Remaining": "59",
                 "Sunset": "Tue, 07 Dec 2027 00:00:00 GMT"}
        srv, mem = self.new([(302, moved, "")])
        with no_wontopos_warning(self), self.assertRaisesRegex(WosError, "unexpected redirect"):
            await done(mem.stats("u"))
        self.assertIsNone(mem.rate_limit)

    async def test_a_control_character_inside_an_id_is_sent_in_the_body_as_typescript_sends_it(self):
        srv, mem = self.new([(200, {}, "PNG")])
        await done(mem.get_image("u", "m\x01a"))
        await done(mem.forget_image("u", "m\x01a"))
        await done(mem.lineage("u", "m\x01a"))
        await done(mem.by_speaker("Da\x01na", "u"))
        got = [json.loads(s["body"]) for s in srv.seen]
        self.assertEqual([g["memory_id"] for g in got[:3]] + [got[3]["speaker"]], ["m\x01a"] * 3 + ["Da\x01na"])

    async def test_an_empty_cursor_is_left_out_as_typescript_leaves_it_out(self):
        srv, mem = self.new()
        await done(mem.list_memories("u", cursor=""))
        await done(mem.list_memories("u", cursor="c-2"))
        got = [json.loads(s["body"]) for s in srv.seen]
        self.assertEqual((got[0], got[1]["cursor"]), ({"user_id": "u", "limit": 100}, "c-2"))

    async def test_a_netrc_login_is_sent_nowhere_and_a_base_urls_own_login_is_sent_as_before(self):
        import tempfile
        from unittest import mock
        srv, mem = self.new()
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "netrc")
            with open(path, "w") as f:
                f.write("default login someone password not-for-this-host\n")
            with mock.patch.dict(os.environ, {"NETRC": path}):
                await done(mem.stats("u"))
                await done(self.fresh(KEY, f"http://u:p@127.0.0.1:{srv.server_port}").stats("u"))
        self.assertEqual([headers_of(s).get("authorization") for s in srv.seen], [None, "Basic dTpw"])


class SyncCoreParityTest(SyncMake, CoreParityCases, unittest.IsolatedAsyncioTestCase):
    def new(self, script=(), **kw):
        return self.make(list(script), **kw)

    def fresh(self, api_key, base_url):
        return Client(api_key, base_url=base_url)


class AsyncCoreParityTest(AsyncMake, CoreParityCases, unittest.IsolatedAsyncioTestCase):
    def new(self, script=(), **kw):
        srv, mem = self.amake(list(script), **kw)
        self.addAsyncCleanup(mem.aclose)
        return srv, mem

    def fresh(self, api_key, base_url):
        mem = AsyncClient(api_key, base_url=base_url)
        self.addAsyncCleanup(mem.aclose)
        return mem


if __name__ == "__main__":
    unittest.main()
