# Offline tests: a scripted localhost HTTP responder, no network, no pytest dep.
# Run from sdk/python:  python3 -m unittest discover -s tests -v
import contextlib
import logging
import json
import os
import re
import sys
import socket
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests

# Test the source in this repo, never whatever is installed.
#
# Without this, a bare `import wontopos` resolves to whatever is on sys.path, which
# is usually an installed build. The failure then reads as "the SDK lost a function"
# when the real answer is that nothing said which copy to test. src/ goes first.
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

# For a test that builds an AsyncClient: without the async extra it is skipped, not failed.
needs_httpx = unittest.skipUnless(HAVE_HTTPX, "httpx not installed (pip install 'wontopos[async]')")


class ScriptedHandler(BaseHTTPRequestHandler):
    """Serves the responses scripted on the server object, recording each request."""

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

    def log_message(self, *_):  # keep test output clean
        pass


def scripted_server(script):
    srv = HTTPServer(("127.0.0.1", 0), ScriptedHandler)
    srv.script = script
    srv.seen = []
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_port}"


def dropping_server(script):
    """Raw-socket server: for each script entry, accept ONE connection.

    ``None``       — read the request head, close WITHOUT responding. This is the
                     CONNECT/HEADER phase, not mid-stream: the body reader is never
                     reached.
    ``"TRUNCATE"`` — send a 200 head promising 200 bytes, send 5, then close. THIS is
                     a mid-body drop: the response existed and the body did not finish.
    ``(code, ct, s)`` — send that status and content-type with ``s`` as the whole body.
    ``(code, "TRUNCATE")`` — the same mid-body drop under an ERROR status. The status
                     arrived and the message body did not, which is the case where the
                     two have to be told apart: the read failed, but the status is what
                     the caller asked about.
    any other str  — served as a normal 200 response.

    Returns (accept_counter_list, base_url)."""
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
    """Raw-socket server that sends each response one byte every ``interval`` seconds.

    With ``trickle_head`` the status line and headers trickle too; otherwise they arrive
    at once and only the body trickles. Returns (requests_seen, base_url)."""
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
    """Raw-socket server: connection n gets ``entries[n]``, the last entry repeating.

    An entry is ``(status, headers_dict, body, body_delay)``: the head goes out at once
    and the body after ``body_delay`` seconds. ``None`` holds the connection open
    without answering. The request body is read in full first.
    Returns (requests_seen, base_url)."""
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
    """A loopback URL nothing listens on, so a connect is refused at once."""
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
        # A get_image() that actually reaches the network: the blank-id cases fail the
        # id guard before any request is built.
        srv, mem = self.make([
            (429, {"Retry-After": "0"}, '{"error": "rate limited"}'),
            (200, {"Content-Type": "image/webp"}, "PNGBYTES"),
        ])
        data, mime = mem.get_image(memory_id="11111111-1111-1111-1111-111111111111")
        self.assertEqual(data, b"PNGBYTES")
        self.assertIn("image/webp", mime)  # the fixture prepends its own Content-Type
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
        mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")  # never reached
        with self.assertRaises(ValueError):
            mem.delete()
        with self.assertRaises(ValueError):
            mem.delete(memory_id=None)
        with self.assertRaises(ValueError):
            mem.delete_all("")
        with self.assertRaises(ValueError):
            mem.delete_store("")

    def test_image_and_lineage_calls_require_a_memory_id(self):
        # These three name ONE memory, like delete and get, so a blank or non-string id
        # is refused before the network, with a sentence about the argument.
        mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")  # never reached
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
        # Both names work, but an explicit `retries` wins. Comparing the given value
        # against the default 2 could not tell "the caller said 2" from "the caller
        # said nothing", so retries=2 was overridden by the alias.
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
        mem.stats(model="scroll-1")  # per-call override
        self.assertEqual(srv.seen[0]["headers"].get("X-WOS-Model"), "scroll-1")
        self.assertEqual(srv.seen[1]["headers"].get("X-WOS-Model"), "scroll-1")
        # UA = version + runtime platform info (support debugging), never anything identifying.
        ua = srv.seen[0]["headers"].get("User-Agent")
        self.assertTrue(ua.startswith(f"wontopos-python/{__version__} (python/"), ua)

    def test_search_opts_cannot_override_reserved_fields(self):
        # An app that forwards untrusted input as **opts must not override the
        # reserved fields. user_id/query/limit bind to params (never opts); the
        # one field reachable via opts is max_results (param is named `limit`),
        # so pin that the limit param wins while other opts still pass through.
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
        # The count is 5..20 for search as for recall, checked at both ends and on every
        # search method. ValueError, not WosError: nothing was sent. `WosError` with
        # status 0 means APIConnectionError — "the request never got a response" — and a
        # caller branching on that would retry a typo forever.
        _, mem = self.make([])
        for bad in (0, 1, 4, 21, 50, 100):
            for call in (mem.search, mem.search_full, mem.search_self):
                with self.assertRaises(ValueError) as cm:
                    call("q", "alice", bad)
                self.assertIn("between 5 and 20", str(cm.exception))
                self.assertNotIsInstance(cm.exception, WosError)

    def test_search_full_keeps_what_search_merges_away(self):
        # search() answers with one merged list; search_full keeps the photos and the
        # count of re-ask passes run as fields of their own.
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
        # the merged call carries the photos too, after the text
        self.assertEqual([m["id"] for m in mem.search("q", user_id="alice")], ["m1", "s1", "i1"])

    def test_search_full_normalizes_a_nulled_field(self):
        srv, mem = self.make([(200, {}, '{"memories": null, "images": null}')])
        r = mem.search_full("q", user_id="alice")
        self.assertEqual(r["memories"], [])
        self.assertEqual(r["images"], [])
        self.assertEqual(r["self_memories"], [])
        self.assertNotIn("verify_used", r)

    def test_search_self_returns_both_fields(self):
        # search_self gives back both `memories` and `self_memories` (the
        # assistant's own words) from ONE call.
        srv, mem = self.make([(200, {}, '{"memories": [{"id": "m1"}], "self_memories": [{"id": "s1"}]}')])
        r = mem.search_self("q", user_id="alice")
        self.assertEqual([m["id"] for m in r["memories"]], ["m1"])
        self.assertEqual([m["id"] for m in r["self_memories"]], ["s1"])
        # one round-trip, one /search call
        self.assertEqual(len(srv.seen), 1)
        self.assertEqual(srv.seen[0]["path"], "/api/v1/memory/search")

    def test_revisions_goes_to_the_won_surface(self):
        # Won is a separate address, not a rename: calls a model makes ABOUT its memory
        # live under /api/v1/won/*. The old /memory path still answers, so nothing fails
        # if this drifts back — the page would just teach an address the SDK never calls.
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
        # The API ignores a field it does not know and answers 200, so verfy=3 sent
        #   as-is would re-ask nothing while the caller believed it had. Filters only
        #   warn because a wrong filter still returns memories; this one returns a
        #   normal answer that is quietly worse.
        srv, mem = self.make([(200, {}, '{"memories": []}')])
        with self.assertRaises(ValueError) as e:
            mem.search("q", user_id="alice", verfy=3)
        self.assertIn("verfy", str(e.exception))
        self.assertIn("verify", str(e.exception))  # names what was meant
        self.assertEqual(srv.seen, [], "a refused option must not reach the wire")

    def test_extra_is_the_forward_compatible_way_through(self):
        # Refusing unknown keys would otherwise mean a new service option cannot be
        #   used until this client learns it. `extra` is the declared way past.
        srv, mem = self.make([(200, {}, '{"memories": []}')])
        mem.search("q", user_id="alice", extra={"future_option": 1})
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body.get("future_option"), 1)
        self.assertNotIn("extra", body)

    def test_omitting_them_sends_neither_key(self):
        # What was not passed is not sent, so a call that names neither option gets
        # exactly the service's defaults.
        srv, mem = self.make([(200, {}, '{"memories": []}')])
        mem.search("q", user_id="alice")
        body = json.loads(srv.seen[0]["body"])
        self.assertNotIn("verify", body)
        self.assertNotIn("max_images", body)

    def test_revisions_asks_for_counts_only_unless_a_page_is_requested(self):
        # What this test protects: **the response stays the same size for a store of a
        #   hundred million memories.** That can only be checked by what is NOT sent,
        #   never by a value — the moment the SDK slips a default into include, calls
        #   that asked for nothing start carrying twenty rows.
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
        # If the cursor loses its name the engine reads "no cursor", and the caller gets
        # **page one forever**, without a single error.
        self.assertEqual(body["skip_ids"], ["a-1"])

    def test_search_self_null_or_missing_self_memories_is_empty_list(self):
        # A non-self model (or a broken proxy) omits/nulls self_memories → [], not None.
        srv, mem = self.make([(200, {}, '{"memories": [{"id": "m1"}], "self_memories": null}')])
        r = mem.search_self("q", user_id="u")
        self.assertEqual(r["self_memories"], [])
        self.assertEqual([m["id"] for m in r["memories"]], ["m1"])

    def test_bad_elements_are_skipped_not_passed_through(self):
        # The CONTAINER guard (`memories` is a list) is not enough: a hostile or
        # broken server can put non-objects INSIDE the array. Unfiltered, those reach
        # the caller typed as memories, so the first `m["content"]` raised a raw
        # TypeError from inside user code. Every element that isn't an object is
        # dropped, and the valid memories still come back.
        bad = '{"memories": [null, 1, "x", [], {"id": "ok", "content": "c"}]}'
        _, mem = self.make([(200, {}, bad)])
        r = mem.search("q", user_id="u")
        self.assertEqual([m["id"] for m in r], ["ok"])
        # same guard on `self_memories` and on history turns
        _, mem = self.make([(200, {}, '{"memories": [null, {"id": "m1"}], "self_memories": [2, {"id": "s1"}]}')])
        r = mem.search_self("q", user_id="u")
        self.assertEqual([m["id"] for m in r["memories"]], ["m1"])
        self.assertEqual([m["id"] for m in r["self_memories"]], ["s1"])
        _, mem = self.make([(200, {}, '{"turns": [null, "x", {"role": "user"}]}')])
        self.assertEqual(mem.history(user_id="u"), [{"role": "user"}])

    def test_search_returns_both_fields(self):
        # Some models answer with the assistant's OWN words in `self_memories` and
        # not repeated in `memories`. search() read only `memories`, so an assistant
        # turn stored with add_turn was missing from its results on those models
        # while the same query returned it on others. What went missing was already
        # on the wire and already paid for.
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
        # get()/delete() take the STORE first, unlike every payload-first method here.
        # With a store set on the client, mem.get(id) is what people write — and the
        # id landed in user_id, so it raised. A lone UUID can only be the memory id.
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
        # A truthy WRONG-type field (server sends memories="x") must coerce to []/{}, not
        # slip through as a str/int/dict and blow up the caller's `for m in …`. `or []`
        # only replaced FALSY values; this pins the isinstance coercion.
        srv, mem = self.make([
            (200, {}, '{"memories": "HACKED"}'),
            (200, {}, '{"turns": 42}'),
            (200, {}, '{"memory": "not-a-dict"}'),
            (200, {}, '{"memories": {"a": 1}, "self_memories": "x"}'),
        ])
        self.assertEqual(mem.search("q", "u"), [])            # non-list memories -> []
        self.assertEqual(mem.history("u"), [])                # non-list turns -> []
        self.assertEqual(mem.get("u", memory_id="x"), {})     # non-dict memory -> {}
        r = mem.search_self("q", "u")
        self.assertEqual(r, {"memories": [], "self_memories": []})  # both fields coerced

    def test_recall_and_engram_send_form_and_tz(self):
        # form/tz reach the body (Scroll 1.2+ renders memory times) — the docs said
        # forms work on recall/engram but the SDK had no way to pass them.
        srv, mem = self.make([(200, {}, '{"long_term":{"memories":[]}}'), (200, {}, '{"memories":[]}')])
        mem.recall("q", "u", form="archive", tz=9)
        b = json.loads(srv.seen[0]["body"])
        self.assertEqual(b["form"], "archive")
        self.assertEqual(b["tz"], 9)
        mem.engram("deep_recall", "q", "u", form="memoir")
        b2 = json.loads(srv.seen[1]["body"])
        self.assertEqual(b2["form"], "memoir")

    def test_post_write_not_retried_on_5xx(self):
        # A 502 on a POST write must NOT retry: the write may already have landed,
        # and a retry would store it twice. Only one request should be made.
        srv, mem = self.make([(502, {}, '{"error": "bad gateway"}')])
        with self.assertRaises(WosError) as cm:
            mem.add("hello", user_id="u")
        self.assertEqual(cm.exception.status, 502)
        self.assertEqual(len(srv.seen), 1)  # no retry on a non-idempotent write

    def test_get_retried_on_503(self):
        # 503 on an idempotent GET (/models) is safe to retry.
        srv, mem = self.make([
            (503, {"Retry-After": "0"}, "{}"),
            (200, {}, '{"models": []}'),
        ])
        mem.list_models()
        self.assertEqual(len(srv.seen), 2)  # retried

    def test_a_long_error_envelope_keeps_its_message_and_request_id(self):
        # The cap ran on the raw body BEFORE json.loads, so an envelope over 4096
        # chars stopped being valid JSON and the parse fell through: a 400 whose
        # message was two words arrived as cut-off JSON text, with no
        # request_id — the one value support asks for.
        env = json.dumps({"type": "error", "error": {
            "type": "invalid_request_error", "message": "bad input",
            "request_id": "req_ABC123", "detail": "x" * 5000}})
        srv, mem = self.make([(400, {}, env)])
        with self.assertRaises(WosError) as cm:
            mem.stats()
        self.assertEqual(cm.exception.message, "bad input")
        self.assertEqual(cm.exception.request_id, "req_ABC123")

    def test_a_huge_error_string_is_still_capped(self):
        # The other half: capping after the parse must still bound the message.
        srv, mem = self.make([(400, {}, json.dumps({"error": "Z" * 9000}))])
        with self.assertRaises(WosError) as cm:
            mem.stats()
        self.assertLessEqual(len(cm.exception.message), 4200)
        self.assertTrue(cm.exception.message.endswith("…(truncated)"))

    def test_get_retried_on_504_and_408(self):
        # Same rule as 502/503. A gateway that stopped waiting (504) and an
        # intermediary-authored 408 are both ambiguous for a write and both safe for
        # an idempotent read.
        for status in (504, 408):
            with self.subTest(status=status):
                srv, mem = self.make([
                    (status, {"Retry-After": "0"}, "{}"),
                    (200, {}, '{"models": []}'),
                ])
                mem.list_models()
                self.assertEqual(len(srv.seen), 2, f"{status} was not retried")

    def test_post_write_not_retried_on_504_or_408(self):
        # The other half: widening the set must not start retrying writes.
        for status in (504, 408):
            with self.subTest(status=status):
                srv, mem = self.make([(status, {}, '{"error": "gateway"}')])
                with self.assertRaises(WosError) as cm:
                    mem.add("hello", user_id="u")
                self.assertEqual(cm.exception.status, status)
                self.assertEqual(len(srv.seen), 1, f"{status} retried a write")

    def test_backoff_parses_http_date_retry_after(self):
        from email.utils import formatdate
        # HTTP-date ~10s in the future → a positive wait (capped at 30).
        future = formatdate(time.time() + 10, usegmt=True)
        self.assertTrue(1.0 <= wontopos._backoff(0, future) <= 30.0)
        # A date in the past → 0 (never negative).
        past = formatdate(time.time() - 100, usegmt=True)
        self.assertEqual(wontopos._backoff(0, past), 0.0)

    def test_non_serializable_body_is_type_error_not_network(self):
        # A non-serializable body is a client bug — must be a TypeError, not an
        # APIConnectionError that sends people debugging their network.
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
        # A server that returns the same next_cursor forever must not loop forever, and
        # the partial walk must not pass for the whole store.
        srv, mem = self.make([
            (200, {}, '{"memories": [{"id": "1"}], "next_cursor": "C"}'),
            (200, {}, '{"memories": [{"id": "2"}], "next_cursor": "C"}'),
        ])
        out = []
        with self.assertRaises(RuntimeError) as cm:
            for m in mem.iter_memories(user_id="u", page_size=1):
                out.append(m["id"])
        self.assertIn("truncated", str(cm.exception))
        self.assertEqual(len(srv.seen), 2)  # stopped after the cursor repeated
        self.assertEqual(out, ["1", "2"])

    def test_delete_all_rejects_blank_and_whitespace(self):
        # A blank OR whitespace user_id would server-side resolve to "default" and
        # wipe it — the guard must reject all of them before any request.
        mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")
        for uid in ["", " ", "\t", "\n", "   ", " "]:
            with self.assertRaises(ValueError):
                mem.delete_all(uid)

    def test_backoff_honors_retry_after_and_caps(self):
        self.assertEqual(wontopos._backoff(0, "2"), 2.0)
        self.assertEqual(wontopos._backoff(0, "999"), 30.0)
        self.assertTrue(4.0 <= wontopos._backoff(3) <= 4.25)
        self.assertTrue(8.0 <= wontopos._backoff(10) <= 8.25)

    # ----- connection-error retry gating (no duplicate write) -----

    def test_post_not_retried_on_midstream_drop(self):
        # The server accepts, reads the request, then drops the connection with
        # no response. For a POST the write may already have landed — the client
        # must NOT fire it again.
        accepted, base = dropping_server([None, None])
        mem = Client("wos-test-xxxxxxxxxx", base_url=base, retries=1)
        with self.assertRaises(wontopos.APIConnectionError):
            mem.add("hello", user_id="u")
        self.assertEqual(len(accepted), 1)  # exactly one attempt

    def test_get_retried_on_midstream_drop(self):
        # The same drop on an idempotent GET is safe to retry.
        accepted, base = dropping_server([None, '{"models": []}'])
        mem = Client("wos-test-xxxxxxxxxx", base_url=base, retries=1)
        self.assertEqual(mem.list_models(), [])
        self.assertEqual(len(accepted), 2)  # dropped once, then retried

    def test_never_sent_classifies_connect_failures(self):
        # Connect-level failures (refused/DNS/connect-timeout) never reached the
        # server → safe to retry even for writes. Mid-stream drops are not.
        from urllib3.exceptions import MaxRetryError, NewConnectionError, ProtocolError

        refused = requests.exceptions.ConnectionError(
            MaxRetryError(None, "/", NewConnectionError(None, "connection refused"))
        )
        self.assertTrue(wontopos._never_sent(refused))
        self.assertTrue(wontopos._never_sent(requests.exceptions.ConnectTimeout()))
        midstream = requests.exceptions.ConnectionError(ProtocolError("Connection aborted."))
        self.assertFalse(wontopos._never_sent(midstream))

    def test_body_read_failure_is_api_connection_error(self):
        # Headers arrive, then the body is cut short of Content-Length — must
        # surface as APIConnectionError, not a raw urllib3 internal.
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
        # Depending on the urllib3 version this surfaces as a transport error
        # (APIConnectionError) or an invalid-JSON error — both are WosError;
        # a raw urllib3/json internal must never escape.
        with self.assertRaises(WosError):
            mem.stats()

    def test_timeout_nan_or_zero_means_the_default(self):
        for unset in (float("nan"), 0):
            self.assertEqual(Client("wos-test-xxxxxxxxxx", timeout=unset)._timeout, 30.0)

    # ----- pending release: per-call tuning clones, close(), debug logging -----

    def test_with_retries_and_with_timeout_clones(self):
        # with_retries(0) must disable retries on the clone (parent unaffected).
        srv, mem = self.make([
            (429, {"Retry-After": "0"}, '{"error": "rate limited"}'),
            (200, {}, "{}"),
        ])
        with self.assertRaises(WosError) as cm:
            mem.with_retries(0).stats()
        self.assertEqual(cm.exception.status, 429)
        self.assertEqual(len(srv.seen), 1)  # clone did not retry
        mem.with_timeout(5).stats()  # clone still works end-to-end
        self.assertEqual(len(srv.seen), 2)

    def test_close_is_idempotent(self):
        srv, mem = self.make([(200, {}, "{}")])
        mem.stats()
        mem.close()
        mem.close()  # double-close must not raise

    def test_debug_logging_via_standard_logger(self):
        # The "wontopos" logger logs method/path/status/timing — and must never
        # log the API key or any request/response content.
        srv, mem = self.make([(200, {}, '{"memories": []}')])
        with self.assertLogs("wontopos", level="DEBUG") as captured:
            mem.search("super secret query text")
        joined = "\n".join(captured.output)
        self.assertIn("POST /api/v1/memory/search -> 200", joined)
        self.assertNotIn("super secret query text", joined)
        self.assertNotIn("wos-test", joined)

    def test_get_single_memory(self):
        # get() unwraps the memory object; the id guard trips before any request.
        srv, mem = self.make([
            (200, {}, '{"user_id": "u", "memory": {"id": "9b2d", "content": "tea", "is_superseded": false}}'),
        ])
        m = mem.get(user_id="u", memory_id="9b2d")
        self.assertEqual(m["content"], "tea")
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body["memory_id"], "9b2d")
        with self.assertRaises(ValueError):
            mem.get(user_id="u")  # no memory_id → never reaches the network

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
        # "store", "model", "user"... are common tags; the service drops them and keeps the memory.
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

    # ----- malformed responses -----

    def test_non_object_2xx_body_is_wos_error(self):
        # A 2xx whose body parses to a NON-object (null / number / string / array)
        # must be a clean WosError, not a raw AttributeError when a method does .get().
        for body in ("null", "12345", '"hi"', "[1,2,3]"):
            srv, mem = self.make([(200, {}, body)])
            with self.assertRaises(WosError):
                mem.search("q", "u")
            srv2, mem2 = self.make([(200, {}, body)])
            with self.assertRaises(WosError):
                mem2.get("u", "9b2d8c1e-1111-4222-8333-444455556666")


    def test_read_capped_incremental_cap(self):
        # A response with NO Content-Length (so the header check can't short-circuit)
        # must still be capped as chunks arrive — the decompression-bomb defense.
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
        # A hostile server's giant error body must not become a giant exception.
        srv, mem = self.make([(400, {}, '{"error": "' + "Z" * 100000 + '"}')])
        with self.assertRaises(WosError) as cm:
            mem.stats()
        self.assertLessEqual(len(cm.exception.message), 4096 + 20)
        self.assertIn("truncated", cm.exception.message)



    # ----- clones share the connection pool -----

    def test_clones_share_the_session_pool(self):
        mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")
        for clone in (mem.with_model("scroll-1"), mem.with_user("bob"), mem.with_timeout(5), mem.with_retries(0)):
            self.assertIs(clone._session, mem._session)  # same pool → connection reuse
            self.assertFalse(clone._owns_session)        # a clone does not own the session
        self.assertTrue(mem._owns_session)

    def test_clone_close_does_not_close_parent_session(self):
        # A transient clone (with_model(...).recall(...)) must not tear down the
        # pool the parent still uses.
        srv, mem = self.make([(200, {}, "{}"), (200, {}, "{}")])
        mem.with_model("scroll-1").close()  # clone close is a no-op on the shared session
        mem.stats()                          # parent still works
        self.assertEqual(len(srv.seen), 1)

    def test_model_still_sent_after_pool_sharing(self):
        # The model moved off the shared session's headers to per-request — make
        # sure a clone (and a per-call override) still send X-WOS-Model.
        srv, mem = self.make([(200, {}, "{}"), (200, {}, "{}"), (200, {}, "{}")])
        mem.with_model("scroll-1").stats()
        mem.stats(model="tablet-1")
        mem.stats()  # no model named -> the SDK default
        self.assertEqual(srv.seen[0]["headers"].get("X-WOS-Model"), "scroll-1")
        self.assertEqual(srv.seen[1]["headers"].get("X-WOS-Model"), "tablet-1")
        # Read the default from the SDK rather than repeating it: a literal here
        # keeps passing the day the default changes, and says nothing about whether
        # the header still carries it.
        self.assertEqual(srv.seen[2]["headers"].get("X-WOS-Model"), wontopos.DEFAULT_MODEL)

    def test_null_memories_and_turns_yield_empty_lists(self):
        # A broken proxy sending `"memories": null` must yield [], not None.
        srv, mem = self.make([
            (200, {}, '{"memories": null}'),
            (200, {}, '{"turns": null}'),
            (200, {}, '{"memories": null, "next_cursor": null}'),
        ])
        self.assertEqual(mem.search("q"), [])
        self.assertEqual(mem.history(), [])
        self.assertEqual(list(mem.iter_memories()), [])

    # ----- security round 2 -----

    def test_key_hygiene(self):
        mem = Client(" wos-test-xxxxxxxxxx\n", base_url="http://127.0.0.1:9")  # trimmed
        self.assertIn("wos-", repr(mem))
        with self.assertRaises(ValueError):
            Client("wos-test xxxxxxxxxx")  # inner whitespace = paste error
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
        self.assertIsNone(mem.rate_limit)  # nothing before the first call
        mem.list_stores()
        self.assertEqual(mem.rate_limit, {"limit": 150, "remaining": 3, "reset": 1720000000})


@unittest.skipUnless(HAVE_HTTPX, "httpx not installed (pip install 'wontopos[async]')")
class AsyncClientTests(unittest.IsolatedAsyncioTestCase):
    def make(self, script, **kw):
        srv, base = scripted_server(script)
        self.addCleanup(srv.shutdown)
        from wontopos import AsyncClient

        return srv, AsyncClient("wos-test-xxxxxxxxxx", base_url=base, **kw)

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
        # `get(memory_id)` — what a client with a default store actually calls — has to
        #   accept the id alone, through `_split_id_args`.
        #   `test_same_surface_as_sync` does not cover it: that test compares names
        #   through dir(). Matching names with different bodies still breaks callers.
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
        # There are two transports, and the suite has to walk both. The sync reader
        # calls `requests`' iter_content; AsyncClient is httpx and has aiter_bytes.
        # Sharing one implementation between them raises AttributeError on the first
        # image, and nothing else in the suite touches this path.
        srv, mem = self.make([(200, {}, "PNGBYTES")])
        async with mem:
            data, ctype = await mem.get_image("alice", "11111111-1111-1111-1111-111111111111")
        self.assertEqual(data, b"PNGBYTES")
        self.assertTrue(isinstance(ctype, str))

    async def test_get_image_retries_a_429_on_the_async_transport(self):
        # The async image retry lived in _request_bytes, and `asyncio` was imported
        # inside _request only — a function-local name that never reached this one.
        # The first 429 raised NameError while handling RateLimitError, so neither
        # `except RateLimitError` nor `except WosError` caught it.
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

        # close (sync) ↔ aclose (async) are the same capability under each
        # ecosystem's naming convention; everything else must match exactly.
        sync_api = {n for n in dir(Client) if not n.startswith("_")} - {"close"}
        async_api = {n for n in dir(AsyncClient) if not n.startswith("_")} - {"aclose"}
        self.assertEqual(sync_api, async_api)



class Base64ShapeTest(unittest.TestCase):
    """base64 produced from a file has to go through unchanged.

    `base64 image.jpg` and `openssl base64` wrap at 76 columns. Left in the payload,
    those line breaks are refused by the service (400). Producing an image's base64 from
    a file and pasting it in is a common route.
    """

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
    """`retries` and `maxRetries` are one option that had two names.

    The SDKs ship as one surface, so code ported between them names this setting either
    way, and a name that is not recognised is worse than an error: an options object can
    drop an unknown key at runtime, and **the retry setting vanishes silently.** Both
    names are accepted.
    """

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
        # AsyncClient rests on an optional dependency (httpx), so guard it the way the
        # other async tests do — absent means skip, not fail.
        self.assertEqual(AsyncClient(api_key="k", max_retries=3)._retries, 3)


class ListEngramsTest(unittest.TestCase):
    """`list_engrams()` asks the service what exists, so callers need not hard-code names
    from the docs and an engram added later is visible to them. The service is the
    authority."""

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
        # A caller's for loop must not blow up.
        srv, mem = self.make([(200, {}, json.dumps({"note": "no engrams on this model"}))])
        cat = mem.list_engrams()
        self.assertEqual(cat["engrams"], [])
        self.assertEqual(cat["forms"], [])
        self.assertIn("no engrams", cat["note"])


class EmptyBodyTest(unittest.TestCase):
    """A response with no body needs one answer, and this settles both directions: a body
    may be absent only when the status code says so (204/205/304), and an empty body on
    any other 2xx is a real fault (a proxy truncating bodies, say) that must not pass as
    success."""

    def make(self, script):
        srv, base = scripted_server(script)
        self.addCleanup(srv.shutdown)
        return Client("wos-test-xxxxxxxxxx", base_url=base)

    def test_204_is_success_not_an_error(self):
        mem = self.make([(204, {}, "")])
        self.assertEqual(mem.delete("alice", "m1"), {})

    def test_empty_200_is_an_error_not_a_silent_ok(self):
        # An add() that returns `{}` reads as a success with no id — the caller never
        # sees that the write went missing.
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
    """Idempotency keys already existed in the service, on every write route,
    This pins three things: that the header actually goes out, that a malformed key is
    refused before the network, and that the key is not swallowed by ``**metadata`` and
    stored on the memory."""

    def make(self, script):
        srv, base = scripted_server(script)
        self.addCleanup(srv.shutdown)
        return srv, Client("wos-test-xxxxxxxxxx", base_url=base)

    def test_key_rides_as_a_header(self):
        srv, mem = self.make([(200, {}, '{"id":"m1","status":"stored"}')])
        mem.add("x", "alice", idempotency_key="import:row-42")
        self.assertEqual(srv.seen[-1]["headers"].get("Idempotency-Key"), "import:row-42")

    def test_key_is_not_stored_as_metadata(self):
        # Declared as a keyword; otherwise this lands in **metadata and sticks to the
        # memory forever.
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
        self.assertEqual(got, ["k1", "k2", "k3", "k4", None])

    def test_a_trailing_newline_is_refused_here_not_by_http_client(self):
        # Python's `$` also matches just BEFORE a trailing newline, so `.match()` would
        # accept "k1\n" and the key would then die inside http.client as
        # `ValueError: Invalid header value` — the opaque failure this check exists
        # to prevent.
        srv, mem = self.make([(200, {}, "{}")])
        for bad in ("k1\n", "k1\r", "k1\r\n", "\nk1"):
            with self.assertRaises(ValueError) as cm:
                mem.add("x", "alice", idempotency_key=bad)
            self.assertIn("invalid idempotency_key", str(cm.exception))
        self.assertEqual(srv.seen, [])

    def test_malformed_key_fails_before_the_request(self):
        # A server-side 400 reads as "my write failed" to a caller that is retrying.
        srv, mem = self.make([(200, {}, "{}")])
        for bad in ("caf\u00e9 key", "a" * 129, "", "has space"):
            with self.assertRaises(ValueError):
                mem.add("x", "alice", idempotency_key=bad)
        self.assertEqual(srv.seen, [])


class StoreIdMustBeAStringTest(unittest.TestCase):
    """A PASSED store id that is not a usable string must not silently become the
    default store. That covers blank strings and the values a failed lookup actually
    produces: `user_id=0` (an integer primary key) is falsy and would otherwise fall
    through to the client default, writing a customer's memories into the wrong store."""

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
        # The documented shortcut has to keep working — the guard must not be so wide
        # that it breaks the zero-setup path.
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
    """`delete_store` and `delete_all` bypass `_uid()`, so each repeats the store-id
    warning itself: the calls that remove memories say which id the service files."""

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
    """The filters work on the API but appeared in neither the OpenAPI spec nor the SDKs."""

    def test_filters_reach_the_api(self):
        srv, base = scripted_server([(200, {}, '{"memories":[]}')])
        self.addCleanup(srv.shutdown)
        mem = Client("wos-test-xxxxxxxxxx", base_url=base)
        mem.search("q", "alice", filters={"categories": ["work"], "event_from": "2026-01-01"})
        body = json.loads(srv.seen[-1]["body"])
        self.assertEqual(body["filters"], {"categories": ["work"], "event_from": "2026-01-01"})


class StoreIdCollisionTest(unittest.TestCase):
    """An id whose normalized form (lowercased, anything outside [a-z0-9_] as '_')
    differs from it gets one warning naming that form."""

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
        # Warning on the normal form is noise, and noise buries the real warning.
        mem = self.make()
        logger = logging.getLogger("wontopos")
        with self.assertNoLogs(logger, level="WARNING") if hasattr(self, "assertNoLogs") else contextlib.nullcontext():
            mem.add("x", "alice_smith")


class FilterTypoTest(unittest.TestCase):
    """The API drops filter keys it does not know, silently — a typo widens the search
    and nobody hears about it."""

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
    """`metadata={...}` and loose keywords both land in the metadata object itself, never
    one layer deeper, so a speaker tag passed either way takes effect."""

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
    """What someone using an idempotency key most wants to know: did my retry write, or
    was this replayed?"""

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
    """No Python docstring mentioned the route to the 0.1x rate.

    It always worked — ``search(**opts)`` merges unknown keys into the body — so it
    went unfound, and callers paid the full rate for queries that did not need it.
    Now that the docstring promises it, a test has to hold the wire shape it promises.
    """

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
        """Documenting the options did not open the reserved fields.

        Uses the shape an app takes when it forwards untrusted input straight through
        (``**untrusted``). ``user_id`` and ``query`` are named parameters, so Python
        stops those with a TypeError first — but the wire name ``max_results`` differs
        from the parameter name (``limit``) and slips that net. Body-build order is
        the last line.
        """
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
    """The async docstring was one line — pin that the same options go out the same way."""

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
    """``__version__`` is hand-written, and it is what the User-Agent is built from.

    Left uncompared it drifts from pyproject, and the package goes to PyPI announcing
    an older number than it is. The existing UA test compares
    ``f"wontopos-python/{__version__}"`` against the same constant, so it can never
    catch this — the comparison has to be against the packaged version.
    """

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
    """If the warning record grows per end user, it leaks in the very case it describes.

    The warning fires on ids whose normalized form differs, and the pattern this SDK
    recommends is one store per end user. Unbounded, 50,000 users left 50,000 entries
    for the life of the process (global, so dropping the client changed nothing).
    It is capped now and evicts oldest-first, so a collision that first appears late
    still gets its one warning.
    """

    def setUp(self):
        wontopos._reset_warning_state()
        self.addCleanup(wontopos._reset_warning_state)
        self.mem = Client("wos-test-xxxxxxxxxx", base_url="http://127.0.0.1:9")

    @contextlib.contextmanager
    def assertNoWarning(self):
        """``assertNoLogs`` is 3.10+; this package supports 3.9."""
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
        # The oldest entry is evicted and warns again,
        with self.assertLogs("wontopos", level="WARNING"):
            self.mem._uid("user.0@example.com")
        # while one just seen stays quiet (otherwise every request repeats the warning).
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
    """A body that STOPS ARRIVING is not the same as a body that was cut off.

    A drop means nothing more is coming, so replaying an idempotent read costs one extra
    request; a timeout means the server may still be working, so a replay adds load to a
    request that could yet be answered. A read timeout is therefore final.

    Count REQUESTS, not ACCEPTS: keep-alive puts retries on one connection.
    """

    def stalling_server(self, stall=4.0):
        """A head promising 200 bytes and then nothing — the body read runs out of time."""
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
        time.sleep(0.3)  # the handler threads append before the client gives up
        self.assertEqual(len(seen), 1, "a stalled body was retried — count requests, not connections")


class BodyDropRetryTest(unittest.IsolatedAsyncioTestCase):
    """A drop AFTER the response head, while the body is being read.

    The class below tests a drop before any response arrives — the connect/header
    phase — and its server helper called that "mid-stream", so nothing in the suite
    ever reached the body reader. These use the TRUNCATE entry, which sends a head and
    then stops, and assert both clients do the same thing.
    """

    def test_sync_retries_an_idempotent_get_after_the_body_drops(self):
        accepted, base = dropping_server(["TRUNCATE", '{"collections": []}'])
        mem = Client("wos-test-xxxxxxxxxx", user_id="u", base_url=base, retries=2)
        self.assertEqual(mem.list_stores(), [])
        self.assertEqual(len(accepted), 2, "the dropped body was not retried")

    def test_sync_does_not_retry_a_write_after_the_body_drops(self):
        # The write may already have landed — replaying it stores twice.
        accepted, base = dropping_server(["TRUNCATE", '{"id": "m1"}'])
        mem = Client("wos-test-xxxxxxxxxx", user_id="u", base_url=base, retries=2)
        with self.assertRaises(APIConnectionError):
            mem.add("hello")
        self.assertEqual(len(accepted), 1, "a write was retried")

    @needs_httpx
    async def test_async_agrees_with_sync_on_both(self):
        accepted, base = dropping_server(["TRUNCATE", '{"collections": []}'])
        async with AsyncClient("wos-test-xxxxxxxxxx", user_id="u", base_url=base, retries=2) as mem:
            self.assertEqual(await mem.list_stores(), [])
        self.assertEqual(len(accepted), 2)

        accepted2, base2 = dropping_server(["TRUNCATE", '{"id": "m1"}'])
        async with AsyncClient("wos-test-xxxxxxxxxx", user_id="u", base_url=base2, retries=2) as mem:
            with self.assertRaises(APIConnectionError):
                await mem.add("hello")
        self.assertEqual(len(accepted2), 1)


class SyncAsyncDropParityTest(unittest.IsolatedAsyncioTestCase):
    """Both clients judge a dropped connection the same way.

    On a mid-stream drop a write is not retried (the first attempt may already have
    been applied), but an idempotent method is — replaying it applies nothing twice.
    Both are pointed at one server that drops.
    """

    def test_sync_retries_idempotent_on_mid_stream_drop(self):
        accepted, base = dropping_server([None, '{"collections": []}'])
        mem = Client("wos-test-xxxxxxxxxx", base_url=base, retries=2)
        mem.list_stores()  # GET — safe to call again after a dropped connection
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
        """Aligning idempotent retries must not open writes — the first attempt may already have landed."""
        accepted, base = dropping_server([None, '{"id": "m1"}'])
        mem = AsyncClient("wos-test-xxxxxxxxxx", base_url=base, retries=2)
        try:
            with self.assertRaises(APIConnectionError):
                await mem.add("x", "alice")
        finally:
            await mem.aclose()
        self.assertEqual(len(accepted), 1, "retrying a POST can store the same memory twice")


class ErrorStatusOutranksTheDeadBodyTest(unittest.IsolatedAsyncioTestCase):
    """A 404 whose message body dies mid-read is still a 404.

    Both clients read the error body to build the message; when that read fails, the
    status still decides the exception, so ``except NotFoundError`` holds and a 429
    whose body dies is still retried.
    """

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
    """`recall` keeps the contract its docstring states: out of range is refused, not
    clamped, and refused here, before a socket is opened.
    """

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
        # 0 is a real answer ("attach none"), not a missing value — it must pass.
        mem.recall("q", "alice", limit=20, context_limit=0)
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body["context_limit"], 0)
        self.assertEqual(body["limit"], 20)

    def test_status_zero_belongs_to_the_connection_error_alone(self):
        # `except WosError as e: if e.status == 0: retry()` is documented behaviour
        # for a request that never got a response. Anything else wearing status 0
        # sends that caller into a loop it cannot leave.
        import re
        src = open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "src", "wontopos", "__init__.py"), encoding="utf-8").read()
        self.assertEqual(re.findall(r"WosError\(\s*0\s*,", src), [],
                         "these construct a WosError with status 0")

    def test_replayed_never_overwrites_what_the_service_sent(self):
        # These bodies are widening — a response may carry fields this client has
        # never seen. If a name ever collides, the service's value is the
        # true one and ours is a guess.
        srv, mem = self.make([
            (200, {"Idempotent-Replayed": "true"}, '{"id": "m1", "replayed": false}'),
            (200, {"Idempotent-Replayed": "true"}, '{"id": "m2"}'),
        ])
        said = mem.add("x", user_id="alice", idempotency_key="k1")
        self.assertIs(said["replayed"], False, "the service said false; we must not flip it")
        silent = mem.add("x", user_id="alice", idempotency_key="k2")
        self.assertIs(silent["replayed"], True, "the service said nothing; the header answers")


class DeadlineTests(unittest.TestCase):
    """`timeout` bounds one attempt; `deadline` bounds the call.

    At the defaults a single call can hold a connection for 30s + backoff + 30s +
    backoff + 30s — over a minute. `deadline` lets a request handler say "I only have
    five seconds".
    """

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
        # The wait does not fit, so the 429 itself is the answer.
        with self.assertRaises(RateLimitError):
            mem.search("q", user_id="alice")
        spent = time.monotonic() - t0
        # Under the budget, not merely under some larger number: the bound here was
        # 1.5s, which a run that slept out the remainder and then gave up passed just
        # as well as one that refused at once.
        self.assertLess(spent, 0.15, f"spent {spent:.2f}s against a 0.2s budget")

    def test_a_clone_keeps_the_deadline(self):
        # The same trap the transport fell into: a clone that quietly drops it keeps
        # working right up to the call that needed the budget.
        srv, mem = self.make([(429, {"Retry-After": "5"}, "{}")], deadline=0.2)
        with self.assertRaises(RateLimitError):
            mem.with_model("tablet-2").search("q", user_id="alice")

    def test_a_non_positive_deadline_means_no_budget(self):
        for unset in (0, -1):
            self.assertIsNone(Client("wos-test-xxxxxxxxxx", deadline=unset)._deadline)


class PooledConnectionWatchdogTest(unittest.TestCase):
    """urllib3 puts a connection back in the pool inside the read that finishes the body,
    before the attempt stops its watchdog. Another thread can take it from there, and the
    first attempt's timer must not shut it under that thread's request. The first attempt
    keeps its body too: every byte had arrived in time."""

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
                    time.sleep(0.8)  # B's request is in flight when A's time runs out
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
            # Stands in for a thread switch right after the pool got the connection back.
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
            time.sleep(max(0.0, t0 + 0.45 - time.monotonic()))  # past A's 0.3s deadline
            go.set()
            a.join(5)
            b.join(5)

        # The scenario only means something if B reused A's connection.
        self.assertEqual([c for c, _ in seen], ["GET", "POST"])
        self.assertEqual(seen[0][1], seen[1][1])
        self.assertEqual(out.get("B"), {"id": "m1", "status": "stored"})
        self.assertEqual(out.get("A"), [])



class BuilderStoreIdGuardTest(unittest.TestCase):
    """The per-call guard lived only in _uid(), so the BUILDER form walked past it:
    Client(user_id=0) and with_user("") became the shared `default` store with no
    warning, while add(text, user_id=0) raised. with_user is the documented per-tenant
    pattern, so a failed session lookup wrote one end-user's memories into a store
    everyone on the account can read."""

    BAD = ("", " ", "\t", None, 0, 42, 3.5, [], {})
    # AsyncClient raises ImportError in its own __init__ without the async extra, which
    # would swallow the ValueError this class is asserting. Drop it from the list rather
    # than skipping the class — the sync guard is worth checking either way, and
    # `python3 -m unittest discover -s tests` is what the release runs on a bare sdist.
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
    """A field the service adds has to reach the caller.

    ``list_engrams`` rebuilt the reply into three keys, so anything else the service
    sent was dropped on the way out — no error, no log, nothing to notice. Responses
    widen; only the shapes this method promises are normalised.
    """

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
        # Only objects, and only a string note, are kept.
        mem = self.make(
            '{"engrams": [null, "x", {"name": "deep_recall"}], "forms": [1], "note": 42}'
        )
        r = mem.list_engrams()
        self.assertEqual(r["engrams"], [{"name": "deep_recall"}])
        self.assertEqual(r["forms"], [])
        self.assertIsNone(r["note"])


class MalformedErrorBodyTest(unittest.TestCase):
    """A 4xx has to arrive as a WosError however odd its body is.

    `message` comes out of somebody else's JSON and is not always a string.
    `{"message": null}` and `{"error": {"message": 42}}` both reached a `len()` that
    only strings answer, so the function whose job is to BUILD the error raised
    TypeError instead — outside every handler, so `except WosError` missed a plain 400.
    """

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
    """A key pasted from a rich-text doc often has its hyphen turned into an en dash. It
    is refused with a message that names api_key, instead of failing inside http.client
    as a UnicodeEncodeError that `except WosError` would miss."""

    def test_a_non_latin1_key_is_refused_with_a_useful_message(self):
        for bad in ("wos\u2013live\u2013" + "a" * 30, "wos-live-\u00e9" + "a" * 30, "wos-live-\uff0d" + "a" * 30):
            with self.assertRaises(ValueError) as cm:
                Client(bad)
            self.assertIn("api_key", str(cm.exception))
            self.assertIn("non-ASCII", str(cm.exception))

    def test_an_ordinary_key_still_works(self):
        self.assertTrue(Client("wos-live-" + "k" * 40))


class ListShapeTest(unittest.TestCase):
    """`x or []` only replaces a FALSY value — a truthy wrong type slips through as a
    dict and blows up in the CALLER's for-loop. _as_list exists for this; six sites
    still used the old form."""

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
            for _ in got:  # must be iterable as the annotation promises
                pass


# ----- retries, time budgets, errors and the transport -----

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
    """``assertNoLogs`` is 3.10+; this package supports 3.9."""
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
    """gzip of ``n_bytes`` zeros, built a block at a time so the test never holds the zeros."""
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
    """A 409 answers two different things. A write already in flight to the store
    (``retry_after_ms``) stored nothing, so any call retries it; a store id that collides
    with an existing one (``conflicts_with``) is permanent and never retried."""

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
    """When the wait before a retry does not fit the deadline, the caller gets the answer
    the service gave (429, 503, ...), not a status-0 connection error."""

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

    # A retry the deadline cuts short: the 429 before it is the answer, unless a write
    # was in flight, which may have been applied.
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
        # A connect that never sent the retry leaves the answer before it standing, even
        # for a write.
        call = wontopos._Call("POST", "/api/v1/memory/store", 2, 0.2)
        self.assertEqual(call.after_status(429, {"Retry-After": "0"}, RATE_429.encode()), 0.0)
        clock = call.clock(10)
        time.sleep(0.25)
        with self.assertRaises(RateLimitError):
            call.after_failure(wontopos._Failure("ConnectTimeout (timed out)", never_sent=True), clock)
        # With no answer yet, a spent deadline is status 0.
        bare = wontopos._Call("GET", "/x", 2, 0.01)
        time.sleep(0.05)
        with self.assertRaises(APIConnectionError):
            bare.clock(10)

    NEVER_SENT = wontopos._Failure("ConnectTimeout (timed out)", never_sent=True)

    def test_the_deadline_ends_a_last_attempt_that_was_never_sent(self):
        # Attempts left or not, the deadline ended the call: the answer before stands.
        for retries in (1, 2):
            with self.subTest(retries=retries):
                call = wontopos._Call("GET", "/api/v1/x", retries, 0.3)
                self.assertEqual(call.after_status(429, {"Retry-After": "0"}, RATE_429.encode()), 0.0)
                call.attempt = 1
                clock = call.clock(10)
                time.sleep(0.35)
                with self.assertRaises(RateLimitError):
                    call.after_failure(self.NEVER_SENT, clock)
        # With no answer before it, the error names the deadline.
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
        # A write that dropped may have been applied, so it is never retried.
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
    """An error status whose body arrives after the attempt's end is still that status:
    a 429 is retried and a 404 raises NotFoundError, in both clients."""

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

    # Not retried: over the 30s cap, or the last attempt. The body is the error, so it is
    # waited for.
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
    """Retry-After is delta-seconds or an HTTP-date. A wait over 30s is not slept through:
    the error is raised at once, carrying ``retry_after``."""

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
    """``timeout`` and ``deadline`` are wall-clock limits on an attempt, however slowly the
    headers or the body arrive."""

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
    """``type`` and ``details`` carry what the error body said beyond its message."""

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
                mem.add("x", "alice")
            self.assertEqual(cm.exception.message, f"HTTP {status}")
            self.assertEqual(str(cm.exception), f"[{status}] HTTP {status}")

    async def test_async_an_empty_error_body_names_the_status(self):
        _, mem = self.amake([(502, {}, "")])
        async with mem:
            with self.assertRaises(wontopos.ServerError) as cm:
                await mem.add("x", "alice")
        self.assertEqual(cm.exception.message, "HTTP 502")

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
    """Each transport has its own TLS context, with certifi's roots besides the OS store,
    so one cannot change what the other trusts and an empty OS store still verifies."""

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
    """A timeout or deadline that is not a positive number means the default; a huge one,
    infinity included, is clamped to what the platform can wait."""

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


@needs_httpx
class AsyncLifecycleTest(unittest.TestCase):
    """An AsyncClient belongs to the event loop it first ran on and ends at aclose(). Using
    it anywhere else is refused before anything is sent."""

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
    """A network error names the method and path and why, and nothing else: no URL, no
    query string, and no route back to the transport's exception (which holds the
    request headers, the API key among them)."""

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
    """A Client pickles into a working client, so it can be handed to a process pool that
    starts workers with spawn. The pickle carries the API key."""

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
        # The copy's pool is dropped after fork() and keeps the wall-clock limit.
        self.assertIn(back._session, list(wontopos._sessions))
        self.assertIs(back._session.get_adapter(base).poolmanager.pool_classes_by_scheme,
                      wontopos._WATCHED_POOLS)
        self.assertEqual(back.stats()["total_memories"], 3)
        self.assertEqual(srv.seen[0]["headers"]["X-API-Key"], KEY)
        self.assertEqual(srv.seen[0]["headers"]["X-WOS-Model"], "tablet-1")
        self.assertNotIn(KEY, repr(back))
        clone = pickle.loads(pickle.dumps(mem.with_user("bob")))
        self.assertEqual(clone.stats()["total_memories"], 3)
        self.assertIn("API key", Client.__doc__)
        self.assertNotIn("cannot be pickled", Client.__doc__)


class CopyTest(unittest.IsolatedAsyncioTestCase):
    """copy.copy and copy.deepcopy give a working client that shares the connections, the
    way an object graph holding a client gets copied (test fixtures, model copies, agent
    configs). Closing a copy leaves the original open."""

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
    """The plain-HTTP warning reads the URL the way the transport does, and a base_url with
    whitespace or a backslash is refused outright."""

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
                        "http://user:s3cret@[bad"):
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


class MetadataKeysTest(SyncMake, unittest.TestCase):
    """The service keeps speaker, event_date, category and conversation_id from metadata
    and drops the rest, so any other key warns once."""

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
    """What the docstrings say has to match what the service does, in current terms."""

    STALE = (
        r'add\(""\s*,\s*image', r"content may be empty", r"IS the memory", r"Scroll 1\.2\+",
        r"Tablet 2 and newer", r"Tablet 2\+", r"store with one and recall with another",
        r"max_bytes", r"\bused to\b", r"\b2\.2\.\d+", r"[Mm]easured",
    )

    def test_no_stale_claims(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        src = open(os.path.join(root, "src", "wontopos", "__init__.py"), encoding="utf-8").read()
        src = re.sub(r'(?m)^__version__ = .*$', "", src)
        found = [p for p in self.STALE if re.search(p, src)]
        self.assertEqual(found, [])

    def test_list_models_documents_capabilities(self):
        # Every capability note sends the reader to list_models(), so its shape has to say where.
        for fn in (Client.list_models, AsyncClient.list_models):
            self.assertIn("capabilities", fn.__doc__)


class PageSizeTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    """Page sizes are checked before sending, with the range in the message."""

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


class CursorRepeatTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    """A cursor that comes back after a page with rows means the walk would loop: that is
    a truncated answer and it raises. A repeat after an empty page is the end."""

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
    """Errors cross process boundaries (ProcessPoolExecutor, multiprocessing) intact."""

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
    """The async reader decodes at most one gzip or deflate layer and stops at the cap
    while decoding, so a small compressed body cannot inflate past it."""

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
    """A lone UUID is read as the memory id only when no memory_id was passed. A memory_id
    passed as '' is a bug at the call site and is refused."""

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
    """A DELETE retried after an answer that may have followed the delete (408/502/503/504,
    or a dropped connection) and then answered 404 says the first attempt may have done it."""

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
    """Raw-socket server that accepts each connection and never sends a byte, so a TLS
    handshake never finishes and no request is ever sent over it.
    Returns (connections_accepted, https_base_url)."""
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
    """A connect that hangs sent nothing, so it is retried like a refused one, even for a
    write. Once the request has gone out, the wall-clock limit ends the call."""

    async def test_a_hung_handshake_is_retried_even_for_a_write(self):
        conns, base = silent_server(self)
        async with AsyncClient(KEY, base_url=base, timeout=0.5, retries=1) as mem:
            with self.assertRaises(APIConnectionError) as cm:
                await mem.add("x", "alice")
        self.assertEqual(len(conns), 2)
        self.assertIn("network error: POST /api/v1/memory/store", str(cm.exception))

    async def test_a_hang_cut_by_the_deadline_reports_the_deadline(self):
        # On the last attempt too, and the same as the sync client.
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
    """A call still waiting for a pooled connection has sent nothing. Whichever timer ends
    the wait, httpx's or the attempt's own, it is the same never-sent failure, retried
    like a refused connect."""

    def test_an_httpx_pool_timeout_was_never_sent(self):
        import httpx
        f = AsyncClient(KEY)._failure(httpx.PoolTimeout("timed out"))
        self.assertEqual((f.never_sent, f.timed_out, f.reason), (True, False, "PoolTimeout (timed out)"))

    async def wait_for_the_pool(self, mem):
        import asyncio
        holder = asyncio.create_task(mem.with_timeout(5).list_stores())
        await asyncio.sleep(0.3)  # the holder takes the only connection
        try:
            for _ in range(4):
                with self.assertRaises(APIConnectionError) as cm:
                    await mem.with_timeout(0.3).with_retries(0).add("x", "alice")
                self.assertIn("network error: POST /api/v1/memory/store: PoolTimeout (timed out)",
                              str(cm.exception))
            t0 = time.monotonic()
            with self.assertRaises(APIConnectionError):
                await mem.with_timeout(0.3).with_retries(1).add("x", "alice")
            # Two waits and the backoff between them.
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
    """An AsyncClient made before fork() works in the child on connections of its own and
    never touches the ones it inherited. The pid is changed by hand to stand in for the
    child."""

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
        self.assertIn("works in the child", " ".join(AsyncClient.__doc__.split()))

    def test_a_used_client_rebuilds_and_leaves_the_old_one_alone(self):
        import asyncio
        from unittest import mock
        srv, base = self.serve(2)
        mem = AsyncClient(KEY, base_url=base)
        asyncio.run(mem.stats("alice"))  # binds a loop and pools a connection
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
    """Under another async library (trio, anyio on trio) there is no asyncio loop: the
    client skips the loop binding and waits through anyio instead of asyncio."""

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


class UndecodableBodyTest(SyncMake, AsyncMake, unittest.IsolatedAsyncioTestCase):
    """A 2xx body that cannot be decoded is a transport failure, APIConnectionError with
    status 0, in both clients: a read is retried and a write is not."""

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
        self.assertEqual(len(srv.seen), 1)

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
                self.assertEqual(len(srv.seen), 1)

    async def test_async_error_status_still_stands(self):
        _, mem = self.amake([(404, {"Content-Encoding": "gzip"}, b"not gzip")])
        async with mem:
            with self.assertRaises(NotFoundError):
                await mem.stats("alice")


# Must stay LAST. It sat at line 947 of a 1,624-line file, so `python tests/test_client.py`
# ran 76 of 129 tests and exited OK — the 53 defined below it never executed. publish.sh
# uses `unittest discover`, so the release gate was never fooled; a developer running the
# file was.
if __name__ == "__main__":
    unittest.main()
