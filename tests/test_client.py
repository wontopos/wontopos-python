# Offline tests: a scripted localhost HTTP responder, no network, no pytest dep.
# Run from sdk/python:  python3 -m unittest discover -s tests -v
import contextlib
import logging
import json
import os
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
from wontopos import APIConnectionError, AsyncClient, Client, WosError, __version__

try:
    import httpx  # noqa: F401
    HAVE_HTTPX = True
except ImportError:
    HAVE_HTTPX = False


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
        data = payload.encode()
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
    """Raw-socket server: for each script entry, accept ONE connection; a None
    entry reads the request head then closes WITHOUT responding (a mid-stream
    drop); a string entry is served as a normal 200 response. Returns
    (accept_counter_list, base_url)."""
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
                if entry is not None:
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
        # The retry loop added in 2.2.35 read `self._max_retries`, which this class
        # never sets — the attribute is `_retries`. Every sync get_image() raised
        # AttributeError before sending anything, and 109 tests passed over it
        # because the only get_image cases used blank ids that fail the id guard
        # first, several statements earlier. This one reaches the request.
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
        # These three name ONE memory, like delete and get, but reached the network
        # with an empty id and let the server say so — and a non-string died inside
        # the header check with "'int' object has no attribute 'search'".
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
        # `recall` has always been 5..20 and the service refuses anything else.
        # Search had no contract: the three clients sent whatever they were given,
        # the MCP server allowed 1..60, and the service capped at 50 with no floor.
        # Both ends, and every search method, because a guard in one of four call
        # sites is the shape of bug this SDK has shipped before.
        # ValueError, not WosError: nothing was sent. `WosError` with status 0 means
        # APIConnectionError — "the request never got a response" — and a caller
        # branching on that would retry a typo forever. Every other argument check in
        # this SDK already raised ValueError; this one was the outlier.
        _, mem = self.make([])
        for bad in (0, 1, 4, 21, 50, 100):
            for call in (mem.search, mem.search_full, mem.search_self):
                with self.assertRaises(ValueError) as cm:
                    call("q", "alice", bad)
                self.assertIn("between 5 and 20", str(cm.exception))
                self.assertNotIsInstance(cm.exception, WosError)

    def test_search_full_keeps_what_search_merges_away(self):
        # search() answers with one merged list, so the photos and the count of
        # re-ask passes actually run had nowhere to land. Both are billable, and
        # both were being discarded.
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
        # the merged call is unchanged: one list, no images, no verify_used
        self.assertEqual([m["id"] for m in mem.search("q", user_id="alice")], ["m1", "s1"])

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
        # verfy=3 used to be absorbed into opts and sent as-is. The API ignores a field
        #   it does not know and answers 200, so re-ask never ran while the caller
        #   believed it had — a paid feature off, and nothing to see. Filters only warn
        #   because a wrong filter still returns memories; this one returns a normal
        #   answer that is quietly worse.
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
        # Re-ask is billed by what it delivers. Slip a default in here and the bill
        # rises quietly for a customer who changed nothing — so what was not passed is
        # not sent.
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
        # A 502 on a POST write must NOT retry: the write may already have landed
        # (double store / double bill). Only one request should be made.
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
        # A server that returns the same next_cursor forever must not loop forever.
        srv, mem = self.make([
            (200, {}, '{"memories": [{"id": "1"}], "next_cursor": "C"}'),
            (200, {}, '{"memories": [{"id": "2"}], "next_cursor": "C"}'),
        ])
        out = list(mem.iter_memories(user_id="u", page_size=1))
        self.assertEqual(len(srv.seen), 2)  # stopped after the cursor repeated
        self.assertEqual([m["id"] for m in out], ["1", "2"])

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

    # ----- 2.2.14: connection-error retry gating (no double-fired writes) -----

    def test_post_not_retried_on_midstream_drop(self):
        # The server accepts, reads the request, then drops the connection with
        # no response. For a POST the write may already have landed (and billed)
        # server-side — the client must NOT fire it again.
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

    def test_timeout_nan_or_zero_falls_back_to_default(self):
        # A NaN/0 timeout must not make every request fail instantly.
        srv, mem = self.make([(200, {}, '{"memories": []}')], timeout=float("nan"))
        self.assertEqual(mem.search("q"), [])
        srv2, mem2 = self.make([(200, {}, "{}")], timeout=0)
        mem2.stats()
        self.assertEqual(len(srv2.seen), 1)

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

    # ----- 2.2.17: fuzz-found robustness -----

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



    # ----- 2.2.17: clones share the connection pool -----

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

    async def test_get_accepts_the_id_alone_like_sync(self):
        # sync had the id recovery (`_split_id_args`) and async did not. `get(memory_id)`
        #   — what a client with a default store actually calls — worked on sync and died
        #   with ValueError on async.
        #   And `test_same_surface_as_sync` passed it, because that test compares names
        #   through dir(). Matching names with different bodies still breaks callers.
        srv, mem = self.make([(200, {}, '{"memory": {"id": "m1"}}')])
        async with mem:
            r = await mem.get("11111111-1111-1111-1111-111111111111")
        self.assertEqual(r["id"], "m1")
        body = json.loads(srv.seen[0]["body"])
        self.assertEqual(body["memory_id"], "11111111-1111-1111-1111-111111111111")

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
    those line breaks are refused by the engine — measured against production
    (2026-08-16): one line 200, wrapped at 76 **400**. Producing an image's base64 from
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

if __name__ == "__main__":
    unittest.main()


class RetriesAliasTest(unittest.TestCase):
    """`retries` (Python) and `maxRetries` (TypeScript) are one option that had two names.

    The docs say the three SDKs ship together as "one surface", so porting TypeScript to
    Python raised a TypeError right here. The other direction (Python -> TS) was worse:
    TypeScript ignores unknown keys on an options object at runtime, so **the retry
    setting vanished silently.** Both names are accepted.
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
        # other async tests do — absent means skip, not fail. This guard was missed once.
        self.assertEqual(AsyncClient(api_key="k", max_retries=3)._retries, 3)


class ListEngramsTest(unittest.TestCase):
    """`engram()` could run one but never ask what exists. So callers copied names out of
    the docs and hardcoded them, and every engram added afterwards was invisible to them.
    The service has to be the authority. MCP dropped its enum for the same reason (1.0.6)."""

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
    """The three SDKs treated a body-less response differently: on the same `204 No
    Content` TypeScript succeeded with `{}` while Python and Rust raised "invalid JSON".
    They ship as one surface, so both directions are settled here — a body may be absent
    only when the status code says so (204/205/304), and an empty body on any other 2xx
    is a real fault (a proxy truncating bodies, say) that must not pass as success."""

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
    """Idempotency keys already existed in the backend (across the whole memory plane),
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
        # Python's `$` also matches just BEFORE a trailing newline, so `.match()`
        # accepted "k1\n" and the key then died inside http.client as
        # `ValueError: Invalid header value` — the opaque failure this check exists
        # to prevent. TypeScript and Rust refused it all along.
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
    default store. Blank strings were already refused; the values a failed lookup
    actually produces were not — `user_id=0` (an integer primary key) is falsy, so it
    fell through to the client default, and any other int died in the warning helper
    with "'int' object has no attribute 'lower'". One wrote a customer's memories into
    the wrong store; neither said what was wrong."""

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
    """`delete_store` drops a whole store, and it bypasses `_uid()`, so the collision
    warning had to be repeated there. `delete_all` repeated it; `delete_store` did not,
    which left the ONE call that removes a store silent about `Alice.Smith` and
    `alice_smith` being the same one. TypeScript and Rust warned on both."""

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
    """The API lowercases a store id and rewrites anything outside [a-z0-9_] to '_', so
    Alice.Smith · alice-smith · alice_smith are **one store**. Measured against
    production: what was stored under alice-smith came back from a search on Alice.Smith.
    The most common use of this SDK is one store per end user, so bob.lee@x and
    bob-lee@x share every memory between them and the response says nothing."""

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
        self.assertIn("share ONE store", joined)

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
    """TypeScript and Rust take metadata positionally. Python alone takes **metadata, so
    the metadata={...} that ported code naturally writes arrived wrapped one layer deeper
    — a speaker tag never took effect, and nothing raised."""

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
    went unfound while repeated queries billed at full price. Now that the docstring
    promises it, a test has to hold the wire shape it promises.
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

    The warning fires on ids that fold — email-shaped ones — and the pattern this SDK
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


@unittest.skipUnless(HAVE_HTTPX, "httpx not installed")
class SyncAsyncDropParityTest(unittest.IsolatedAsyncioTestCase):
    """Do the two clients judge the same failure the same way?

    On a mid-stream drop a write must not be retried (it may already be stored and
    billed), but an idempotent method can be — replaying it double-processes nothing.
    Sync always did that; async caught connect-level failures only, so the same
    dropped GET was retried by one and raised by the other. Same illness as the
    empty-body split in 2.2.24, so: point both at one server that lies.
    """

    def test_sync_retries_idempotent_on_mid_stream_drop(self):
        accepted, base = dropping_server([None, '{"collections": []}'])
        mem = Client("wos-test-xxxxxxxxxx", base_url=base, retries=2)
        mem.list_stores()  # GET — safe to call again after a dropped connection
        self.assertEqual(len(accepted), 2, "sync did not retry an idempotent GET")

    async def test_async_retries_idempotent_on_mid_stream_drop(self):
        accepted, base = dropping_server([None, '{"collections": []}'])
        mem = AsyncClient("wos-test-xxxxxxxxxx", base_url=base, retries=2)
        try:
            await mem.list_stores()
        finally:
            await mem.aclose()
        self.assertEqual(len(accepted), 2, "if async does not retry the same GET, the two clients diverge")

    async def test_async_does_not_retry_a_write_on_mid_stream_drop(self):
        """Aligning idempotent retries must not open writes — it may already be stored and billed."""
        accepted, base = dropping_server([None, '{"id": "m1"}'])
        mem = AsyncClient("wos-test-xxxxxxxxxx", base_url=base, retries=2)
        try:
            with self.assertRaises(APIConnectionError):
                await mem.add("x", "alice")
        finally:
            await mem.aclose()
        self.assertEqual(len(accepted), 1, "retrying a POST can store the same memory twice")


class RecallContractTests(unittest.TestCase):
    """`recall` promised a contract in its docstring and did not keep it.

    "out of range is refused, not clamped" was written while the value went straight
    to the wire, so `limit=500` travelled to the engine and died there — a service
    error for a mistake the client knew about before opening a socket. The promise
    and the missing code were written months apart, which is why no review of a
    single diff could have seen it.
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
    """`timeout` bounded one attempt, so nothing bounded the call.

    At the defaults a single call can hold a connection for 30s + backoff + 30s +
    backoff + 30s — over a minute — and a request handler awaiting it had no way to
    say "I only have five seconds". The TypeScript and Rust SDKs got the same option
    in the same release; a budget that exists in one language is a budget nobody can
    rely on.
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
        with self.assertRaises(APIConnectionError) as cm:
            mem.search("q", user_id="alice")
        self.assertIn("deadline of 0.2s exhausted", str(cm.exception))
        spent = time.monotonic() - t0
        # Under the budget, not merely under some larger number: the bound here was
        # 1.5s, which a run that slept out the remainder and then gave up passed just
        # as well as one that refused at once.
        self.assertLess(spent, 0.15, f"spent {spent:.2f}s against a 0.2s budget")

    def test_a_clone_keeps_the_deadline(self):
        # The same trap the transport fell into: a clone that quietly drops it keeps
        # working right up to the call that needed the budget.
        srv, mem = self.make([(429, {"Retry-After": "5"}, "{}")], deadline=0.2)
        with self.assertRaises(APIConnectionError):
            mem.with_model("tablet-2").search("q", user_id="alice")

    def test_a_non_positive_deadline_means_no_budget(self):
        # A zero budget would fail every call before it started — that reads as "no
        # budget", the same fallback a non-positive timeout takes.
        srv, mem = self.make([(200, {}, '{"memories": []}')], deadline=0)
        self.assertEqual(mem.search("q", user_id="alice"), [])



class BuilderStoreIdGuardTest(unittest.TestCase):
    """The per-call guard lived only in _uid(), so the BUILDER form walked past it:
    Client(user_id=0) and with_user("") became the shared `default` store with no
    warning, while add(text, user_id=0) raised. with_user is the documented per-tenant
    pattern, so a failed session lookup wrote one end-user's memories into a store
    everyone on the account can read."""

    BAD = ("", " ", "\t", None, 0, 42, 3.5, [], {})

    def test_with_user_refuses_every_unusable_id(self):
        for cls in (Client, AsyncClient):
            bound = cls("wos-test-xxxxxxxxxx", user_id="tenant-A")
            for bad in self.BAD:
                with self.assertRaises(ValueError, msg=f"{cls.__name__}.with_user({bad!r})") as cm:
                    bound.with_user(bad)
                self.assertIn("non-blank string", str(cm.exception))

    def test_constructor_refuses_every_unusable_id(self):
        for cls in (Client, AsyncClient):
            for bad in self.BAD:
                with self.assertRaises(ValueError, msg=f"{cls.__name__}(user_id={bad!r})") as cm:
                    cls("wos-test-xxxxxxxxxx", user_id=bad)
                self.assertIn("non-blank string", str(cm.exception))

    def test_the_zero_setup_path_and_other_clones_still_work(self):
        for cls in (Client, AsyncClient):
            self.assertEqual(cls("wos-test-xxxxxxxxxx")._user_id, "default")
            bound = cls("wos-test-xxxxxxxxxx", user_id="tenant-A")
            self.assertEqual(bound.with_model("tablet-2")._user_id, "tenant-A")
            self.assertEqual(bound.with_user("tenant-B")._user_id, "tenant-B")


class KeyCharsetTest(unittest.TestCase):
    """A key pasted from a rich-text doc has had its hyphen turned into an en dash. That
    used to travel into http.client and die there as UnicodeEncodeError — not a WosError,
    so `except WosError` missed it, and the message never said "api_key"."""

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
