# Wontopos — long-term memory for AI agents

```bash
pip install wontopos
```

Get an API key in the [console](https://wontopos.com). Keys look like `wos-live-...`;
the client also reads `WONTOPOS_API_KEY` from the environment.

```python
from wontopos import Client

mem = Client(api_key="wos-live-...")

# Each end-user / agent / topic gets its own store — create it once.
# (A "default" store already exists, so you can skip this and omit the id.)
mem.create_store("alice")
mem.add("she prefers tea over coffee", user_id="alice")

# one call → short-term + long-term + context, ready for your LLM prompt
ctx = mem.recall("what does alice drink?", user_id="alice")
```

## Why

- **The same in every language** — identical recall whichever language a memory was written in (Korean · Japanese · Chinese · English).
- **No LLM in the loop** — `store` / `search` / `recall` never call a language model. You pay retrieval, not generation.
- **Bounded retrieval** — `recall()` returns a small, fixed-size slice regardless of how much you've stored (~1,000 tokens on `tablet-2`, the default engine). Your LLM bill stops growing with history.

## Methods

| Method | Purpose |
|---|---|
| `add(content, user_id, **metadata)` | Store one memory |
| `add_turn(user_msg, assistant_msg, user_id?)` | Store a conversation exchange |
| `add_bulk(content, user_id, category=, timestamp=)` | Backfill a long history |
| `update(old_memory_id, new_content, user_id?)` | Supersede an old fact |
| `search(query, user_id, limit=10, **opts)` | Search stored memories |
| `search_full(query, user_id, limit=10, **opts)` | The same search with every field kept — `images` and `verify_used` included |
| `recall(query, user_id)` | One-call context (short + long + surrounding) |
| `history(user_id)` | Recent turns (short-term) |
| `stats(user_id)` | Counts |
| `get(user_id, memory_id)` | Fetch one memory by id (the text you stored, and its metadata) |
| `list_memories(user_id, limit=100, cursor=)` | Browse/export a store's raw memories, paged |
| `delete(user_id, memory_id)` | Delete one memory |
| `delete_all(user_id)` | GDPR erase (delete every memory for the user) |
| `add_speaker(speaker, user_id?)` | Register a person (explicit, up to 50 to start) |
| `list_speakers(user_id?)` | Registered people + per-person memory counts |
| `remove_speaker(speaker, user_id?)` | Unregister; memories stay, the tag goes |

All methods take a `user_id` — it names the **store**: one isolated memory space per end-user, agent, or topic, then per account (your API key). WHO said each memory inside a store is the `speaker` tag below — storing the assistant's own words never needs a separate id.

## Who said it (speakers)

Every memory can carry a speaker: `"me"` for the assistant's own words, or a
person's name. Speakers are explicit, like stores: register a person once,
then store under their name — a typo can never silently become a new person.
Search accepts a speaker too, so you can recall one person's words only.

```python
mem.add_speaker("Bob", user_id="alice")      # once per person
mem.add("I promised to send the report on Friday", user_id="alice", speaker="me")
mem.add("Bob said the deadline moved to Tuesday", user_id="alice", speaker="Bob")
mem.search("what did Bob say about deadlines?", user_id="alice", speaker="Bob")
```

Results arrive best-first — take the list in the order given. ``similarity`` on each
memory is a raw closeness score, not the ranking key: what produces the order is
internal and is not returned, so sorting by it makes results worse. There is no
``score`` field.

A store registers up to 50 people to start (a limit we plan to raise);
`"me"` never needs registration and never counts against it.

## Async

Same surface, awaitable — needs the extra:

```bash
pip install "wontopos[async]"
```

```python
from wontopos import AsyncClient

async with AsyncClient(api_key="wos-live-...", user_id="alice") as mem:
    await mem.add("she prefers tea over coffee")
    hits = await mem.search("what does alice drink?")
```

Every `Client` method exists on `AsyncClient` with identical arguments and
semantics (retries, redirect refusal, guards). Close with `async with` or
`await mem.aclose()`.

## Recall caching

Opt in per search and repeated or extended queries reuse the previous result
at 10% of the normal rate (Tablet and Scroll models).

It is not free to turn on: the FIRST call writes the cache and bills the query
tokens at 2x for a `5m` TTL, 3x for `1h`. Only hits inside the TTL bill at 0.1x.
So it pays for a query you repeat or extend, and costs more for one you issue
once — do not switch it on globally. Any write to the store invalidates its cache
at once, so a hit can never predate a new memory.

```python
hits = mem.search("...the conversation so far...", user_id="alice",
                  cache_control={"ttl": "5m"})   # or "1h"
```

## Reliability

Built in, no configuration needed:

- **Automatic retries** — 429 always, and 502 / 503 or a connection error only when a
  retry cannot double-process a write. The writes and the searches are POSTs, and a
  502 on one of those may have been returned *after* the service already stored and
  billed it, so those get 429 and connect-level failures only. The reads and the
  deletes that address a whole store — `list_stores`, `list_speakers`, `delete_store`,
  `remove_speaker`, `forget_image` — are GET or DELETE and do retry a 502. Twice, with
  exponential backoff + jitter, honoring the server's `Retry-After`. Tune with
  `Client(retries=...)`; `retries=0` disables.
- **Redirects refused** — the API key never follows a 3xx to another host.
- **Timeouts** — 30s per attempt by default (`Client(timeout=...)`), and a total
  budget for the whole call across every retry with `Client(deadline=...)` /
  `with_deadline(secs)`. At the defaults one call can hold for 30s + backoff + 30s
  + backoff + 30s, which a request handler with five seconds cannot use.
- **Key never in logs** — `repr(client)` masks the API key.
- **Wipe guard** — `delete()` without a `memory_id` raises instead of silently
  meaning "delete everything"; wiping a store is only ever the explicit
  `delete_all(user_id)` / `delete_store(user_id)`.

## Security

Built in, none of it configurable off:

- **TLS 1.2 floor** and certificate verification that cannot be disabled.
- **Redirects refused** — a 3xx is an error, so the key never follows one to
  another host.
- **Response size cap** — anything over 64MB is refused instead of buffered.
- **Key hygiene** — keys are trimmed (a stray newline from a file otherwise
  becomes a mystery 401) and inner whitespace is rejected; model names are
  validated before they reach a header.
- **`Client.from_env()`** reads `WONTOPOS_API_KEY` (or `WOS_API_KEY`) — keep
  keys out of source code.
- Plain-HTTP base URLs on non-local hosts warn. One dependency (`requests`,
  floor `>=2.32` for its certificate-verification fix).

## Errors

Any non-2xx response raises `WosError(status, message)`. When the server sent a
request id it's on `e.request_id` — include it when contacting support.

```python
from wontopos import Client, WosError

try:
    mem.search("...", user_id="alice")
except WosError as e:
    if e.status == 401:
        print("API key invalid or revoked")
    elif e.status == 429:
        print("Rate limited — back off")   # already retried twice by then
    else:
        print(e.status, e.message, e.request_id)
```

## A different API host

Point the client somewhere other than the default endpoint - a dedicated region,
a proxy of your own, or a local test server:

```python
mem = Client(api_key="...", base_url="https://api.example.com")
```

## Links

- Homepage: <https://wontopos.com>
- API reference: <https://wontopos.com/en/why> (Developers tab)

## Reporting a bug

Found something wrong, or something that looks unsafe? Tell us — every report gets read.

- Bugs: <https://wontopos.com/contact?topic=bug>
- Security: <https://wontopos.com/contact?topic=security> (also published at
  [`/.well-known/security.txt`](https://wontopos.com/.well-known/security.txt))

Include the SDK version (`wontopos.__version__`) and the language. If it involves a store id or a
memory, describe the shape rather than pasting the contents — we do not need your
data to fix it.

## Changelog

The three clients release in lockstep — same version, same surface, same day. Patch
releases are additive: nothing is removed or reordered within a minor line, and a
release that bends that says so at the top of its own entry.

See [CHANGELOG.md](https://github.com/wontopos/wontopos-python/blob/main/CHANGELOG.md).
