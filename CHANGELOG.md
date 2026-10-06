# Wontopos SDK changelog

One entry per release, covering all three SDKs (Python · TypeScript · Rust).

**Versioning policy:** the three SDKs always release in lockstep — same version,
same surface, same day. Patch releases are additive (new options, hardening,
docs); nothing is removed or reordered within a minor line.

## 2.2.45 — 2026-10-06

**The three clients refuse the same arguments in the same words.**

- `revisions` (`revisions_page` in Rust) refuses an `include` other than `"revised"` or
  `"unrevised"` before sending, in all three. The service answered such a value with a
  400 that named no field. In Python this is now a `ValueError` and in TypeScript an
  `Error`, where it was a `BadRequestError`; Rust still returns `ErrorKind::BadRequest`.
  A `null` include is left out in TypeScript, as Python leaves out `None`.
- A count out of range is refused with one sentence in all three: `<name> must be an
  integer between <min> and <max>, got <value>.` Python's search and recall counts and
  Rust's counts said "must be between", and Rust's `max_images` "a whole number".
- Python leaves out a keyword of `None` instead of sending `null`; a `null` speaker or
  `cache_control` was a 400. Such a keyword does not override the same key in
  `metadata=`, which is sent as given, as before and as in TypeScript and Rust.
  TypeScript leaves out a search option of `null` the same way. `add_bulk(category=None)` sends `general`,
  and so does TypeScript's `addBulk` for a `null` category. Counts accept an integer of any type, numpy's included; a `bool` is
  still refused.
- TypeScript warns once about a write option a call does not take, such as
  `idempotency_key` (the option is `idempotencyKey`), or `image` anywhere but `add`. It was dropped without a word, so a
  re-run could store a memory twice.
- Rust's `with_model("")` sends no model header, so the service's default model answers,
  as in Python and TypeScript; it was refused. A name of only whitespace is still refused. `list_speakers` always carries a `speakers` list in Python and Rust, and Rust's
  `list_engrams` an `engrams` and a `forms` list, as TypeScript's do.
- Docs: the READMEs say `from_env()` (`fromEnv()` in TypeScript) reads `WONTOPOS_API_KEY`;
  a client does not read it on its own. The Python async example runs as a script, the
  TypeScript proxy example imports `Client`, five TypeScript methods take the store
  positionally (`getImage` among them), and search options pass through only under
  `extra`. Rust's crate doc no longer says every timeout is unretried: one at connect
  time is retried.

## 2.2.44 — 2026-10-05

**A retired model has its own error.** A model past its retirement answers `410`. Python
and TypeScript raise it as `GoneError`, a `WosError` subclass, and Rust's
`WosError::is_gone()` is true for it; before, it arrived as a plain `WosError` (in Rust,
`ErrorKind::Other`, which it still is). The message says what to do: `list_models()`
(`listModels()` in TypeScript) lists the models you can use. It is not retried, as before,
and `delete_store` still works under a retired model.

## 2.2.43 — 2026-10-04

**Types and docs that match what the API sends.** No request or result changes.

- TypeScript's `HistoryTurn` has `role` and `content`, which is what `history()` returns.
  `user_msg` and `assistant_msg` were never sent; they stay, marked deprecated.
- TypeScript's `ModelInfo` has `retires_at`, and the model listing docs in all three name
  it: it is present on a live model that is scheduled to retire, and from that instant
  calls naming the model are refused.
- The `by_speaker` docs name `records_to_delete`; `points_to_delete` is the same number
  under its old name. TypeScript's `SpeakerPage` has both.
- `add` / `store`: when something was saved, `status` starts with `"stored"` and can carry
  more text, so match on the prefix; it is `"duplicate"` when nothing was saved. A
  duplicate can arrive with an empty `id` and no `duplicate_of`; then search with the same
  text to find the memory it matched. TypeScript's `StoreResult` and the Python and Rust
  `add` docs say so.
- `usage` lists `stores` highest spend first, not busiest first.
- A `list_memories` cursor works with the model that returned it.
- Rust's `Memory` doc names `speaker` as a typed field.

## 2.2.42 — 2026-09-30

**A read sent as a POST reports the answer before a retry the deadline cuts short**, as
a GET or DELETE does. That is every call on the search, recall, engram, get, list,
images, by-speaker, stats, history, lineage and revisions routes, `search_full`,
`search_self` and Rust's `_with` and `_page` forms included. They raised the status-0
deadline error there; `get_image` already did this.

**Docs corrected: models on the shared pool read the same memory**, so you can store with
one and recall with another. The 2.2.41 advice to store and search through one model is
withdrawn.

## 2.2.41 — 2026-09-30

**A 409 for a write already in flight is retried**, on every method, after the wait the
service asks for. A 409 for a store id that collides with an existing one is not
retried; the error names that store (`conflictsWith` / `conflicts_with`, TypeScript and
Python).

**When a retry would not fit the deadline, the call raises that response's error**
(`RateLimitError`, `ServerError`, `ConflictError`) instead of a status-0 deadline error.
A `Retry-After` over 30 seconds is not waited out: the call raises at once, and
`RateLimitError` carries `retryAfter` / `retry_after` (TypeScript and Python). A
`Retry-After` that is neither seconds nor an HTTP date falls back to the normal backoff.
A retry is not held up by an error body that stops arriving. When the deadline cuts an
attempt short, a GET or DELETE that was retrying, and `get_image` / `getImage`, raise
the error of the answer before it; any other call raises the status-0 deadline error.

**Errors say more.** TypeScript and Python errors carry the service's error `type` and
the rest of its error object as `details`. 413 and 422 are `BadRequestError`
(`ErrorKind::BadRequest` in Rust). Control characters are removed from server error
text, and a message says so when the error body could not be read. Network errors no
longer carry the URL's query string or credentials, and a client's printed form
(`repr`, `Debug`, `JSON.stringify`, Node's inspect) masks credentials in its base URL.
Rust's `WosError` implements `source()` and its message names the cause. Python's
`APIConnectionError` no longer chains the transport exception.

**Refused before sending, where 2.2.40 sent them:** page sizes outside the service's
range (`list_images`, `export_images`, `iter_images`, `by_speaker` and `revisions`
(`revisions_page` in Rust) take 5 to 20; `list_memories` takes 1 to 500; `None`/`null`
still means the default);
`max_images` outside 0 to 5; a base URL whose scheme is not http or https, or with
whitespace inside it or a backslash (surrounding whitespace is trimmed); in TypeScript,
`userId: undefined` in the constructor; in Python, an explicitly empty memory id. Rust
reports an invalid base URL as `BadRequest` and does not retry it.

**Timeouts:** a value that is not a positive number still means the default; a value
past what a timer can hold, infinity included, is clamped. In Python `timeout` and
`deadline` are wall-clock and bound an attempt whose headers or body trickle in; a
connect that hangs is still retried.

**`add` warns once per metadata key the service does not keep** (it keeps `speaker`,
`event_date`, `category`, `conversation_id`).

**Python `AsyncClient`:** made before `fork()`, it builds its own connections in the
child. After `aclose()`, or on a second asyncio event loop, it refuses calls before
sending. It decodes at most one gzip layer within the 64MB cap, and a corrupt gzip body
is `APIConnectionError` as on `Client`. TLS trusts certifi's roots as well as the
operating system's, on both clients.

**Python pickling:** `Client` pickles, and the pickle carries its API key.
`AsyncClient` does not pickle. Every error class survives pickling. A non-finite number
in a body raises `ValueError` on both clients, and deeply nested JSON raises `WosError`.

**TypeScript:** a POST whose connection failed after it was sent is not re-sent. A
custom `fetch` may send the request to another URL; an answer it reports as redirected
is refused. A page-relative `baseUrl` resolves against `location` where there is one.
The published types no longer need the DOM library; the two test hooks are deprecated.

**Rust:** `search_full_with` combines filters, `speaker` and any extra field with the
full answer. An attempt the deadline cuts short is no longer a `Network` error. A read
is no longer sent again because its error body dropped mid-read. Backoff jitter is
spread on every platform.

**A repeated cursor after a non-empty page raises** in every iterator and export,
instead of ending the walk with a partial result. **A retried `DELETE` that finds
nothing** raises `NotFoundError` saying an earlier attempt may already have applied it.

**Docs corrected:** a bulk `timestamp` must be RFC3339; dates elsewhere accept a plain
`YYYY-MM-DD`, and a plain end date covers the whole day; an image needs a caption;
sending `image.reference` means the service keeps no bytes; filters do not apply to
`self_memories`; an idempotency key does not cover a retry that overlaps a first attempt
still running; features are named by the capability `list_models` reports.

**Packaging:** Python requires `requests>=2.32.4` and, for `AsyncClient`,
`httpx>=0.27,<1`, and declares its license as an SPDX expression. The Rust crate no
longer lists a documentation URL (crates.io links docs.rs) and does not package the
fuzz example.

**Correction to 2.2.39:** Rust's `WosError::status()` also changed for two local stops,
the 20,000-page ceiling (0 to 200) and the 64MB response cap (0 to the response's
status).

## 2.2.40 — 2026-09-28

**`get` returns the memory on the default model.** Tablet 2 answers with the memory
itself rather than `{memory: …}`, and every client returned `{}`. All three now take
either shape.

**`search` returns the image rows the answer carried**, after the text memories and
de-duplicated by id. They were billed and dropped. `search_full` / `searchFull` keeps
the fields apart; `search_self` / `searchSelf` is unchanged.

**Search takes `form` and `tz` again** in TypeScript and Python. 2.2.38 and 2.2.39
refused them.

**`add` refuses a metadata key that spells a store id or the idempotency key**
(`userId`, `store_id`, `idempotencyKey`, in any case, with `_`, `-` or `.` anywhere)
before sending, in all three clients; in Python that covers `metadata=` and extra
keyword arguments alike. The store and the idempotency key have their own parameters.

**Python: `fork()` is safe for `Client`.** A forked child drops the pooled connections
it inherited, including proxy pools, so two processes no longer read each other's
responses. Create an `AsyncClient` in the process that uses it.

**Store ids:** the docs and the runtime warning now describe what the service has done
since 2026-09-23. A store id is 1-64 ASCII letters, digits, `.`, `_` and `-`, starting
with a letter or digit; any other id, such as an email address, is refused on create
(400), and the warning now says so. Ids that differ only by case name one store; one
that differs only by punctuation from an existing store is refused on create (409) and
not found on use (404). `create_store` / `createStore` and store listings carry
`canonical_id` when the normalized form differs.

**Images need both edges of at least 700px**; the docs now say so.

## 2.2.39 — 2026-09-18

**Rust: the lockfile moves to rustls 0.23.45, for RUSTSEC-2026-0285.** That does not
reach you. This client uses reqwest's default TLS backend, `native-tls`, and a
library's lockfile is ignored by whoever depends on it. If another crate in your tree
turns on reqwest's rustls backend, update rustls in your own lockfile with
`cargo update -p rustls`.

**408 and 504 now retry on idempotent methods**, as 502 and 503 do. No write is
retried on any of them. 429 is retried on every method, as before.

**Python and TypeScript retry an idempotent method when the connection dies while the
body is being read.** A write is not retried then, because the first attempt may
already have landed.

**`export_images(user_id, page_size=…)` / `exportImages(userId, { pageSize })`**, new
in all three: every image in a store as one list, the image-side pair of
`export_memories`.

`iter_images` / `iterImages` is unchanged. In Python and TypeScript it yields one image
at a time and fetches pages behind the scenes; in Rust it returns the whole list. To
page in Rust, call `list_images` and pass back both cursors it returned, `before` and
`skip_ids`.

**Rust: an empty `add_bulk` category is sent as passed.** It used to be rewritten to
`general`. **This changes what a Rust caller's data is filed under.**

**Rust: `add_bulk_with` and `add_bulk_with_idempotent`.** The first takes extra body
fields such as a `timestamp`, so a backfill can be dated; the second takes them
together with an idempotency key.

**Rust: `add_with_idempotent`** takes an image (or other extra fields) and an
idempotency key in one call.

**TypeScript: a refused argument arrives as a rejection.** Every method that returns a
promise is `async` now. **A `.catch()` that never fired will start firing**, and a
`try`/`catch` around a call you do not await stops catching. Await the call, or handle
the rejection.

**TypeScript: `engram` refuses an option it does not know**, as `recall` does. A
misspelled option used to be dropped without a word.

**`get_image` raises `APIConnectionError` when the body stops arriving**, instead of a
raw transport error (Python and TypeScript). On an error response it reports the
status: a 404 whose body dies mid-read is `NotFoundError` from `get_image`, and on the
async Python client a 429 whose body dies is now retried.

**Rust: a field that will not convert falls back to its default** instead of taking the
whole record out of the list.

**Replies keep fields this version does not name.** Rust's `recall` puts them in
`RecallResponse.extra`; `list_engrams` in Python and TypeScript returns the reply with
only its promised fields normalised.

**Rust: `RecallResponse` is `#[non_exhaustive]`.** Adding `extra` already breaks a
pattern that names every field, and a struct literal; marking it means later fields do
not break it again. Destructure with `..` and build one with `serde_json::from_value`.

**Rust: a caller's own mistake is `ErrorKind::BadRequest`** (status 400), where an
argument out of range used to be `Other`. An exhausted deadline is status 0, which is
`ErrorKind::Connection`. **A `match` arm on `Other` that caught these will stop.**

Comments and docstrings were shortened throughout these files.

## 2.2.38 — 2026-09-15

**A blank store id is refused wherever it is passed.** `add(text, "")` already threw;
`withUser("")`, `with_user(0)` and `Client(user_id=None)` did not — they became the
shared `default` store, silently, and dropped the store the client was bound to on the
way. `withUser` is the per-tenant pattern the docs teach, so a handler whose session
lookup came back empty wrote that end-user's memories where everyone on the account
could read them. The guard lived where only a PASSED id reached it. TypeScript and Python now refuse in
the constructor, so `withUser("")` throws where it is written; Rust's builder returns
`Self` and cannot, so it refuses on the first call that resolves the store. Zero setup —
no `user_id` anywhere — still reaches `default`. **This refuses calls that used to be answered.**

**An option this client does not know is refused before anything is sent.** The service
drops keys it does not recognise and answers normally, so `verfy: 3` bought nothing and
looked exactly like `verify: 3` that worked. `search` now raises and names the key you
probably meant; a genuinely new option goes under `extra`. TypeScript's `recall` does
the same — Python's takes named arguments only, so there was never a bag for a typo to
land in. Rust is unchanged: `SearchOpts` and `RecallOpts` are typed, but `search_with`,
`search_self_with` and `recall_with` still forward a raw JSON object key for key.
**This refuses calls that used to be answered.**

**`getImage` retries like every other call.** Rust and the async Python client had no
retry loop on that route at all, so a 429 was fatal and a connect failure during a
rolling deploy was too — while the documentation promised "429 always". An image
export loop is the most rate-limit-prone code in either client. Both now retry a 429
(honouring `Retry-After`) and a connect-level failure, and both record the quota
headers they were dropping. The sync Python client records them too.

**`require("wontopos")` can be typed, not only run.** The package shipped one
declaration file, and under `"type": "module"` that file is an ESM declaration — so a
TypeScript CommonJS consumer on `node16`/`nodenext` resolution got TS1479 while the
runtime worked fine. The `require` condition now names its own `.d.cts`, and
`./package.json` is exported for the tools that reach for it.

**Smaller, all three clients.** A deadline larger than the clock can represent panicked
inside the Rust SDK on every call; the largest retry count wrapped to no retries at all;
`verify_used` truncated past 255. The async Python backoff slept holding a pooled
connection, so a 429 burst parked the pool. A body that cannot be serialized came back
from TypeScript as a network error with status 0, after sleeping out the whole retry
budget. A whitespace memory id is refused by all three, and an id that passes is sent
trimmed — Python's `get` did neither, and the other two validated by trimming and sent
the raw string. `list_models`, `list_stores` and `list_engrams` coerce a wrong-typed answer
instead of handing it to your for-loop, as does `list_engrams`' `forms`. An API key
with a non-ASCII character — rich text turns a hyphen into an en dash — is named as
such in all three clients instead of dying inside the HTTP stack.

## 2.2.37

**`usage()` — what this key has spent, and what is left.** All three clients, same
surface: this key's lifetime cost, its workspace and per-store cost over a window, and
the prepaid balance that gates the next call. Free, and it skips the balance gate —
charge for it and an account at zero cannot find out why, because the gate would refuse
the very call that explains the refusal. Scoped to the calling key; a sibling key's
spend is not this key's business.

**The npm package ships the documentation and not the implementation notes.** The
build emits JavaScript with comments stripped and declarations with them kept, so
`dist/wontopos.d.ts` carries the doc comments your editor shows on hover and the
JavaScript carries none of the notes we write for ourselves. A test asserts both
halves and fails in either direction.


## Before 2.2.37

These repositories start at 2.2.37. Three releases inside the 2.2 line were
not additive, each deliberately and with its reason recorded at the time:

- **2.2.31** removed the `Wos` client — `create_character` and `chat`, plus
  `WosOptions` in Rust. Exported names disappeared inside the minor line.
- **2.2.34** moved the default engine to `tablet-2`. Pass `tablet-1` as the model
  for the previous default.
- **2.2.35** gave `search` the count range `recall` already had, 5-20. A count
  outside it is refused rather than sent on, so a call that passed 30 was answered
  and now raises instead.
