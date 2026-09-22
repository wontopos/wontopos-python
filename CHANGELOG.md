# Wontopos SDK changelog

One entry per release, covering all three SDKs (Python · TypeScript · Rust).

**Versioning policy:** the three SDKs always release in lockstep — same version,
same surface, same day. Patch releases are additive (new options, hardening,
docs); nothing is removed or reordered within a minor line.

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
