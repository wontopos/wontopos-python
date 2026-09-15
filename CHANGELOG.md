# Wontopos SDK changelog

One entry per release, covering all three SDKs (Python · TypeScript · Rust).

**Versioning policy:** the three SDKs always release in lockstep — same version,
same surface, same day. Patch releases are additive (new options, hardening,
docs); nothing is removed or reordered within a minor line.

Four patches have bent that rule so far, deliberately and with the reason written
down in each entry. 2.2.31 removed the `Wos` client: an exported name disappeared
inside the minor line. 2.2.34 moves the default engine: nothing was removed and no
signature changed, but a caller who names no model gets different results than before.
2.2.35 gives `search` the count range `recall` has always had, so a call that passed
30 was answered and now raises instead. 2.2.38 refuses an option TypeScript and Python do not know, and refuses a blank store
id in all three — both were accepted and sent before. All four are named here rather than left for a reader to
notice — a rule you can bend without saying so is not a rule.

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
