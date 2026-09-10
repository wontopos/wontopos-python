# Wontopos SDK changelog

One entry per release, covering all three SDKs (Python · TypeScript · Rust).

**Versioning policy:** the three SDKs always release in lockstep — same version,
same surface, same day. Patch releases are additive (new options, hardening,
docs); nothing is removed or reordered within a minor line.

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
