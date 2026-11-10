# Wontopos SDK changelog

One entry per release, covering all three SDKs (Python · TypeScript · Rust).

**Versioning policy:** the three SDKs always release in lockstep — same version,
same surface, same day. Patch releases are additive (new options, hardening,
docs); nothing is removed or reordered within a minor line.

## 2.2.51 — 2026-11-10

**`add` takes when a memory was said, own words off is sent as its own field, the sleep is 1.0.2, and a round of fixes.**

- `add` takes `created_at`, when the memory was said: `created_at=` in Python (a `datetime`
  with a time zone, or RFC 3339 with an offset), `opts.createdAt` in TypeScript,
  `WriteOpts.created_at` with `add_opts` in Rust. It goes as `metadata.created_at` and works
  from Scroll 2 on; a model before it refuses the write (403). The metadata key is no longer
  warned about as dropped, and TypeScript exports the options type `AddOptions`.
- With own words off (`own_words=False`, `ownWords: false`, `.with_own_words(false)`, and the
  `S-2-live` preset), `open_work` sends `own_words: false`. 2.2.50 sent it as a filter, which
  the service has taken since 2026-11-09; before that such a look still brought the
  assistant's lines.
- A memory tool result shows a date past year 9999, or before year 0, whole (`+010000-01-01`,
  `08:59`), where it cut both short.
- Inside a memory, a vertical tab, a form feed and the file, group and record separators
  (U+000B, U+000C, U+001C to U+001E) are shown in a tool result as a line break is: a line
  feed and four spaces in their place.
- In TypeScript a call's deadline is measured on a steady clock, as Python and Rust measure
  it: a step of the system clock no longer ends a call or lets a retry run past the deadline.
- In TypeScript a `baseUrl` with `user:password@` sends them as Basic auth and leaves them
  out of the URL, as Python and Rust do; with the built-in `fetch` every call failed before.
- In Python `close()` also closes the connections of the model callers the router built from
  `router.key` and `router.skill_key`; a caller you passed in stays open.
- The router's prompt writes U+0085, U+2028 and U+2029 inside a skill's input schema as
  escapes, so a schema cannot break a line of the prompt.
- On a model without the router's history (the read answers 404 or 501), `ask` no longer
  sends the write that would fail too, and its `history` reads `skipped` for both where it
  read `failed`.
- Rust's `RecallResponse` has `room_memories()`: the memories from the rooms.
- Python names `urllib3` (`>=1.26.20,<3`) as a dependency, since it imports it. `requests`
  asks for 1.21.1 or later, so an install that pins `urllib3` below 1.26.20 has to move it.
- Python's source archive no longer carries the tests.
- Refused now, where 2.2.50 sent them:
  - `add` with `user_speaker` or `assistant_speaker` as a metadata key, in all three (in
    Python the keyword too, which becomes that key): they name a side of a turn, and `add`
    takes `speaker`. As an option, TypeScript refuses `userSpeaker` and `assistantSpeaker`
    where it warned and left them out, and Rust refused them already;
  - `add` with a `created_at` that is not an instant, such as a plain date or a time with no
    offset, where the metadata key was sent as given.
- The sleep is 1.0.2. Its rules are the same; it holds them in seven more places:
  - with two or more people, a speaker named as the being, as `user`, or holding ` · ` is
    shown in quotes;
  - what the files and the notes hold is shown to the sleep's models, and to the being, with
    the sleep's tags opened and every break but a plain line break kept inside its line: a
    body, a box title, a section heading and a proposal's text;
  - a file's age reads `1 day ago`, `today` for a day after today, and is left out when the
    file has no day;
  - text a core holds before its first heading is kept as the start of `## Me`;
  - a section is moved only into a file that does not hold one of that name, and a box remade
    after a person was moved into it that night still holds that person;
  - a core section named for the being, by its name or the name it speaks under, in any
    case, is `## Me`;
  - one of the sleep's tags written in capitals is opened as one in small letters is.
- The docs say which models an option works from: in TypeScript and Rust, `recall`'s
  `short_term_limit`, `short_term_budget_tokens`, `max_images` and `include_superseded` and
  `add_bulk`'s `segments` from Scroll 2 on, and in all three `forget_speaker` from Tablet 2
  on.

## 2.2.50 — 2026-11-08

**Your code and your model can see how much of the plan's week is used, the memory tools look the way a conversation needs, and more than 140 fixes.**

- `usage()` with a key in a subscription's workspace returns `subscription.week.days`: one
  entry for each UTC day from the day the week began to today, with the percent of the week
  that day used. A plan's usage is still given in percent only.
- `week_used` (`weekUsed` in TypeScript, `week_used()` in Rust) is the whole percent of the
  plan's week used, from 0 to 100, read from the latest answer that carried the
  `X-WOS-Week-Used` header, whatever its status. It is `None` (`null`) after a successful
  answer without it, which is what a key outside a subscription's workspace gets.
- TypeScript types `overflow_to_balance` and `week.days` in `usage()`'s `subscription`.
- The memory tools take a `usage` switch (`usage=True`, `{ usage: true }`,
  `.with_usage(true)`), off by default. On, it adds `memory_usage`, which tells your model
  this week's plan usage in percent: used, left, today's (UTC) and when the week resets.
  Once a turn it also adds one line to a tool result when the week is at 80% or more, or
  when that call was paid from API credit, so the model can tell the user. Off, it adds no
  tool and no line.
- The memory tools have two presets on scroll-2. `S-2` is for measuring memory: its first
  look searches with the question alone, as a benchmark asks. `S-2-live`, now the default,
  is for everyday conversation: its first look also reads the last two lines said in the
  past five minutes, leaves the assistant's own lines out, and spreads what it brings across
  topics and time. `preset="S-2"` keeps what 2.2.49 did. In Rust, `memory_tools_preset`
  names a preset.
- Two settings choose the same things on any preset: whether a look may bring the
  assistant's own lines (`own_words`, `ownWords`, `with_own_words`) and the diversity it
  asks for (`diversity`, `with_diversity`: `none`, `mmr`, `temporal` or `full`). `S-2-live`
  sets them to off and `full`; `S-2` leaves both as 2.2.49 did. In Rust they are also fields of
  `LookOpts`: code that builds it without `..Default::default()` needs it now.
- A stored memory's line breaks stay inside it: in a tool result each line feed, carriage
  return, carriage return and line feed pair, U+0085, U+2028 and U+2029 becomes one line feed
  followed by four spaces; a vertical tab or a form feed is left as it is. A speaker's name is
  shown on one line, and a person named `you`, `user` or `note` is shown in quotes. A
  memory without a line break, from a speaker whose name is none of those, is shown as before.
- The line that says a call was paid from API credit goes only on that call's own result.
- The sleep is 1.0.1. Its rules are the same; it holds them more closely:
  - an edit whose section heading, move heading or box title holds a line break or another
    control character is dropped before anything else is checked;
  - a memory's line breaks stay inside its line in the sleep's prompts, and the sleep's own
    tags inside a memory lose their opening bracket;
  - the merge rewrites a person's section only when tonight's items name that person: the same name,
    the name with something joined to its end when the name has two letters or more, or the
    start of a longer name of three letters or more;
  - the room made for a core over its cap keeps only sections the core had, and moves no one
    when it is refused;
  - each model call ends at its 600-second limit;
  - naming a proposal as ripe marks it applied only on a rewrite of "## Me" made from a ripe
    proposal;
  - a new proposal takes the next free id, and a proposal the sleep cannot read is written
    back as it was;
  - past a size cap, the notes drop the oldest applied proposals first, then the oldest marks;
  - `open_box` refuses a blank name and looks for an exact match first.
- Refused now, where 2.2.49 sent them:
  - a `base_url` with a query (`?`) or an anchor (`#`), for the Wontopos API and for a model
    provider;
  - a Wontopos key that starts with `sk-`, and a model provider key that starts with `wos-`
    or is the client's own Wontopos key;
  - a deadline or timeout that is not a number, is NaN or is negative (0 keeps its meaning),
    and a retry count that is not a whole number at 0 or more;
  - a speaker name with a line break or another control character, on every write that
    takes one; a name is trimmed, and a blank or over-long one is refused, as `add_speaker`
    refuses it;
  - values the docs bound that the service would have taken or dropped: `verify` outside 0
    to 3, `filters.min_importance` outside 0 to 1, a per-call `tz` that is not a number of
    hours from -14 to 14, a blank `engram` name (now trimmed), a blank `update` text, and a
    NaN or an infinity anywhere in a request body (TypeScript sent them as `null`);
  - an `add_turn` whose user and assistant messages are both blank;
  - a blank `update` id or `remove_speaker` name (both now trimmed);
  - a sleep's `today` whose hour is over 23 or minute over 59;
  - in Python, a `forget_image` `preview` that is not `True` or `False`;
  - in TypeScript, options that `forgetImage`, `listMemories`, `iterMemories`,
    `exportImages`, `listImages`, `iterImages`, `revisions` and `bySpeaker` do not know, and
    a `preview` that is not `true` or `false`;
  - in TypeScript, an `addTurn` `createdAt` Date before the year 1 or after 9999;
  - in TypeScript, a client in a browser Web Worker, Shared Worker or Service Worker, as a
    browser page is refused, unless `dangerouslyAllowBrowser` is set;
  - in Rust, `with_router` with skills the new options cannot run.
- A `search` with filters leaves out the assistant's own words, which the service does not
  filter.
- A sleep that cannot read part of the new memories, or whose merge comes back twice as text
  that is not JSON, no longer moves the store's last sleep past them: its `done` is `None`
  (`null`), and the next sleep reads them again.
- The router reads a skill input that the model wrote as an object, where it read an empty
  one, and `ask` gives `too_large`, not `bad_request`, for any provider's 400 that says the
  prompt is too long.
- In Python, `RateLimitError.retry_after` rounds a `Retry-After` given as a date up to whole
  seconds, as TypeScript does.
- A printed client shows at most the last four characters of a key, and only for a key of
  24 characters or more.
- More than 140 fixes across the three clients and the MCP server, each first shown by a
  test that failed. Among them:
  - Python reads a compressed answer under the size cap, and refuses an answer with more
    than one encoding;
  - Python's async model callers work without asyncio (anyio on trio), and a SOCKS proxy no
    longer outlasts the call's deadline;
  - the router finds the model's JSON, and the sleep its fenced JSON, in one pass over the
    answer;
  - the router's limits keep today's counts when many stores ask, and in Rust a router from
    `with_router` keeps them;
  - `openai_caller` steps its response format down only when the provider refuses the
    format;
  - TypeScript calls a `fetch` you pass as a plain function, so the browser's own `fetch`
    works;
  - Rust sends `X-API-Key` marked as sensitive;
  - the docs of `revisions` say what it counts: memories edited after they were written,
    and storing new ones never edits an old one.
- Each client's source is split into files of at most 5,000 lines and grouped by feature.
  Nothing you import moved.

## 2.2.49 — 2026-11-05

**Scroll 2: a short-term window and rooms beside long-term memory, a working memory for one question, memory tools your model uses by itself, sleep, and a router for your skills.**

- Each call below is in all three clients and in Python's `AsyncClient`, unless its line
  names one language, each in its language's spelling: `open_work` in Python and Rust,
  `openWork` in TypeScript.
- A Scroll 2 store needs its time zone and the names of who speaks before its first write:
  `set_settings(time_zone=..., default_speakers=...)` and `add_speaker(...)`, and
  `settings()` reads them back. A write without them is refused with `time_zone_required` or
  `speaker_required`, and the error carries that code. `delete_all` clears both, so the
  store needs them again before its next write.
- `relabel_speaker` moves memories from one speaker to another. `forget_speaker` forgets what
  one or more speakers said; call it with `preview=True` first to see what would go.
  `remove_speaker` takes a name off the list and leaves the memories where they are; the
  store's default speaker is refused with 409 `speaker_is_default`.
- `add_turn` takes `created_at` (an RFC 3339 time with `Z` or `±hh:mm`), `user_speaker` and
  `assistant_speaker`, so a past conversation can be written as it happened, each side under
  its own name. `add_bulk` takes `segments`, each with its own speaker and time, and
  `add(..., window=True)` also puts the line in the short-term window.
- `recall` returns the short-term window and the rooms of the last days beside the
  long-term memories, and takes options for each: one room, how many, how far back, and
  `rooms=False` for long-term alone. `seen` returns the memories one window line held, and
  `clear_window` empties the window. `around`, `why` and `retrievals` show what was said
  around a memory, what an answer rested on, and what was recalled.
- `open_work` opens a working memory for one question. The first 20 memories come numbered
  `[01]` to `[20]`, later calls refer to them by number, and no memory comes twice. A work
  has five steps: `work_search` searches again with a question of your own, and
  `work_around` shows what was said around one numbered memory, leaving out what the work
  already holds. `work_deeper` brings the next 20 for the same question, up to three times,
  and is not a step. Every call answers with the work's state (`seeking`, `asked`, `kept` or
  `closed`), the steps left, and only the memories that call added. `rooms` looks in the
  rooms of the last three days instead of all of long-term, and `max_images` (0 to 5) is how
  many photos each look of the work brings up.
- With the steps spent, or when your model is unsure, `work_ask` marks the work as waiting
  on the person. Ask them in your own words and pass the reply to `work_confirm`: yes keeps
  the work for five more turns with five steps again, no sends it seeking again without
  giving the same memories twice. `close_work` closes it, with the memory that answered as
  `answer_id`, and storing the answer with `work_id` on `add` or `add_turn` closes it too.
  When the answer took several steps, how it was reached is kept three days and `why` shows
  it. `work_state` shows the open works and the recent ones.
- `memory_tools(user_id, preset="S-2")` (`memoryTools`, Rust `memory_tools_preset`) gives
  your model tools to look through its memory by itself, in Anthropic's or OpenAI's tool
  format. Each turn, `begin(question)` gives what comes to mind, numbered, for the prompt,
  and `run(call)` answers each tool call: `memory_deeper` looks further with the same
  question, `memory_around` shows what was said around one memory, and `memory_search` looks
  again with a question the model writes. Put `note` and the definitions in the part of the
  prompt that stays the same, so they cache, and store the answer with
  `add_turn(..., work_id=tools.work_id)`. The preset `S-2` sets the note, the definitions
  and the model the tools call, together.
- Each memory tools switch is off by default. `speakers` puts who said it on every line,
  `dates` adds `memory_timeline`, `images` brings up to three photos a look and adds
  `memory_photo`, `sleep` puts the files a sleep wrote in the note and adds `open_box`, and
  `seen` adds `memory_recent` and `memory_seen`: the recent conversation, and the memories one
  of its lines held, as they stand now. After a sleep, `load()` reads the files again. In
  Rust the switches are `with_speakers`, `with_dates`, `with_images`, `with_sleep` and
  `with_seen`.
- `sleep()` reads what came into the store since the last sleep and rewrites the AI's own
  files: a core about itself and four boxes it names and fills, with every version kept. The
  core is written with "I" as the subject: what the AI likes, what it won't do, and how it
  sees the people in its life. The core can hold 70,000 tokens and each box 20,000. A sleep
  runs on the model your app picks, with your app's key (Claude Sonnet 5.5 unless you name
  another), and only when you call it: at night, for a nap, or on a command. Sleep is
  experimental.
- `sleep_estimate()` says what a sleep would read and about what it would cost before it
  runs, and whether the AI is `sleepy`: past 10 million tokens of new memory since the last
  sleep, and it need not sleep then either. Pass its `sleep_id` to `sleep` so the same
  memories are not read twice. `being` is the name the sleep calls the AI by, `prompt` is
  written into the core the first time, `settings` sets the size of the core and each box,
  and `done=False` leaves the last sleep where it was, so the next one reads the same lines
  again.
- A model call in a sleep that meets a rate limit, an overload or a dropped connection is
  tried twice more, and any other failure ends the sleep with nothing written. If the files
  change while it runs, it raises `ConflictError` and keeps none of that night's changes.
  When the lines name two or more people, each keeps their own name.
- `sleep_files`, `open_sleep_file` and `sleep_file_history` read what a sleep wrote. To build
  the steps yourself there are `sleep_read`, `sleep_verify`, `write_sleep_files` (all or
  none), `sleep_done`, `sleep_notes` and `write_sleep_notes`.
- `ask` sends a request your model cannot do itself to a router. The router reads the
  store's skills, its last exchanges with the store and the time on the person's clock, then
  runs one skill, says what the skills can do, or says it cannot; it never reads memory. The
  router and the skills run on your app's model and key (Claude Haiku 5.5 unless you name
  another), and the key never goes to Wontopos. Once it runs, `ask` never raises: a failure
  comes back as `ok: false` with a reason and a text for the model. `can_ask` says, with no
  model call, whether `ask` can run now, `router_tools` is the tool to offer the model then,
  and `router_history` shows the router's last exchanges with a store.
- The router needs the person's time zone, an IANA name in `time_zone`; without one, `ask`
  is refused. Its `limits` default to 3 asks per answer, 3 per 60 seconds, 75 seconds per
  answer and 200 per store a day, counted in your process.
- Skills live per store, in your process. `add_skill` takes a name, the description the
  router reads, a kind (`llm`, which the skill model follows, or `code`, your function in
  `handlers`) and a JSON Schema for its input; a skill with `confirm=True` runs only through
  `run_skill`, after a person agrees. `list_skills`, `remove_skill`, `copy_skill`,
  `export_skills` and `import_skills` manage them, and `import_skill_md` makes an `llm`
  skill from an Agent Skills `SKILL.md`, reading that text only.
- `anthropic_caller` sends to Anthropic's Messages API. `openai_caller` sends to any server
  that speaks OpenAI's chat completions: OpenAI, DeepSeek, Qwen, Llama hosts, Gemini's
  compatible endpoint, and local servers with or without a key. A function of your own works
  too, and sleep uses the same connections.
- Python now depends on `tzdata`, so a router `time_zone` works on a system with no
  time-zone database.
- `key_scope()` (`keyScope()`) says what a key may do: `"full"`, `"read"` or `"admin"`. An
  admin key lists stores and reads usage, and opens no memory. Where only an admin key lists
  stores, `list_stores()` with a standard key raises a permission error, and `usage()` leaves
  `stores` empty with `stores_admin_only` set. `ping()` checks the key without listing
  anything.
- Every client gives the service's error `code` (`sleep_expired`, `speaker_is_default`, ...).
  A `sleep_expired` 410 says so, not that the model was retired: Python and TypeScript raise
  it as `WosError`, not `GoneError`, and Rust's `is_gone()` is false. In Rust,
  `WosError::reason()` and `WosError::resets_at()` give a 402's reason and when the week
  resets, as `details` does in Python and TypeScript.
- `usage()` gives a key in a subscription's workspace its plan and this week:
  `subscription.week.used_percent` and `resets_at`. `billed_to` (`billedTo`, `billed_to()`)
  is `"balance"` when the last call was paid from API credit because the week is used up and
  the workspace carries on with API credit. A used-up week is a 402 whose `details.reason` is
  `weekly_limit_reached` or `weekly_limit_insufficient`, with `resets_at`.
- Rust: `RecallOpts` has new fields, so code that builds it without `..Default::default()`
  needs it now. serde_json's `preserve_order` is on, so a `serde_json::Map` anywhere in a
  build that uses this crate keeps its keys in the order they were written.

## 2.2.48 — 2026-10-20

**A long text is stored without the client giving up first.**

- `add`, `add_turn` and `add_bulk` (in Rust, their `_with` and `_idempotent` forms too)
  wait up to 200 seconds an attempt when you set no timeout. Storing a long text can take the service up to three minutes, and a client
  that stopped at 30 seconds reported a timeout for a memory that was still being
  stored. Every other call still waits 30 seconds. A timeout you set (`timeout`,
  `timeoutMs`, `with_timeout`) applies to every call, these three included.

## 2.2.47 — 2026-10-14

**A write that met a server error is sent again safely, a retiring model says so before
the day, and an API key stays out of browser pages.**

- Every memory write (`add`, `add_turn`, `add_bulk`, `update`) carries an idempotency
  key: the one you pass, or one the client makes for the call. With it a write is retried
  on 408/502/503/504 and on a connection that dropped, as a read is, and the service
  answers the repeat with the first attempt's result instead of storing it again. A repeat
  that arrives while the first attempt is still running is answered with a 409 to wait,
  and retried. One whose first attempt did not come back with an answer, so whether it
  was stored is not known, is refused with a 409 instead of being stored twice. The
  service keeps a key for 24 hours. The client sets `replayed` only for
  a key you passed. The other writes sent as POST (`create_store`, `add_speaker`, `delete`,
  `delete_all`) are still not sent again on those four.
- A model scheduled to retire names the day in a `Sunset` header on its answers, and the
  client warns once per model and day: which model, the day, and that its calls answer
  410 from then on, `delete_store` aside.
- TypeScript refuses to run in a browser page that would send the API key to the service:
  anyone who opens the page can read the key, and with it every memory in its workspace.
  Call Wontopos from your server, or point `baseUrl` at a proxy of your own that holds the
  key. `dangerouslyAllowBrowser: true` turns the refusal off, for a local demo with a key
  you will revoke.
- `recall`'s parts are typed. TypeScript types `short_term.turns` as `HistoryTurn[]` and
  `context.around_top_memory` as `string[]`; Rust adds `RecallResponse::turns`,
  `memories` and `context_rows`, and a `Turn` type; Python's docstring describes each
  part. A row of any other kind is dropped, as `history` drops it. Each row around the
  best memory starts with `[before] `, `[match] ` or `[after] `.
- The READMEs have a table of the methods by what they do, and show a recall put into a prompt, photos, export,
  edit history and usage.

## 2.2.46 — 2026-10-09

**Reads are retried, and the docs say what the service does.**

- A call that only reads is retried on 408/502/503/504 and on an answer that dropped
  mid-body, in all three: `search`, `recall`, `get`, `list_memories`, `stats`, `history`,
  `lineage`, `by_speaker`, the image listing and fetch, `engram` and `revisions` go as
  POST, and the clients decided from the method alone, so none of them was retried,
  though the docs said reads were. A 503 "Retry in a moment" on a read now reaches
  the caller only when the retries run out. A write sent as POST is not sent again on those, and a timeout
  after the request went out is still not retried.
- Docs, where they said what is not so:
  - Photos: a long edge between 1568px and 2500px is downscaled to 1568; over 2500px,
    or an image file over 7MB, is refused (400) with the reason. They said every long
    edge over 1568px was downscaled.
  - `recall` returns the recent turns, at most `limit` memories and at most
    `context_limit` rows of the surrounding conversation. The READMEs called it a fixed slice
    of about 1,000 tokens.
  - The no-LLM line reads: no language model reads your stored memories.
  - `lineage` follows the edits from the id you pass forward; pass the first version's
    id to see the whole history. It was described as the whole chain behind any id.
  - Filters: a narrow filter can return fewer than `limit` even when the store holds
    more inside it. A memory with no `event_date` is matched by when it was stored.
  - `search` lists memories best first, then the assistant's own words and image
    memories in their own order, and each `verify` pass can add up to `limit` more.
  - `usage`: `workspace` and `stores` include every key in the workspace; `key` is this
    key's own. `remove_speaker` leaves the name on stored memories on `tablet-2`. A 402
    can also mean the month's spend cap for the account's tier. Recall caching bills a
    repeated query at 0.1x, and an extended one at 0.1x for its cached part only.
  - `get` no longer promises a 404 for every id `list_memories` would not show,
    `stats` says its total can be larger than what `list_memories` lists,
    `verify_used` counts passes that added memories, an API key picks a workspace's
    memory (it said an account's), and Python's examples create the store and register
    a speaker before using them.

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
