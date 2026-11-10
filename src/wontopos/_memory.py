from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Optional, Sequence, Union

from . import (
    PermissionDeniedError, WosError, _DELETE_ALL_NEEDS_ID, _DELETE_NEEDS_ID, _DELETE_STORE_NEEDS_ID, _GET_NEEDS_ID,
    _LISTED_NEEDS_ID, _MAX_PAGES, _NO_ID, _around_body, _as_records, _bulk_body, _check_include, _cursor_repeats,
    _default_tz, _form_body, _id_body, _is_int, _js_str, _js_trim, _list_size, _memory_from_get, _memory_page,
    _merge_results, _metadata, _named_store, _normalize_image, _page_fields, _recall_body, _recall_shape,
    _require_memory_id, _required, _retrievals_body, _search_body, _supersede_body, _truncated, _turn_body,
    _window_line, _with_records, _work_result, _write_extras,
)


def _engram_catalog(r: Any) -> dict:
    out = _with_records(_with_records(r, "engrams"), "forms")
    note = out.get("note")
    out["note"] = note if isinstance(note, str) else None
    return out


def _full_search(r: Any) -> dict:
    out = dict(r) if isinstance(r, dict) else {}
    for field in ("memories", "self_memories", "images"):
        out[field] = _as_records(out.get(field))
    return out


def _self_search(r: Any) -> dict:
    return {"memories": _as_records(r.get("memories")), "self_memories": _as_records(r.get("self_memories"))}


_ADMIN_ONLY = "Only an admin key lists stores; this is a standard API key."


def _store_list(r: Any) -> list:
    if isinstance(r, dict) and r.get("admin_only") is True:
        raise PermissionDeniedError(403, _ADMIN_ONLY)
    return _as_records(r.get("collections"))


def _key_scope(r: Any) -> str:
    scope = r.get("scope") if isinstance(r, dict) else None
    if not isinstance(scope, str) or not scope:
        raise WosError(200, "the answer carried no key scope")
    return scope


def _clear_window_body(sid: str, up_to: Optional[str]) -> dict:
    body: dict = {"user_id": sid}
    if up_to is not None:
        body["up_to"] = _required(up_to, "up_to")
    return body


def _list_memories_body(client: Any, user_id: Optional[str], limit: Optional[int], cursor: Optional[str]) -> dict:
    limit = _list_size(limit, "limit")
    body: dict = {"user_id": client._uid(user_id), "limit": limit}
    if cursor:
        body["cursor"] = cursor
    return body


def _usage_path(days: Any) -> str:
    if not _is_int(days) or not 1 <= days <= 365:
        raise ValueError(f"days must be an integer between 1 and 365, got {_js_str(days)}.")
    return f"/api/v1/won/usage?days={days}"


def _revisions_body(client: Any, user_id: Optional[str], include: Optional[str], limit: Optional[int],
                    before: Optional[str], skip_ids: Optional[list]) -> dict:
    _check_include(include)
    page = _page_fields(limit, before, skip_ids)
    return {"user_id": client._uid(user_id), **({} if include is None else {"include": include}), **page}


def _add_body(client: Any, content: str, user_id: Optional[str], metadata: Optional[dict], extra: dict,
              image: Optional[dict], tz: Optional[float], work_id: Optional[str], window: Optional[bool],
              window_role: Optional[str], created_at: Any = None) -> dict:
    sid = client._uid(user_id)
    md = _metadata(metadata, extra, created_at)
    body = _window_line(_write_extras({"user_id": sid, "content": content, "metadata": md},
                                      tz, client._tz, work_id), window, window_role)
    if image is not None:
        body["image"] = _normalize_image(image)
    return body


def _by_speaker_body(client: Any, speaker: Any, user_id: Optional[str], limit: Optional[int],
                     before: Optional[str], skip_ids: Optional[list]) -> dict:
    if not isinstance(speaker, str) or not _js_trim(speaker):
        raise ValueError('speaker is required — "me" for the assistant, or a person\'s name')
    page = _page_fields(limit, before, skip_ids)
    return {"user_id": client._uid(user_id), "speaker": _js_trim(speaker), **page}


class _MemoryCalls:
    def add(self, content: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
            idempotency_key: Optional[str] = None, metadata: Optional[dict] = None,
            image: Optional[dict] = None, tz: Optional[float] = None,
            work_id: Optional[str] = None,
            window: Optional[bool] = None, window_role: Optional[str] = None,
            created_at: Optional[Union[str, datetime]] = None,
            **extra: Any) -> dict:
        """Store one memory. Extra keyword arguments become metadata.
        ``created_at``: when it was said, a datetime with a time zone or RFC 3339 with an offset (from Scroll 2 on)."""
        body = _add_body(self, content, user_id, metadata, extra, image, tz, work_id, window, window_role, created_at)
        return self._write("/api/v1/memory/store", body, model=model, idempotency_key=idempotency_key)

    store = add

    def add_turn(
        self, user_msg: str = "", assistant_msg: str = "", user_id: Optional[str] = None, *,
        model: Optional[str] = None, idempotency_key: Optional[str] = None,
        tz: Optional[float] = None, work_id: Optional[str] = None,
        created_at: Optional[Union[str, datetime]] = None,
        user_speaker: Optional[str] = None, assistant_speaker: Optional[str] = None,
    ) -> dict:
        """Store a conversation turn: the person's message and the assistant's reply."""
        body = _turn_body({"user_id": self._uid(user_id), "user_msg": user_msg, "assistant_msg": assistant_msg},
                          created_at, user_speaker, assistant_speaker)
        return self._write("/api/v1/memory/store-turn", _write_extras(body, tz, self._tz, work_id), model=model,
                           idempotency_key=idempotency_key)

    def add_bulk(
        self, content: Optional[str] = None, user_id: Optional[str] = None,
        category: Optional[str] = "general", timestamp: Optional[str] = None, *,
        segments: Optional[Sequence[Mapping[str, Any]]] = None, speaker: Optional[str] = None,
        model: Optional[str] = None, idempotency_key: Optional[str] = None,
    ) -> dict:
        """Bulk-ingest a large blob of text in one call."""
        body = _bulk_body(self._uid(user_id), content, category, timestamp, segments, speaker)
        return self._write("/api/v1/memory/bulk-store", body, model=model,
                          idempotency_key=idempotency_key)

    def update(
        self, old_memory_id: str = "", new_content: str = "", user_id: Optional[str] = None, *,
        model: Optional[str] = None, idempotency_key: Optional[str] = None,
        tz: Optional[float] = None, work_id: Optional[str] = None,
    ) -> dict:
        """Supersede an old memory with new content."""
        return self._write("/api/v1/memory/supersede",
                           _supersede_body(self, user_id, old_memory_id, new_content, tz, work_id),
                           model=model, idempotency_key=idempotency_key)

    def search(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> list[dict]:
        """Search a store's memories."""
        body = _default_tz(_search_body(self._uid(user_id), query, limit, opts, verify, max_images), self._tz)
        r = self._post("/api/v1/memory/search", body, model=model)
        return _merge_results(r, bool(body.get("filters")))

    def search_full(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> dict:
        """Search, and keep every field the answer came with."""
        body = _default_tz(_search_body(self._uid(user_id), query, limit, opts, verify, max_images), self._tz)
        return _full_search(self._post("/api/v1/memory/search", body, model=model))

    def search_self(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> dict:
        """Search, with the assistant's own words kept apart from the other memories."""
        body = _default_tz(_search_body(self._uid(user_id), query, limit, opts, verify, max_images), self._tz)
        return _self_search(self._post("/api/v1/memory/search", body, model=model))

    def recall(
        self, query: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
        form: Optional[str] = None, tz: Optional[float] = None,
        limit: Optional[int] = None, context_limit: Optional[int] = None,
        rooms: Optional[Union[str, bool, Sequence[str]]] = None, room: Optional[str] = None,
        room_limit: Optional[int] = None, room_expand_tokens: Optional[int] = None,
        long_term: Optional[bool] = None, long_term_cutoff: Optional[bool] = None,
        today_start: Optional[str] = None, now: Optional[str] = None,
        short_term_limit: Optional[int] = None, short_term_budget_tokens: Optional[int] = None,
        max_images: Optional[int] = None, filters: Optional[dict] = None,
        speaker: Optional[str] = None, include_superseded: Optional[bool] = None,
    ) -> dict:
        """One-call context for an LLM: recent turns, long-term memories and the context around them."""
        body = _recall_body(self._uid(user_id), query, form, tz, limit, context_limit, rooms, self._tz,
                            room=room, room_limit=room_limit, room_expand_tokens=room_expand_tokens,
                            long_term=long_term, long_term_cutoff=long_term_cutoff,
                            today_start=today_start, now=now, short_term_limit=short_term_limit,
                            short_term_budget_tokens=short_term_budget_tokens, max_images=max_images,
                            filters=filters, speaker=speaker, include_superseded=include_superseded)
        return _recall_shape(self._post("/api/v1/memory/recall", body, model=model))

    def engram(
        self, name: str, query: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
        form: Optional[str] = None, tz: Optional[float] = None,
    ) -> dict:
        """Run a built-in engram."""
        body = _form_body({"name": _required(name, "name"), "user_id": self._uid(user_id), "query": query}, form,
                          tz if tz is not None else self._tz)
        return self._post("/api/v1/engram/run", body, model=model)

    def around(
        self, memory_id: str, user_id: Optional[str] = None, *, from_: Optional[int] = None,
        to: Optional[int] = None, center: Optional[int] = None,
        held: Optional[Sequence[int]] = None,
    ) -> dict:
        """What was said around a memory, counted from it."""
        body = _around_body(self._uid(user_id), memory_id, from_, to, center, held)
        return _with_records(self._post("/api/v1/memory/around", body), "pieces")

    def why(self, memory_id: str, user_id: Optional[str] = None) -> dict:
        """How a memory was reached, when it answered a work of three steps or more."""
        memory_id = _require_memory_id(memory_id, "the id a search, recall or work returned")
        return self._post("/api/v1/memory/why", {"user_id": self._uid(user_id), "memory_id": memory_id})

    def retrievals(self, user_id: Optional[str] = None, *, limit: Optional[int] = None,
                   before: Optional[str] = None) -> dict:
        """What this store handed out, newest first."""
        body = _retrievals_body(self._uid(user_id), limit, before)
        return _with_records(self._post("/api/v1/memory/retrievals", body), "entries")

    def history(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """Recent conversation turns (short-term memory)."""
        return _as_records(self._post("/api/v1/memory/history", {"user_id": self._uid(user_id)}, model=model).get("turns"))

    def seen(self, at: str, user_id: Optional[str] = None) -> dict:
        """The memories a line of the recent conversation had in hand when it was said."""
        body = {"user_id": self._uid(user_id), "at": _required(at, "at")}
        return _with_records(self._post("/api/v1/memory/seen", body), "seen")

    def clear_window(self, user_id: Optional[str] = None, *, up_to: Optional[str] = None) -> dict:
        """Empty the recent conversation, or with ``up_to`` only the lines last seen at or before it."""
        body = _clear_window_body(self._uid(user_id), up_to)
        return self._post("/api/v1/memory/clear-window", body)

    def stats(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> dict:
        """Memory counts for a store."""
        return self._post("/api/v1/memory/stats", {"user_id": self._uid(user_id)}, model=model)

    def get(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *, model: Optional[str] = None) -> dict:
        """Fetch one memory by id. An unknown id raises :class:`NotFoundError`."""
        body = _id_body(self, user_id, memory_id, _GET_NEEDS_ID)
        return _memory_from_get(self._post("/api/v1/memory/get", body, model=model))

    def list_memories(
        self, user_id: Optional[str] = None, *, limit: Optional[int] = 100, cursor: Optional[str] = None,
        model: Optional[str] = None
    ) -> dict:
        """One page of a store's memories. Pass ``next_cursor`` back as ``cursor`` for the next page."""
        body = _list_memories_body(self, user_id, limit, cursor)
        return _memory_page(self._post("/api/v1/memory/list", body, model=model))

    def iter_memories(self, user_id: Optional[str] = None, *, page_size: Optional[int] = 100,
                      model: Optional[str] = None):
        """Every memory in a store, one at a time, paging as it goes."""
        page_size = _list_size(page_size, "page_size")
        cursor: Optional[str] = None
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = self.list_memories(user_id, limit=page_size, cursor=cursor, model=model)
            rows = _as_records(page.get("memories"))
            for m in rows:
                yield m
            nxt = page.get("next_cursor")
            if not nxt or _cursor_repeats(nxt, seen, rows):
                return
            cursor = nxt
        raise _truncated(f"stopped after {_MAX_PAGES} pages — the store did not end")

    def export_memories(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """All of a store's memories, as a list."""
        return list(self.iter_memories(user_id, model=model))

    def usage(self, days: int = 7) -> dict:
        """What this key has spent, and what is left."""
        return self._request("GET", _usage_path(days))

    def revisions(self, user_id: Optional[str] = None, *, include: Optional[str] = None,
                  limit: Optional[int] = None, before: Optional[str] = None,
                  skip_ids: Optional[list] = None, model: Optional[str] = None) -> dict:
        """How many of this store's memories were edited after they were written (storing new ones never edits an old one), out of all of them: 0 means every memory is as it was first stored."""
        body = _revisions_body(self, user_id, include, limit, before, skip_ids)
        return _work_result(self._post("/api/v1/won/revisions", body, model=model))

    def lineage(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                model: Optional[str] = None) -> dict:
        """The edits that followed one memory, oldest first."""
        body = _id_body(self, user_id, memory_id, _LISTED_NEEDS_ID)
        return _with_records(self._post("/api/v1/memory/lineage", body, model=model), "chain")

    def by_speaker(self, speaker: str, user_id: Optional[str] = None, *,
                   limit: Optional[int] = None, before: Optional[str] = None,
                   skip_ids: Optional[list] = None, model: Optional[str] = None) -> dict:
        """What one person said, newest first."""
        body = _by_speaker_body(self, speaker, user_id, limit, before, skip_ids)
        return _with_records(self._post("/api/v1/memory/by-speaker", body, model=model), "memories")

    def ping(self) -> bool:
        """Check that the service is reachable and the API key works."""
        self._request("GET", "/api/v1/memory/ping")
        return True

    def key_scope(self) -> str:
        """What this API key may do: ``"full"``, ``"read"`` or ``"admin"``."""
        return _key_scope(self._request("GET", "/api/v1/memory/ping"))

    def list_models(self) -> list[dict]:
        """The available models, each with its capabilities."""
        return _as_records(self._request("GET", "/api/v1/models").get("models"))

    def list_engrams(self) -> dict:
        """The engrams and delivery forms the selected model can run."""
        return _engram_catalog(self._request("GET", "/api/v1/engram"))

    def create_store(self, user_id: Optional[str] = None) -> dict:
        """Create a store. A store must exist before it is written to or searched."""
        return self._post("/api/v1/memory/collection", {"user_id": self._uid(user_id)})

    def list_stores(self) -> list[dict]:
        """List your stores."""
        return _store_list(self._request("GET", "/api/v1/memory/collections"))

    def delete_store(self, user_id: str) -> dict:
        """Delete a store and all its memories."""
        user_id = _named_store(user_id, _DELETE_STORE_NEEDS_ID)
        return self._request("DELETE", "/api/v1/memory/collection", json_body={"user_id": user_id})

    def delete(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *, model: Optional[str] = None) -> dict:
        """Delete one memory by id."""
        return self._post("/api/v1/memory/forget", _id_body(self, user_id, memory_id, _DELETE_NEEDS_ID), model=model)

    def delete_all(self, user_id: str, *, model: Optional[str] = None) -> dict:
        """Delete all memories in a store; it also clears the store's time zone and speakers.
        On Scroll 2, set them again with ``set_settings`` and ``add_speaker`` before the next write."""
        user_id = _named_store(user_id, _DELETE_ALL_NEEDS_ID)
        return self._post("/api/v1/memory/forget", {"user_id": user_id}, model=model)


class _AsyncMemoryCalls:
    async def add(self, content: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
            idempotency_key: Optional[str] = None, metadata: Optional[dict] = None,
            image: Optional[dict] = None, tz: Optional[float] = None,
            work_id: Optional[str] = None,
            window: Optional[bool] = None, window_role: Optional[str] = None,
            created_at: Optional[Union[str, datetime]] = None,
            **extra: Any) -> dict:
        """Store one memory. Extra keyword arguments become metadata.
        ``created_at``: when it was said, a datetime with a time zone or RFC 3339 with an offset (from Scroll 2 on)."""
        body = _add_body(self, content, user_id, metadata, extra, image, tz, work_id, window, window_role, created_at)
        return await self._write("/api/v1/memory/store", body, model=model, idempotency_key=idempotency_key)

    store = add

    async def add_turn(
        self, user_msg: str = "", assistant_msg: str = "", user_id: Optional[str] = None, *,
        model: Optional[str] = None, idempotency_key: Optional[str] = None,
        tz: Optional[float] = None, work_id: Optional[str] = None,
        created_at: Optional[Union[str, datetime]] = None,
        user_speaker: Optional[str] = None, assistant_speaker: Optional[str] = None,
    ) -> dict:
        """Store a conversation turn: the person's message and the assistant's reply."""
        body = _turn_body({"user_id": self._uid(user_id), "user_msg": user_msg, "assistant_msg": assistant_msg},
                          created_at, user_speaker, assistant_speaker)
        return await self._write("/api/v1/memory/store-turn", _write_extras(body, tz, self._tz, work_id),
                                 model=model, idempotency_key=idempotency_key)

    async def add_bulk(
        self, content: Optional[str] = None, user_id: Optional[str] = None,
        category: Optional[str] = "general", timestamp: Optional[str] = None, *,
        segments: Optional[Sequence[Mapping[str, Any]]] = None, speaker: Optional[str] = None,
        model: Optional[str] = None, idempotency_key: Optional[str] = None,
    ) -> dict:
        """Bulk-ingest a large blob of text in one call."""
        body = _bulk_body(self._uid(user_id), content, category, timestamp, segments, speaker)
        return await self._write("/api/v1/memory/bulk-store", body, model=model,
                                idempotency_key=idempotency_key)

    async def update(
        self, old_memory_id: str = "", new_content: str = "", user_id: Optional[str] = None, *,
        model: Optional[str] = None, idempotency_key: Optional[str] = None,
        tz: Optional[float] = None, work_id: Optional[str] = None,
    ) -> dict:
        """Supersede an old memory with new content."""
        return await self._write("/api/v1/memory/supersede",
                                 _supersede_body(self, user_id, old_memory_id, new_content, tz, work_id),
                                 model=model, idempotency_key=idempotency_key)

    async def search(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> list[dict]:
        """Search a store's memories."""
        body = _default_tz(_search_body(self._uid(user_id), query, limit, opts, verify, max_images), self._tz)
        return _merge_results(await self._post("/api/v1/memory/search", body, model=model), bool(body.get("filters")))

    async def search_full(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> dict:
        """Search, and keep every field the answer came with."""
        body = _default_tz(_search_body(self._uid(user_id), query, limit, opts, verify, max_images), self._tz)
        return _full_search(await self._post("/api/v1/memory/search", body, model=model))

    async def search_self(
        self, query: str, user_id: Optional[str] = None, limit: Optional[int] = 10, *,
        verify: Optional[int] = None, max_images: Optional[int] = None,
        model: Optional[str] = None, **opts: Any
    ) -> dict:
        """Search, with the assistant's own words kept apart from the other memories."""
        body = _default_tz(_search_body(self._uid(user_id), query, limit, opts, verify, max_images), self._tz)
        return _self_search(await self._post("/api/v1/memory/search", body, model=model))

    async def recall(
        self, query: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
        form: Optional[str] = None, tz: Optional[float] = None,
        limit: Optional[int] = None, context_limit: Optional[int] = None,
        rooms: Optional[Union[str, bool, Sequence[str]]] = None, room: Optional[str] = None,
        room_limit: Optional[int] = None, room_expand_tokens: Optional[int] = None,
        long_term: Optional[bool] = None, long_term_cutoff: Optional[bool] = None,
        today_start: Optional[str] = None, now: Optional[str] = None,
        short_term_limit: Optional[int] = None, short_term_budget_tokens: Optional[int] = None,
        max_images: Optional[int] = None, filters: Optional[dict] = None,
        speaker: Optional[str] = None, include_superseded: Optional[bool] = None,
    ) -> dict:
        """One-call context for an LLM: recent turns, long-term memories and the context around them."""
        body = _recall_body(self._uid(user_id), query, form, tz, limit, context_limit, rooms, self._tz,
                            room=room, room_limit=room_limit, room_expand_tokens=room_expand_tokens,
                            long_term=long_term, long_term_cutoff=long_term_cutoff,
                            today_start=today_start, now=now, short_term_limit=short_term_limit,
                            short_term_budget_tokens=short_term_budget_tokens, max_images=max_images,
                            filters=filters, speaker=speaker, include_superseded=include_superseded)
        return _recall_shape(await self._post("/api/v1/memory/recall", body, model=model))

    async def engram(
        self, name: str, query: str, user_id: Optional[str] = None, *, model: Optional[str] = None,
        form: Optional[str] = None, tz: Optional[float] = None,
    ) -> dict:
        """Run a built-in engram."""
        body = _form_body({"name": _required(name, "name"), "user_id": self._uid(user_id), "query": query}, form,
                          tz if tz is not None else self._tz)
        return await self._post("/api/v1/engram/run", body, model=model)

    async def around(
        self, memory_id: str, user_id: Optional[str] = None, *, from_: Optional[int] = None,
        to: Optional[int] = None, center: Optional[int] = None,
        held: Optional[Sequence[int]] = None,
    ) -> dict:
        """What was said around a memory, counted from it."""
        body = _around_body(self._uid(user_id), memory_id, from_, to, center, held)
        return _with_records(await self._post("/api/v1/memory/around", body), "pieces")

    async def why(self, memory_id: str, user_id: Optional[str] = None) -> dict:
        """How a memory was reached, when it answered a work of three steps or more."""
        memory_id = _require_memory_id(memory_id, "the id a search, recall or work returned")
        return await self._post("/api/v1/memory/why", {"user_id": self._uid(user_id), "memory_id": memory_id})

    async def retrievals(self, user_id: Optional[str] = None, *, limit: Optional[int] = None,
                         before: Optional[str] = None) -> dict:
        """What this store handed out, newest first."""
        body = _retrievals_body(self._uid(user_id), limit, before)
        return _with_records(await self._post("/api/v1/memory/retrievals", body), "entries")

    async def history(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """Recent conversation turns (short-term memory)."""
        return _as_records((await self._post("/api/v1/memory/history", {"user_id": self._uid(user_id)}, model=model)).get("turns"))

    async def seen(self, at: str, user_id: Optional[str] = None) -> dict:
        """The memories a line of the recent conversation had in hand when it was said."""
        body = {"user_id": self._uid(user_id), "at": _required(at, "at")}
        return _with_records(await self._post("/api/v1/memory/seen", body), "seen")

    async def clear_window(self, user_id: Optional[str] = None, *, up_to: Optional[str] = None) -> dict:
        """Empty the recent conversation, or with ``up_to`` only the lines last seen at or before it."""
        body = _clear_window_body(self._uid(user_id), up_to)
        return await self._post("/api/v1/memory/clear-window", body)

    async def stats(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> dict:
        """Memory counts for a store."""
        return await self._post("/api/v1/memory/stats", {"user_id": self._uid(user_id)}, model=model)

    async def get(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *, model: Optional[str] = None) -> dict:
        """Fetch one memory by id. An unknown id raises :class:`NotFoundError`."""
        body = _id_body(self, user_id, memory_id, _GET_NEEDS_ID)
        return _memory_from_get(await self._post("/api/v1/memory/get", body, model=model))

    async def list_memories(
        self, user_id: Optional[str] = None, *, limit: Optional[int] = 100, cursor: Optional[str] = None,
        model: Optional[str] = None
    ) -> dict:
        """One page of a store's memories. Pass ``next_cursor`` back as ``cursor`` for the next page."""
        body = _list_memories_body(self, user_id, limit, cursor)
        return _memory_page(await self._post("/api/v1/memory/list", body, model=model))

    async def iter_memories(self, user_id: Optional[str] = None, *, page_size: Optional[int] = 100,
                            model: Optional[str] = None):
        """Every memory in a store, one at a time, paging as it goes."""
        page_size = _list_size(page_size, "page_size")
        cursor: Optional[str] = None
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = await self.list_memories(user_id, limit=page_size, cursor=cursor, model=model)
            rows = _as_records(page.get("memories"))
            for m in rows:
                yield m
            nxt = page.get("next_cursor")
            if not nxt or _cursor_repeats(nxt, seen, rows):
                return
            cursor = nxt
        raise _truncated(f"stopped after {_MAX_PAGES} pages — the store did not end")

    async def export_memories(self, user_id: Optional[str] = None, *, model: Optional[str] = None) -> list[dict]:
        """All of a store's memories, as a list."""
        return [m async for m in self.iter_memories(user_id, model=model)]

    async def usage(self, days: int = 7) -> dict:
        """What this key has spent, and what is left."""
        return await self._request("GET", _usage_path(days))

    async def revisions(self, user_id: Optional[str] = None, *, include: Optional[str] = None,
                        limit: Optional[int] = None, before: Optional[str] = None,
                        skip_ids: Optional[list] = None, model: Optional[str] = None) -> dict:
        """How many of this store's memories were edited after they were written (storing new ones never edits an old one), out of all of them: 0 means every memory is as it was first stored."""
        body = _revisions_body(self, user_id, include, limit, before, skip_ids)
        return _work_result(await self._post("/api/v1/won/revisions", body, model=model))

    async def lineage(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                      model: Optional[str] = None) -> dict:
        """The edits that followed one memory, oldest first."""
        body = _id_body(self, user_id, memory_id, _LISTED_NEEDS_ID)
        return _with_records(await self._post("/api/v1/memory/lineage", body, model=model), "chain")

    async def by_speaker(self, speaker: str, user_id: Optional[str] = None, *,
                         limit: Optional[int] = None, before: Optional[str] = None,
                         skip_ids: Optional[list] = None, model: Optional[str] = None) -> dict:
        """What one person said, newest first."""
        body = _by_speaker_body(self, speaker, user_id, limit, before, skip_ids)
        return _with_records(await self._post("/api/v1/memory/by-speaker", body, model=model), "memories")

    async def ping(self) -> bool:
        """Check that the service is reachable and the API key works."""
        await self._request("GET", "/api/v1/memory/ping")
        return True

    async def key_scope(self) -> str:
        """What this API key may do: ``"full"``, ``"read"`` or ``"admin"``."""
        return _key_scope(await self._request("GET", "/api/v1/memory/ping"))

    async def list_engrams(self) -> dict:
        """The engrams and delivery forms the selected model can run."""
        return _engram_catalog(await self._request("GET", "/api/v1/engram"))

    async def list_models(self) -> list[dict]:
        """The available models, each with its capabilities."""
        return _as_records((await self._request("GET", "/api/v1/models")).get("models"))

    async def create_store(self, user_id: Optional[str] = None) -> dict:
        """Create a store. A store must exist before it is written to or searched."""
        return await self._post("/api/v1/memory/collection", {"user_id": self._uid(user_id)})

    async def list_stores(self) -> list[dict]:
        """List your stores."""
        return _store_list(await self._request("GET", "/api/v1/memory/collections"))

    async def delete_store(self, user_id: str) -> dict:
        """Delete a store and all its memories."""
        user_id = _named_store(user_id, _DELETE_STORE_NEEDS_ID)
        return await self._request("DELETE", "/api/v1/memory/collection", json_body={"user_id": user_id})

    async def delete(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *, model: Optional[str] = None) -> dict:
        """Delete one memory by id."""
        body = _id_body(self, user_id, memory_id, _DELETE_NEEDS_ID)
        return await self._post("/api/v1/memory/forget", body, model=model)

    async def delete_all(self, user_id: str, *, model: Optional[str] = None) -> dict:
        """Delete all memories in a store; it also clears the store's time zone and speakers.
        On Scroll 2, set them again with ``set_settings`` and ``add_speaker`` before the next write."""
        user_id = _named_store(user_id, _DELETE_ALL_NEEDS_ID)
        return await self._post("/api/v1/memory/forget", {"user_id": user_id}, model=model)
