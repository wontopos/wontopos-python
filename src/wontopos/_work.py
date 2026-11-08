from __future__ import annotations

from typing import Any, Optional, Sequence, Union

from . import (
    _check_flag, _close_work_body, _look, _open_work_body, _required, _with_records, _work_around_body, _work_result,
)


def _work_search_body(client: Any, work_id: str, query: str, user_id: Optional[str], rooms: Any,
                      tz: Optional[float]) -> dict:
    body = {"user_id": client._uid(user_id), "work_id": _required(work_id, "work_id"),
            "query": _required(query, "query")}
    return _look(body, rooms, tz, client._tz)


def _work_confirm_body(client: Any, work_id: str, confirmed: Any, user_id: Optional[str]) -> dict:
    confirmed = _check_flag(confirmed, "confirmed")
    return {"user_id": client._uid(user_id), "work_id": _required(work_id, "work_id"), "confirmed": confirmed}


class _WorkCalls:
    def _work(self, op: str, body: dict) -> dict:
        return _work_result(self._post(f"/api/v1/memory/work/{op}", body))

    def open_work(
        self, question: str, user_id: Optional[str] = None, *,
        rooms: Optional[Union[str, bool, Sequence[str]]] = None, tz: Optional[float] = None,
        max_images: Optional[int] = None, own_words: Optional[bool] = None, diversity: Optional[str] = None,
    ) -> dict:
        """Open a working memory for one question.
        ``own_words``: whether this work may bring the assistant's own lines (default on).
        ``diversity``: the diversity this work asks for, "none", "mmr", "temporal" or "full" (default unset: none is
        sent)."""
        return self._work("open", _open_work_body(self._uid(user_id), question, rooms, tz, self._tz, max_images,
                                                  own_words, diversity))

    def work_search(
        self, work_id: str, query: str, user_id: Optional[str] = None, *,
        rooms: Optional[Union[str, bool, Sequence[str]]] = None, tz: Optional[float] = None,
    ) -> dict:
        """A step: search again with a question of your own."""
        return self._work("search", _work_search_body(self, work_id, query, user_id, rooms, tz))

    def work_deeper(self, work_id: str, user_id: Optional[str] = None) -> dict:
        """The next 20 memories for the work's question, at most three times."""
        return self._work("deeper", {"user_id": self._uid(user_id), "work_id": _required(work_id, "work_id")})

    def work_around(
        self, work_id: str, n: int, user_id: Optional[str] = None, *,
        from_: Optional[int] = None, to: Optional[int] = None, center: Optional[int] = None,
    ) -> dict:
        """A step: what was said around memory [n] that this work does not have yet."""
        body = _work_around_body(self._uid(user_id), work_id, n, from_, to, center)
        return _with_records(self._work("around", body), "pieces")

    def work_ask(self, work_id: str, user_id: Optional[str] = None) -> dict:
        """Mark the work as waiting on the person. Pass their reply to :meth:`work_confirm`."""
        return self._work("ask", {"user_id": self._uid(user_id), "work_id": _required(work_id, "work_id")})

    def work_confirm(self, work_id: str, confirmed: bool, user_id: Optional[str] = None) -> dict:
        """Answer a work waiting on the person with their yes or no."""
        return self._work("confirm", _work_confirm_body(self, work_id, confirmed, user_id))

    def close_work(self, work_id: str, user_id: Optional[str] = None, *,
                   answer_id: Optional[str] = None) -> dict:
        """Close a work, with the memory that answered if one did."""
        return self._work("close", _close_work_body(self._uid(user_id), work_id, answer_id))

    def work_state(self, user_id: Optional[str] = None) -> dict:
        """The open works on this store, and the traces of recent ones."""
        return self._post("/api/v1/memory/work/state", {"user_id": self._uid(user_id)})


class _AsyncWorkCalls:
    async def _work(self, op: str, body: dict) -> dict:
        return _work_result(await self._post(f"/api/v1/memory/work/{op}", body))

    async def open_work(
        self, question: str, user_id: Optional[str] = None, *,
        rooms: Optional[Union[str, bool, Sequence[str]]] = None, tz: Optional[float] = None,
        max_images: Optional[int] = None, own_words: Optional[bool] = None, diversity: Optional[str] = None,
    ) -> dict:
        """Open a working memory for one question.
        ``own_words``: whether this work may bring the assistant's own lines (default on).
        ``diversity``: the diversity this work asks for, "none", "mmr", "temporal" or "full" (default unset: none is
        sent)."""
        return await self._work("open", _open_work_body(self._uid(user_id), question, rooms, tz, self._tz,
                                                        max_images, own_words, diversity))

    async def work_search(
        self, work_id: str, query: str, user_id: Optional[str] = None, *,
        rooms: Optional[Union[str, bool, Sequence[str]]] = None, tz: Optional[float] = None,
    ) -> dict:
        """A step: search again with a question of your own."""
        return await self._work("search", _work_search_body(self, work_id, query, user_id, rooms, tz))

    async def work_deeper(self, work_id: str, user_id: Optional[str] = None) -> dict:
        """The next 20 memories for the work's question, at most three times."""
        return await self._work("deeper", {"user_id": self._uid(user_id), "work_id": _required(work_id, "work_id")})

    async def work_around(
        self, work_id: str, n: int, user_id: Optional[str] = None, *,
        from_: Optional[int] = None, to: Optional[int] = None, center: Optional[int] = None,
    ) -> dict:
        """A step: what was said around memory [n] that this work does not have yet."""
        body = _work_around_body(self._uid(user_id), work_id, n, from_, to, center)
        return _with_records(await self._work("around", body), "pieces")

    async def work_ask(self, work_id: str, user_id: Optional[str] = None) -> dict:
        """Mark the work as waiting on the person. Pass their reply to :meth:`work_confirm`."""
        return await self._work("ask", {"user_id": self._uid(user_id), "work_id": _required(work_id, "work_id")})

    async def work_confirm(self, work_id: str, confirmed: bool, user_id: Optional[str] = None) -> dict:
        """Answer a work waiting on the person with their yes or no."""
        return await self._work("confirm", _work_confirm_body(self, work_id, confirmed, user_id))

    async def close_work(self, work_id: str, user_id: Optional[str] = None, *,
                         answer_id: Optional[str] = None) -> dict:
        """Close a work, with the memory that answered if one did."""
        return await self._work("close", _close_work_body(self._uid(user_id), work_id, answer_id))

    async def work_state(self, user_id: Optional[str] = None) -> dict:
        """The open works on this store, and the traces of recent ones."""
        return await self._post("/api/v1/memory/work/state", {"user_id": self._uid(user_id)})
