from __future__ import annotations

from typing import Any, Optional

from . import (
    WosError, _IMAGE_NEEDS_ID, _MAX_PAGES, _NO_ID, _UNIMAGE_NEEDS_ID, _as_records, _check_flag, _check_page,
    _cursor_repeats, _id_body, _page_fields, _truncated, _with_records,
)


def _forget_image_body(client: Any, user_id: Optional[str], memory_id: Any, preview: bool) -> dict:
    _check_flag(preview, "preview")
    body = _id_body(client, user_id, memory_id, _UNIMAGE_NEEDS_ID)
    if preview:
        body["preview"] = True
    return body


def _image_bytes(status: int, headers: Any, data: bytes) -> tuple[bytes, str]:
    if not data:
        raise WosError(status, "empty image body — the service returned no bytes")
    return data, headers.get("content-type", "application/octet-stream")


class _ImageCalls:
    def export_images(self, user_id: Optional[str] = None, *, page_size: Optional[int] = None,
                      model: Optional[str] = None) -> list[dict]:
        """All of a store's image memories, as a list."""
        return list(self.iter_images(user_id, page_size=page_size, model=model))

    def get_image(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                  model: Optional[str] = None) -> tuple[bytes, str]:
        """Fetch the bytes of an image memory."""
        return self._request_bytes("/api/v1/memory/image", _id_body(self, user_id, memory_id, _IMAGE_NEEDS_ID),
                                   model=model)

    def forget_image(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                     preview: bool = False, model: Optional[str] = None) -> dict:
        """Remove the photo from a memory; it can take the whole memory with it."""
        body = _forget_image_body(self, user_id, memory_id, preview)
        return self._request("DELETE", "/api/v1/memory/image", json_body=body, model=model, removes=not preview)

    def list_images(self, user_id: Optional[str] = None, *, limit: Optional[int] = None,
                    before: Optional[str] = None, skip_ids: Optional[list] = None,
                    model: Optional[str] = None) -> dict:
        """One page of image memories, newest first, with the store's total count."""
        page = _page_fields(limit, before, skip_ids)
        body = {"user_id": self._uid(user_id), **page}
        return _with_records(self._post("/api/v1/memory/images", body, model=model), "images")

    def iter_images(self, user_id: Optional[str] = None, *, page_size: Optional[int] = None,
                    model: Optional[str] = None):
        """Every image memory in a store, one at a time, paging as it goes."""
        if page_size is not None:
            _check_page(page_size, "page_size")
        before: Optional[str] = None
        skip: Optional[list] = None
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = self.list_images(user_id, limit=page_size, before=before, skip_ids=skip, model=model)
            rows = _as_records(page.get("images"))
            for m in rows:
                yield m
            if not page.get("has_more") or not page.get("next_before"):
                return
            before = page.get("next_before")
            nxt = page.get("next_skip_ids")
            skip = nxt if isinstance(nxt, list) else None
            if _cursor_repeats((before, tuple(skip) if skip else ()), seen, rows):
                return
        raise _truncated(f"stopped after {_MAX_PAGES} pages — the store did not end")


class _AsyncImageCalls:
    async def export_images(self, user_id: Optional[str] = None, *, page_size: Optional[int] = None,
                            model: Optional[str] = None) -> list[dict]:
        """All of a store's image memories, as a list."""
        return [m async for m in self.iter_images(user_id, page_size=page_size, model=model)]

    async def get_image(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                        model: Optional[str] = None) -> tuple[bytes, str]:
        """Fetch the bytes of an image memory."""
        return await self._request_bytes("/api/v1/memory/image", _id_body(self, user_id, memory_id, _IMAGE_NEEDS_ID),
                                         model=model)

    async def forget_image(self, user_id: Optional[str] = None, memory_id: str = _NO_ID, *,
                           preview: bool = False, model: Optional[str] = None) -> dict:
        """Remove the photo from a memory; it can take the whole memory with it."""
        body = _forget_image_body(self, user_id, memory_id, preview)
        return await self._request("DELETE", "/api/v1/memory/image", json_body=body, model=model, removes=not preview)

    async def list_images(self, user_id: Optional[str] = None, *, limit: Optional[int] = None,
                          before: Optional[str] = None, skip_ids: Optional[list] = None,
                          model: Optional[str] = None) -> dict:
        """One page of image memories, newest first, with the store's total count."""
        page = _page_fields(limit, before, skip_ids)
        body = {"user_id": self._uid(user_id), **page}
        return _with_records(await self._post("/api/v1/memory/images", body, model=model), "images")

    async def iter_images(self, user_id: Optional[str] = None, *, page_size: Optional[int] = None,
                          model: Optional[str] = None):
        """Every image memory in a store, one at a time, paging as it goes."""
        if page_size is not None:
            _check_page(page_size, "page_size")
        before: Optional[str] = None
        skip: Optional[list] = None
        seen: set = set()
        for _ in range(_MAX_PAGES):
            page = await self.list_images(user_id, limit=page_size, before=before, skip_ids=skip, model=model)
            rows = _as_records(page.get("images"))
            for m in rows:
                yield m
            if not page.get("has_more") or not page.get("next_before"):
                return
            before = page.get("next_before")
            nxt = page.get("next_skip_ids")
            skip = nxt if isinstance(nxt, list) else None
            if _cursor_repeats((before, tuple(skip) if skip else ()), seen, rows):
                return
        raise _truncated(f"stopped after {_MAX_PAGES} pages — the store did not end")
