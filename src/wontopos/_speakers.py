from __future__ import annotations

from collections.abc import Mapping
from typing import Optional, Sequence, Union

from . import _forget_speaker_body, _relabel_body, _required, _settings_body, _turn_speaker, _with_records


class _SpeakerCalls:
    def settings(self, user_id: Optional[str] = None) -> dict:
        """The store's settings: its time zone, its speakers and every change to them."""
        return self._request("GET", "/api/v1/memory/settings", params={"user_id": self._uid(user_id)})

    def set_settings(
        self, user_id: Optional[str] = None, *, time_zone: Optional[str] = None,
        default_speakers: Optional[Mapping[str, Optional[str]]] = None,
        confirmed_by_user: Optional[bool] = None,
    ) -> dict:
        """Set the store's time zone, its default speakers, or both."""
        body = _settings_body(self._uid(user_id), time_zone, default_speakers, confirmed_by_user)
        return self._post("/api/v1/memory/settings", body)

    def add_speaker(self, speaker: str, user_id: Optional[str] = None) -> dict:
        """Register a person for this store, before storing what they said."""
        speaker = _turn_speaker(speaker, "speaker")
        return self._post("/api/v1/memory/speakers", {"user_id": self._uid(user_id), "speaker": speaker})

    def list_speakers(self, user_id: Optional[str] = None) -> dict:
        """The store's registered people, each with its memory count."""
        r = self._request("GET", "/api/v1/memory/speakers", params={"user_id": self._uid(user_id)})
        return _with_records(r, "speakers")

    def remove_speaker(self, speaker: str, user_id: Optional[str] = None) -> dict:
        """Unregister a person; their memories stay."""
        speaker = _required(speaker, "speaker")
        return self._request(
            "DELETE", "/api/v1/memory/speakers", json_body={"user_id": self._uid(user_id), "speaker": speaker}
        )

    def relabel_speaker(self, from_speaker: str, to_speaker: str, user_id: Optional[str] = None, *,
                        memory_ids: Optional[Sequence[str]] = None) -> dict:
        """Move memories from one speaker to another."""
        body = _relabel_body(self._uid(user_id), from_speaker, to_speaker, memory_ids)
        return self._until_complete("/api/v1/memory/speakers/relabel", body)

    def forget_speaker(self, speaker: Union[str, Sequence[str]], user_id: str, *,
                       preview: bool = False) -> dict:
        """Delete everything a person said in this store."""
        body = _forget_speaker_body(speaker, user_id, preview)
        if preview:
            return self._post("/api/v1/memory/forget", body)
        return self._until_complete("/api/v1/memory/forget", body)


class _AsyncSpeakerCalls:
    async def settings(self, user_id: Optional[str] = None) -> dict:
        """The store's settings: its time zone, its speakers and every change to them."""
        return await self._request("GET", "/api/v1/memory/settings", params={"user_id": self._uid(user_id)})

    async def set_settings(
        self, user_id: Optional[str] = None, *, time_zone: Optional[str] = None,
        default_speakers: Optional[Mapping[str, Optional[str]]] = None,
        confirmed_by_user: Optional[bool] = None,
    ) -> dict:
        """Set the store's time zone, its default speakers, or both."""
        body = _settings_body(self._uid(user_id), time_zone, default_speakers, confirmed_by_user)
        return await self._post("/api/v1/memory/settings", body)

    async def add_speaker(self, speaker: str, user_id: Optional[str] = None) -> dict:
        """Register a person for this store, before storing what they said."""
        speaker = _turn_speaker(speaker, "speaker")
        return await self._post("/api/v1/memory/speakers", {"user_id": self._uid(user_id), "speaker": speaker})

    async def list_speakers(self, user_id: Optional[str] = None) -> dict:
        """The store's registered people, each with its memory count."""
        r = await self._request("GET", "/api/v1/memory/speakers", params={"user_id": self._uid(user_id)})
        return _with_records(r, "speakers")

    async def remove_speaker(self, speaker: str, user_id: Optional[str] = None) -> dict:
        """Unregister a person; their memories stay."""
        speaker = _required(speaker, "speaker")
        return await self._request(
            "DELETE", "/api/v1/memory/speakers", json_body={"user_id": self._uid(user_id), "speaker": speaker}
        )

    async def relabel_speaker(self, from_speaker: str, to_speaker: str, user_id: Optional[str] = None, *,
                              memory_ids: Optional[Sequence[str]] = None) -> dict:
        """Move memories from one speaker to another."""
        body = _relabel_body(self._uid(user_id), from_speaker, to_speaker, memory_ids)
        return await self._until_complete("/api/v1/memory/speakers/relabel", body)

    async def forget_speaker(self, speaker: Union[str, Sequence[str]], user_id: str, *,
                             preview: bool = False) -> dict:
        """Delete everything a person said in this store."""
        body = _forget_speaker_body(speaker, user_id, preview)
        if preview:
            return await self._post("/api/v1/memory/forget", body)
        return await self._until_complete("/api/v1/memory/forget", body)
