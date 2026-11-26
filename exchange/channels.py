"""Small channel adapter contract and in-memory implementation.

Real chain integrations can implement the same two methods with HTTP/signing,
retries and their own idempotency keys.  The core service never treats a
receipt as evidence that can rewrite a product association.
"""
from __future__ import annotations

from typing import Any


class ChannelGateway:
    name = "abstract"

    def fetch_listings(self, channel: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def publish_listing(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


class MemoryChannelGateway(ChannelGateway):
    def __init__(self, listings: dict[str, list[dict[str, Any]]] | None = None) -> None:
        self.listings = listings or {}
        self.published: list[dict[str, Any]] = []

    def seed(self, channel: str, records: list[dict[str, Any]]) -> None:
        self.listings.setdefault(channel, []).extend(records)

    def fetch_listings(self, channel: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self.listings.get(channel, [])]

    def publish_listing(self, payload: dict[str, Any]) -> dict[str, Any]:
        message = dict(payload)
        self.published.append(message)
        return {"status": "acknowledged", "channel": payload["channel"], "push": payload["push"]}
