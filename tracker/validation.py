"""Validation of the existing Tracker wire contract (no database side effects)."""

from __future__ import annotations

import ipaddress
from typing import Any

from common.metadata import HASH, validate_metadata
from tracker.errors import RequestError

TYPES = {"register_peer", "register_file", "announce_chunks", "get_peers",
         "heartbeat", "unregister_peer", "list_files"}


def validate_request(request: dict[str, Any]) -> dict[str, Any]:
    def fail(message: str) -> None:
        raise RequestError("INVALID_REQUEST", message)

    def text(field: str, limit: int = 256) -> str:
        value = request.get(field)
        if not isinstance(value, str) or not value.strip() or len(value) > limit or "\0" in value:
            fail(f"{field} must be a nonempty string of at most {limit} characters")
        return value

    # IDs are echoed as strings; tolerate the abbreviated IDs in the original examples.
    text("request_id")
    kind = request.get("type")
    if not isinstance(kind, str) or kind not in TYPES:
        fail("Unsupported request type")
    if kind in {"register_peer", "announce_chunks", "heartbeat", "unregister_peer"}:
        text("peer_id")
    elif kind == "get_peers" and "peer_id" in request:
        text("peer_id")
    if kind == "register_peer":
        try:
            address = ipaddress.ip_address(text("host"))
            if address.is_unspecified or address.is_multicast or getattr(address, "scope_id", None):
                fail("host must be a unicast IP address reachable by peers")
        except ValueError:
            fail("host must be an IPv4 or IPv6 address (PostgreSQL INET)")
        port = request.get("port")
        if type(port) is not int or not 1 <= port <= 65535:
            fail("port must be an integer between 1 and 65535")
        request = {**request, "host": str(address)}
    if kind in {"register_file", "announce_chunks", "get_peers"}:
        if not HASH.fullmatch(text("file_id")):
            fail("file_id must be a lowercase SHA-256 hex string")
    if kind == "register_file":
        try:
            validate_metadata(request)
        except ValueError as error:
            fail(str(error))
    if kind == "announce_chunks" or (kind == "get_peers" and "chunk_indexes" in request):
        indexes = request.get("chunk_indexes")
        if not isinstance(indexes, list) or any(type(i) is not int or not 0 <= i < 2**31 for i in indexes):
            fail("chunk_indexes must be a list of nonnegative integers")
        # Duplicates are harmless and must not produce duplicate directory entries.
        request = {**request, "chunk_indexes": sorted(set(indexes))}
    return request
