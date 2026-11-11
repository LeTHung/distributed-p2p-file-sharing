"""TCP framing shared by Tracker and Peer processes.

Every JSON control message is prefixed by a four-byte unsigned big-endian
length. Binary chunk data is sent only after a successful get_chunk response.
"""

from __future__ import annotations

import json
import socket
import struct
import time
from typing import Any


MAX_JSON_MESSAGE_BYTES = 1_048_576


class ProtocolError(Exception):
    """Raised when a socket stream violates the shared control-message format."""


def recv_exact(sock: socket.socket, byte_count: int, *, deadline: float | None = None) -> bytes:
    """Receive exactly byte_count bytes or raise EOFError when the peer closes."""
    if byte_count < 0:
        raise ValueError("byte_count must not be negative")
    data = bytearray()
    while len(data) < byte_count:
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("message receive deadline exceeded")
            sock.settimeout(remaining)
        chunk = sock.recv(byte_count - len(data))
        if not chunk:
            raise EOFError("peer closed socket before the message was complete")
        data.extend(chunk)
    return bytes(data)


def send_json(sock: socket.socket, payload: dict[str, Any]) -> None:
    """Serialize and send one control message using the agreed length prefix."""
    if not isinstance(payload, dict):
        raise ProtocolError("control message must be a JSON object")
    try:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError) as error:
        raise ProtocolError("cannot encode JSON control message") from error
    if len(encoded) > MAX_JSON_MESSAGE_BYTES:
        raise ProtocolError("control message exceeds maximum allowed size")
    sock.sendall(struct.pack("!I", len(encoded)) + encoded)


def recv_json(sock: socket.socket, *, timeout: float | None = None) -> dict[str, Any]:
    """Read one framed JSON object and reject malformed or oversized input."""
    deadline = None if timeout is None else time.monotonic() + timeout
    message_length = struct.unpack("!I", recv_exact(sock, 4, deadline=deadline))[0]
    if message_length == 0 or message_length > MAX_JSON_MESSAGE_BYTES:
        raise ProtocolError("received control message exceeds maximum allowed size")
    try:
        payload = json.loads(recv_exact(sock, message_length, deadline=deadline).decode("utf-8"),
                             parse_constant=_invalid_constant)
    except (UnicodeError, ValueError, RecursionError) as error:
        raise ProtocolError("received invalid JSON control message") from error
    if not isinstance(payload, dict):
        raise ProtocolError("control message must be a JSON object")
    return payload


def _invalid_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant: {value}")
