"""TCP framing shared by Tracker and Peer processes.

Every JSON control message is prefixed by a four-byte unsigned big-endian
length. Binary chunk data is sent only after a successful get_chunk response.
"""

from __future__ import annotations

import json
import socket
import struct
from typing import Any


MAX_JSON_MESSAGE_BYTES = 1_048_576


class ProtocolError(Exception):
    """Raised when a socket stream violates the shared control-message format."""


def recv_exact(sock: socket.socket, byte_count: int) -> bytes:
    """Receive exactly byte_count bytes or raise EOFError when the peer closes."""
    if byte_count < 0:
        raise ValueError("byte_count must not be negative")
    data = bytearray()
    while len(data) < byte_count:
        chunk = sock.recv(byte_count - len(data))
        if not chunk:
            raise EOFError("peer closed socket before the message was complete")
        data.extend(chunk)
    return bytes(data)


def send_json(sock: socket.socket, payload: dict[str, Any]) -> None:
    """Serialize and send one control message using the agreed length prefix."""
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_JSON_MESSAGE_BYTES:
        raise ProtocolError("control message exceeds maximum allowed size")
    sock.sendall(struct.pack("!I", len(encoded)) + encoded)


def recv_json(sock: socket.socket) -> dict[str, Any]:
    """Read one framed JSON object and reject malformed or oversized input."""
    message_length = struct.unpack("!I", recv_exact(sock, 4))[0]
    if message_length > MAX_JSON_MESSAGE_BYTES:
        raise ProtocolError("received control message exceeds maximum allowed size")
    try:
        payload = json.loads(recv_exact(sock, message_length).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError("received invalid JSON control message") from error
    if not isinstance(payload, dict):
        raise ProtocolError("control message must be a JSON object")
    return payload
