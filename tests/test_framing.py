import socket
import unittest

from common.framing import ProtocolError, recv_json, send_json


class FramingTests(unittest.TestCase):
    def test_round_trip_json_message(self) -> None:
        sender, receiver = socket.socketpair()
        try:
            payload = {"type": "heartbeat", "peer_id": "peer-a", "message": "Đang hoạt động"}
            send_json(sender, payload)
            self.assertEqual(recv_json(receiver), payload)
        finally:
            sender.close()
            receiver.close()

    def test_rejects_non_object_payload(self) -> None:
        sender, receiver = socket.socketpair()
        try:
            sender.sendall(b"\x00\x00\x00\x02[]")
            with self.assertRaisesRegex(ProtocolError, "JSON object"):
                recv_json(receiver)
        finally:
            sender.close()
            receiver.close()
