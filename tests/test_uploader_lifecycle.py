import threading
import unittest
from unittest.mock import Mock

from common.framing import ProtocolError
from peer.uploader_lifecycle import SeederRegistration


class LifecycleTests(unittest.TestCase):
    def make_registration(self, rpc):
        shared = Mock(file_id="a" * 64, chunks=[{}, {}])
        shared.metadata.return_value = {"file_id": shared.file_id}
        return SeederRegistration(rpc, shared, "seed", "127.0.0.1", 7001, 0.01)

    def test_heartbeat_stops_before_unregister(self):
        heartbeated = threading.Event()
        calls = []

        def rpc(payload):
            calls.append(payload)
            if payload["type"] == "heartbeat":
                heartbeated.set()

        registration = self.make_registration(rpc)
        with registration:
            self.assertTrue(heartbeated.wait(2))
        self.assertFalse(registration.thread.is_alive())
        self.assertEqual([p["type"] for p in calls[:3]],
                         ["register_peer", "register_file", "announce_chunks"])
        self.assertEqual(calls[2]["chunk_indexes"], [0, 1])
        self.assertEqual(calls[-1]["type"], "unregister_peer")

    def test_expiry_or_disconnect_registers_and_announces_again(self):
        recovered = threading.Event()
        calls = []

        def rpc(payload):
            calls.append(payload["type"])
            if payload["type"] == "heartbeat" and not recovered.is_set():
                raise ProtocolError("peer expired")
            if calls.count("announce_chunks") == 2:
                recovered.set()

        with self.make_registration(rpc):
            self.assertTrue(recovered.wait(2))
        self.assertEqual(calls[:7], ["register_peer", "register_file", "announce_chunks",
                                     "heartbeat", "register_peer", "register_file", "announce_chunks"])

    def test_partial_registration_is_cleaned_up(self):
        rpc = Mock(side_effect=[{}, ProtocolError("file rejected"), {}])
        with self.assertRaises(ProtocolError):
            with self.make_registration(rpc):
                self.fail("Must not start serving after failed registration")
        self.assertEqual(rpc.call_args.args[0]["type"], "unregister_peer")

    def test_tracker_down_during_shutdown_does_not_mask_original_error(self):
        def rpc(payload):
            if payload["type"] == "unregister_peer":
                raise OSError("tracker down")

        with self.assertRaisesRegex(ValueError, "original"):
            with self.make_registration(rpc):
                raise ValueError("original")
