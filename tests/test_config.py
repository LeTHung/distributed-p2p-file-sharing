import os
import unittest
from unittest.mock import patch

from common.config import load_settings


class SettingsTests(unittest.TestCase):
    def test_invalid_limits_ports_and_ttl(self):
        for values in ({"POSTGRES_PORT": "0"}, {"TRACKER_PORT": "65536"},
                       {"TRACKER_MAX_CLIENTS": "0"}, {"TRACKER_CLEANUP_INTERVAL_SECONDS": "-1"},
                       {"PEER_TTL_SECONDS": "5", "HEARTBEAT_INTERVAL_SECONDS": "5"},
                       {"SOCKET_TIMEOUT_SECONDS": "0"}):
            with self.subTest(values=values), patch.dict(os.environ, values, clear=True), \
                 patch("common.config.load_dotenv"), self.assertRaises(ValueError):
                load_settings()

    def test_password_is_excluded_from_repr(self):
        with patch.dict(os.environ, {"POSTGRES_PASSWORD": "test-value-only"}, clear=True), \
             patch("common.config.load_dotenv"):
            self.assertNotIn("test-value-only", repr(load_settings()))
