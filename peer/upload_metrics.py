"""Thread-safe counters for explaining and measuring the Seeder worker pool."""

import threading


class UploadMetrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._values = dict(accepted_connections=0, rejected_connections=0,
                            finished_connections=0, active_connections=0, in_flight=0,
                            peak_active_connections=0, peak_in_flight=0, successful_chunks=0,
                            completed_payload_bytes=0, error_responses=0, connection_errors=0,
                            cancelled_connections=0)

    def accepted(self):
        with self._lock:
            self._values["accepted_connections"] += 1
            self._values["in_flight"] += 1
            self._values["peak_in_flight"] = max(self._values["peak_in_flight"], self._values["in_flight"])

    def started(self):
        with self._lock:
            self._values["active_connections"] += 1
            self._values["peak_active_connections"] = max(
                self._values["peak_active_connections"], self._values["active_connections"])

    def finished(self, active):
        with self._lock:
            self._values["finished_connections"] += 1
            self._values["in_flight"] -= 1
            if active:
                self._values["active_connections"] -= 1

    def increment(self, key):
        with self._lock:
            self._values[key] += 1

    def sent(self, byte_count):
        with self._lock:
            self._values["successful_chunks"] += 1
            self._values["completed_payload_bytes"] += byte_count

    def snapshot(self):
        with self._lock:
            return dict(self._values)
