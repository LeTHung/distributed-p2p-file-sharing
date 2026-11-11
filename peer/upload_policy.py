"""Conservative aggregate payload pacing shared by every Seeder worker."""

import threading


class UploadPolicy:
    """Limit total binary bytes per second; JSON control frames are excluded.

    Each block is paced before sending. A shared lock covers wait + send so
    concurrent workers cannot each consume the whole configured bandwidth.
    There is no startup burst or catch-up credit after an idle/slow socket.
    """

    def __init__(self, bytes_per_second=0, *, stopping=None):
        if type(bytes_per_second) is not int or bytes_per_second < 0:
            raise ValueError("upload rate must be a nonnegative integer (bytes/second)")
        self.rate = bytes_per_second
        self.stopping = stopping if stopping is not None else threading.Event()
        self._lock = threading.Lock()

    def send(self, sock, data):
        block_size = min(64 * 1024, max(1, self.rate // 10)) if self.rate else 64 * 1024
        for offset in range(0, len(data), block_size):
            block = memoryview(data)[offset:offset + block_size]
            if self.rate:
                with self._lock:
                    if self.stopping.wait(len(block) / self.rate):
                        raise OSError("Seeder upload stopped")
                    sock.sendall(block)
            else:
                if self.stopping.is_set():
                    raise OSError("Seeder upload stopped")
                sock.sendall(block)
