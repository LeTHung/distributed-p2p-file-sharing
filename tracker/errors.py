"""Errors that can safely be returned to a protocol client."""


class RequestError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code

