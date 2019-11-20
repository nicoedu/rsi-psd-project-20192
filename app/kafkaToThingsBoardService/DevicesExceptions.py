#!/usr/bin/env python3
class httpException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return "Error code: %s with message: %s" % (str(self.code), self.message)


class UnauthorizedException(Exception):
    pass


class DeviceNotFound(Exception):
    pass
