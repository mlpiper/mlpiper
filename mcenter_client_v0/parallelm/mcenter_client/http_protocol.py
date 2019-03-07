from enum import Enum

URL_SCHEME = ":"
URL_HIER_PART = "//"


class HttpProtocol(Enum):
    http = 1
    https = 2

    @classmethod
    def protocol_prefix(cls, protocol):
        name = HttpProtocol[protocol.name].name
        return name

    @classmethod
    def from_str(cls, protocol_str):
        enum = HttpProtocol[protocol_str]
        return enum
