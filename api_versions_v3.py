from __future__ import annotations

import socket
from dataclasses import dataclass
from io import BytesIO
from pprint import pprint
from typing import BinaryIO

from raw_tagged_fields import (
    RawTaggedField,
    write_unknown_tagged_fields,
    read_unknown_tagged_fields,
)
from read_write import (
    read_int16,
    write_int32,
    read_int32,
    write_string,
    read_array,
)
from request_response_headers import RequestHeaderV2, ResponseHeaderV0


@dataclass
class ApiVersionsRequestV3:
    client_software_name: str
    client_software_version: str
    _unknownTaggedFields: list[RawTaggedField]

    def write(self, buffer: BinaryIO) -> None:
        write_string(self.client_software_name, buffer, True)
        write_string(self.client_software_version, buffer, True)
        write_unknown_tagged_fields(self._unknownTaggedFields, buffer)


@dataclass
class ApiVersionsResponseApiKeyV3:
    api_key: int
    min_version: int
    max_version: int
    _unknownTaggedFields: list[RawTaggedField]

    @classmethod
    def read(cls, buffer: BinaryIO) -> ApiVersionsResponseApiKeyV3:
        return ApiVersionsResponseApiKeyV3(
            api_key=read_int16(buffer),
            min_version=read_int16(buffer),
            max_version=read_int16(buffer),
            _unknownTaggedFields=read_unknown_tagged_fields(buffer),
        )


@dataclass
class ApiVersionsResponseV3:
    error_code: int
    api_keys: list[ApiVersionsResponseApiKeyV3]
    throttle_time_ms: int
    _unknownTaggedFields: list[RawTaggedField]

    @classmethod
    def read(cls, buffer: BinaryIO) -> ApiVersionsResponseV3:
        return ApiVersionsResponseV3(
            error_code=read_int16(buffer),
            api_keys=read_array(ApiVersionsResponseApiKeyV3.read, buffer, True),
            throttle_time_ms=read_int32(buffer),
            _unknownTaggedFields=read_unknown_tagged_fields(buffer),
        )


def send_request(request_correlation_id: int, sock: socket.socket) -> None:
    buffer = BytesIO()

    # message_size
    # will be filled later
    write_int32(0, buffer)

    header = RequestHeaderV2(
        request_api_key=18,
        request_api_version=3,
        correlation_id=request_correlation_id,
        client_id="test-client",
        _unknownTaggedFields=[],
    )
    header.write(buffer)

    message = ApiVersionsRequestV3(
        client_software_name="test-client",
        client_software_version="1",
        _unknownTaggedFields=[],
    )
    message.write(buffer)

    # Now we know the message size.
    # Return to the beginning of the buffer and put it there.
    request_message_size = buffer.tell() - 4
    buffer.seek(0)
    write_int32(request_message_size, buffer)

    sock.sendall(buffer.getvalue())


def receive_response(request_correlation_id: int, sock: socket.socket) -> None:
    # The first 4 bytes is the length.
    message_size = read_int32(BytesIO(sock.recv(4)))
    buffer = BytesIO(sock.recv(message_size))

    header = ResponseHeaderV0.read(buffer)
    if header.correlation_id != request_correlation_id:
        raise ValueError()
    pprint(header)

    message = ApiVersionsResponseV3.read(buffer)
    pprint(message)


def main() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 9092))

    request_correlation_id = 123
    send_request(request_correlation_id, sock)

    receive_response(request_correlation_id, sock)

    sock.close()


if __name__ == "__main__":
    main()
