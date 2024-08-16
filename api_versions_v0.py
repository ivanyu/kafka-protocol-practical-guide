from __future__ import annotations

import socket
from dataclasses import dataclass
from io import BytesIO
from pprint import pprint
from typing import BinaryIO

from read_write import (
    read_int16,
    read_array,
    write_int32,
    read_int32,
)
from request_response_headers import RequestHeaderV1, ResponseHeaderV0


@dataclass
class ApiVersionsRequestV0:
    def write(self, buffer: BinaryIO) -> None:
        pass


@dataclass
class ApiVersionsResponseApiKeyV0:
    api_key: int
    min_version: int
    max_version: int

    @classmethod
    def read(cls, buffer: BinaryIO) -> ApiVersionsResponseApiKeyV0:
        return ApiVersionsResponseApiKeyV0(
            api_key=read_int16(buffer),
            min_version=read_int16(buffer),
            max_version=read_int16(buffer),
        )


@dataclass
class ApiVersionsResponseV0:
    error_code: int
    api_keys: list[ApiVersionsResponseApiKeyV0]

    @classmethod
    def read(cls, buffer: BinaryIO) -> ApiVersionsResponseV0:
        return ApiVersionsResponseV0(
            error_code=read_int16(buffer),
            api_keys=read_array(ApiVersionsResponseApiKeyV0.read, buffer, False),
        )


def send_request(request_correlation_id: int, sock: socket.socket) -> None:
    buffer = BytesIO()

    # message_size
    # will be filled later
    write_int32(0, buffer)

    header = RequestHeaderV1(
        request_api_key=18,
        request_api_version=0,
        correlation_id=request_correlation_id,
        client_id="test-client",
    )
    header.write(buffer)

    message = ApiVersionsRequestV0()
    message.write(buffer)

    request_message_size = buffer.tell() - 4
    buffer.seek(0)
    write_int32(request_message_size, buffer)

    sock.sendall(buffer.getvalue())


def receive_response(request_correlation_id: int, sock: socket.socket) -> None:
    message_size_buffer = BytesIO(sock.recv(4))
    message_size = read_int32(message_size_buffer)

    buffer = BytesIO(sock.recv(message_size))

    header = ResponseHeaderV0.read(buffer)
    if header.correlation_id != request_correlation_id:
        raise ValueError()
    pprint(header)

    message = ApiVersionsResponseV0.read(buffer)
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
