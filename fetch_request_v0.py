from __future__ import annotations

import socket
from dataclasses import dataclass
from io import BytesIO
from pprint import pprint
from typing import BinaryIO

from request_response_headers import RequestHeaderV1, ResponseHeaderV0
from read_write import (
    write_int32,
    write_string,
    write_array,
    write_int64,
    read_int32,
    read_array,
    read_string,
    read_int16,
    read_int64,
    read_nullable_bytes,
)


@dataclass
class FetchRequestTopicPartitionV0:
    partition: int
    fetch_offset: int
    partition_max_bytes: int

    def write(self, buffer: BinaryIO) -> None:
        write_int32(self.partition, buffer)
        write_int64(self.fetch_offset, buffer)
        write_int32(self.partition_max_bytes, buffer)


@dataclass
class FetchRequestTopicV0:
    topic: str
    partitions: list[FetchRequestTopicPartitionV0]

    def write(self, buffer: BinaryIO) -> None:
        write_string(self.topic, buffer, False)
        write_array(self.partitions, FetchRequestTopicPartitionV0.write, buffer, False)


@dataclass
class FetchRequestV0:
    replica_id: int
    max_wait_ms: int
    min_bytes: int
    topics: list[FetchRequestTopicV0]

    def write(self, buffer: BinaryIO) -> None:
        write_int32(self.replica_id, buffer)
        write_int32(self.max_wait_ms, buffer)
        write_int32(self.min_bytes, buffer)
        write_array(self.topics, FetchRequestTopicV0.write, buffer, False)


def send_request(request_correlation_id: int, sock: socket.socket) -> None:
    buffer = BytesIO()

    # message_size
    # will be filled later
    write_int32(0, buffer)

    header = RequestHeaderV1(
        request_api_key=1,  # ApiVersions
        request_api_version=0,
        correlation_id=request_correlation_id,
        client_id="test-client",
    )
    header.write(buffer)

    message = FetchRequestV0(
        replica_id=-1,
        max_wait_ms=3000,
        min_bytes=1,
        topics=[
            FetchRequestTopicV0(
                topic="test-topic1",
                partitions=[
                    FetchRequestTopicPartitionV0(
                        partition=0,
                        fetch_offset=0,
                        partition_max_bytes=10_000,
                    )
                ],
            )
        ],
    )
    message.write(buffer)

    request_message_size = buffer.tell() - 4
    buffer.seek(0)
    write_int32(request_message_size, buffer)

    sock.sendall(buffer.getvalue())


@dataclass
class FetchResponseResponsePartitionV0:
    partition_index: int
    error_code: int
    high_watermark: int
    records: bytes | None

    @classmethod
    def read(cls, buffer: BinaryIO) -> FetchResponseResponsePartitionV0:
        return FetchResponseResponsePartitionV0(
            partition_index=read_int32(buffer),
            error_code=read_int16(buffer),
            high_watermark=read_int64(buffer),
            records=read_nullable_bytes(buffer, False),
        )


@dataclass
class FetchResponseResponseV0:
    topic: str
    partitions: list[FetchResponseResponsePartitionV0]

    @classmethod
    def read(cls, buffer: BinaryIO) -> FetchResponseResponseV0:
        return FetchResponseResponseV0(
            topic=read_string(buffer, False),
            partitions=read_array(FetchResponseResponsePartitionV0.read, buffer, False),
        )


@dataclass
class FetchResponseV0:
    responses: list[FetchResponseResponseV0]

    @classmethod
    def read(cls, buffer: BinaryIO) -> FetchResponseV0:
        return FetchResponseV0(
            responses=read_array(FetchResponseResponseV0.read, buffer, False)
        )


def receive_response(request_correlation_id: int, sock: socket.socket) -> None:
    message_size_buffer = BytesIO(sock.recv(4))
    message_size = read_int32(message_size_buffer)

    buffer = BytesIO(sock.recv(message_size))

    header = ResponseHeaderV0.read(buffer)
    if header.correlation_id != request_correlation_id:
        raise ValueError()
    pprint(header)

    message = FetchResponseV0.read(buffer)
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
