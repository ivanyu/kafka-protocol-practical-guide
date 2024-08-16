from __future__ import annotations
from dataclasses import dataclass
from typing import BinaryIO

from read_write import (
    read_unsigned_varint,
    read_exact,
    write_unsigned_varint,
    write_array_length,
    read_array_length,
)


@dataclass
class RawTaggedField:
    tag: int
    data: bytes

    @classmethod
    def read(cls, buffer: BinaryIO) -> RawTaggedField:
        tag = read_unsigned_varint(buffer)
        size = read_unsigned_varint(buffer)
        data = read_exact(buffer, size)
        return RawTaggedField(tag=tag, data=data)

    def write(self, buffer: BinaryIO) -> None:
        write_unsigned_varint(self.tag, buffer)
        write_unsigned_varint(len(self.data), buffer)
        buffer.write(self.data)


def read_unknown_tagged_fields(buffer: BinaryIO) -> list[RawTaggedField]:
    # As the tagged field array cannot be null,
    # we need to compensate the length shift that
    # `read_array_length` applies in the compact mode.
    size = read_array_length(buffer, True) + 1
    result = []
    for _ in range(size):
        result.append(RawTaggedField.read(buffer))
    return result


def write_unknown_tagged_fields(
    unknown_tagged_fields: list[RawTaggedField], buffer: BinaryIO
) -> None:
    # As the tagged field array cannot be null,
    # we need to compensate the length shift that
    # `write_array_length` applies in the compact mode.
    write_array_length(len(unknown_tagged_fields) - 1, buffer, True)
    for tf in unknown_tagged_fields:
        tf.write(buffer)
