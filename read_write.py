from __future__ import annotations

import struct
from typing import BinaryIO, Callable, TypeVar, Final
from uuid import UUID

UUID_ZERO: Final = UUID(int=0)

T = TypeVar("T")


def read_exact(buffer: BinaryIO, num_bytes: int) -> bytes:
    value = buffer.read(num_bytes)
    if len(value) != num_bytes:
        raise ValueError(f"Buffer underflow: expected {num_bytes}, got {len(value)}")
    return value


def read_int8(buffer: BinaryIO) -> int:
    return int.from_bytes(read_exact(buffer, 1), byteorder="big", signed=True)


def write_int8(value: int, buffer: BinaryIO) -> None:
    if -(2**7) <= value <= 2**7 - 1:
        buffer.write(value.to_bytes(1, byteorder="big", signed=True))
    else:
        raise ValueError(f"Value {value} is out of range for INT8")


def read_boolean(buffer: BinaryIO) -> bool:
    return read_int8(buffer) != 0


def write_boolean(value: bool, buffer: BinaryIO) -> None:
    write_int8(1 if value is True else 0, buffer)


def read_int16(buffer: BinaryIO) -> int:
    return int.from_bytes(read_exact(buffer, 2), byteorder="big", signed=True)


def write_int16(value: int, buffer: BinaryIO) -> None:
    if -(2**15) <= value <= 2**15 - 1:
        buffer.write(value.to_bytes(2, byteorder="big", signed=True))
    else:
        raise ValueError(f"Value {value} is out of range for INT16")


def read_int32(buffer: BinaryIO) -> int:
    return int.from_bytes(read_exact(buffer, 4), byteorder="big", signed=True)


def write_int32(value: int, buffer: BinaryIO) -> None:
    if -(2**31) <= value <= 2**31 - 1:
        buffer.write(value.to_bytes(4, byteorder="big", signed=True))
    else:
        raise ValueError(f"Value {value} is out of range for INT32")


def read_int64(buffer: BinaryIO) -> int:
    return int.from_bytes(read_exact(buffer, 8), byteorder="big", signed=True)


def write_int64(value: int, buffer: BinaryIO) -> None:
    if -(2**63) <= value <= 2**63 - 1:
        buffer.write(value.to_bytes(8, byteorder="big", signed=True))
    else:
        raise ValueError(f"Value {value} is out of range for INT64")


def read_uint16(buffer: BinaryIO) -> int:
    return int.from_bytes(read_exact(buffer, 2), byteorder="big", signed=False)


def write_uint16(value: int, buffer: BinaryIO) -> None:
    if 0 <= value <= 2**16 - 1:
        buffer.write(value.to_bytes(2, byteorder="big", signed=False))
    else:
        raise ValueError(f"Value {value} is out of range for UINT16")


def read_float64(buffer: BinaryIO) -> float:
    return struct.unpack(">d", read_exact(buffer, 8))[0]


def write_float64(value: float, buffer: BinaryIO) -> None:
    buffer.write(struct.pack(">d", value))


def read_unsigned_varint(buffer: BinaryIO) -> int:
    result = 0
    # Go by 7 bit steps.
    for offset in [0, 7, 14, 21, 28]:
        byte = int.from_bytes(read_exact(buffer, 1), byteorder="big", signed=False)

        # Concat the payload, 7 lower bits, to the result.
        payload_bits = byte & 0b111_1111
        result |= payload_bits << offset

        # This is the last byte if its most significant bit is 0.
        if byte & 0b1000_0000 == 0:
            return result
    else:
        raise ValueError("Varint is too long, most significant bit in 5th byte is set")


def write_unsigned_varint(value: int, buffer: BinaryIO) -> None:
    if value < 0 or value > 2**31 - 1:
        raise ValueError(f"Value {value} is out of range for UNSIGNED VARINT")

    written = False  # has at least one byte been written?
    while not written or value > 0:
        byte_to_write = value & 0b111_1111  # 7 lower bits
        value = value >> 7
        # Add the bit that signifies that more is to come.
        if value > 0:
            byte_to_write |= 0b1000_0000
        buffer.write(byte_to_write.to_bytes(1, byteorder="big", signed=False))
        written = True


def read_uuid(buffer: BinaryIO) -> UUID | None:
    byte_value: bytes = read_exact(buffer, 16)
    if byte_value == UUID_ZERO.bytes:
        return None
    else:
        return UUID(bytes=byte_value)


def write_uuid(value: UUID | None, buffer: BinaryIO) -> None:
    if value is None:
        buffer.write(UUID_ZERO.bytes)
    else:
        buffer.write(value.bytes)


def read_string(buffer: BinaryIO, compact: bool) -> str:
    result = read_nullable_string(buffer, compact)
    if result is None:
        raise ValueError("Non-nullable field was serialized as null")
    return result


def read_nullable_string(buffer: BinaryIO, compact: bool) -> str | None:
    length = read_string_length(buffer, compact)
    if length == -1:
        return None
    else:
        return read_exact(buffer, length).decode(encoding="utf-8")


def read_string_length(buffer: BinaryIO, compact: bool) -> int:
    # In the compact variant, stored lengths are increased by 1
    # to preserve unsignedness.
    length: int
    if compact:
        length = read_unsigned_varint(buffer) - 1
    else:
        length = read_int16(buffer)
    if length < -1 or length > 2**15 - 1:
        raise ValueError(f"string has invalid length {length}")
    return length


def write_string(value: str, buffer: BinaryIO, compact: bool) -> None:
    write_nullable_string(value, buffer, compact)


def write_nullable_string(value: str | None, buffer: BinaryIO, compact: bool) -> None:
    if value is None:
        write_string_length(-1, buffer, compact)
    else:
        value_b = value.encode(encoding="utf-8")
        write_string_length(len(value_b), buffer, compact)
        buffer.write(value_b)


def write_string_length(length: int, buffer: BinaryIO, compact: bool) -> None:
    if length > 2**15 - 1:
        raise ValueError(f"string has invalid length {length}")

    # In the compact variant, stored lengths are increased by 1
    # to preserve unsignedness.
    if compact:
        write_unsigned_varint(length + 1, buffer)
    else:
        write_int16(length, buffer)


def read_bytes(buffer: BinaryIO, compact: bool) -> bytes:
    result = read_nullable_bytes(buffer, compact)
    if result is None:
        raise ValueError("Non-nullable field was serialized as null")
    return result


def read_nullable_bytes(buffer: BinaryIO, compact: bool) -> bytes | None:
    length = read_array_length(buffer, compact)
    if length < -1 or length > 2**31 - 1:
        raise ValueError(f"bytes has invalid length {length}")

    if length == -1:
        return None
    else:
        return read_exact(buffer, length)


def read_array_length(buffer: BinaryIO, compact: bool) -> int:
    # In the compact variant, stored lengths are increased by 1
    # to preserve unsignedness.
    if compact:
        return read_unsigned_varint(buffer) - 1
    else:
        return read_int32(buffer)


def write_bytes(value: bytes, buffer: BinaryIO, compact: bool) -> None:
    write_nullable_bytes(value, buffer, compact)


def write_nullable_bytes(value: bytes | None, buffer: BinaryIO, compact: bool) -> None:
    if value is None:
        write_array_length(-1, buffer, compact)
    else:
        write_array_length(len(value), buffer, compact)
        buffer.write(value)


def write_array_length(length: int, buffer: BinaryIO, compact: bool) -> None:
    if length > 2**31 - 1:
        raise ValueError(f"bytes has invalid length {length}")

    # In the compact variant, stored lengths are increased by 1
    # to preserve unsignedness.
    if compact:
        write_unsigned_varint(length + 1, buffer)
    else:
        write_int32(length, buffer)


def read_array(
    read_element: Callable[[BinaryIO], T], buffer: BinaryIO, compact: bool
) -> list[T]:
    result = read_nullable_array(read_element, buffer, compact)
    if result is None:
        raise ValueError("Non-nullable field was serialized as null")
    return result


def read_nullable_array(
    read_element: Callable[[BinaryIO], T], buffer: BinaryIO, compact: bool
) -> list[T] | None:
    length = read_array_length(buffer, compact)
    if length == -1:
        return None
    else:
        array = []
        for _ in range(length):
            array.append(read_element(buffer))
        return array


def write_array(
    array: list[T],
    write_element: Callable[[T, BinaryIO], None],
    buffer: BinaryIO,
    compact: bool,
) -> None:
    write_nullable_array(array, write_element, buffer, compact)


def write_nullable_array(
    array: list[T] | None,
    write_element: Callable[[T, BinaryIO], None],
    buffer: BinaryIO,
    compact: bool,
) -> None:
    if array is None:
        write_array_length(-1, buffer, compact)
    else:
        write_array_length(len(array), buffer, compact)
        for el in array:
            write_element(el, buffer)
