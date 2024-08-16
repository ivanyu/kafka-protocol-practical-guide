from io import BytesIO
from uuid import UUID

import pytest

from read_write import (
    write_boolean,
    read_boolean,
    write_int8,
    read_int8,
    write_int16,
    read_int16,
    read_int32,
    write_int32,
    read_int64,
    write_int64,
    write_uint16,
    read_uint16,
    write_float64,
    read_float64,
    write_unsigned_varint,
    read_unsigned_varint,
    write_uuid,
    read_uuid,
    write_string,
    read_string,
    write_nullable_string,
    read_nullable_string,
    write_bytes,
    read_bytes,
    write_nullable_bytes,
    read_nullable_bytes,
    write_nullable_array,
    read_nullable_array,
    write_array,
    read_array,
)


@pytest.mark.parametrize("value", [True, False])
def test_boolean(value: bool) -> None:
    buf = BytesIO()
    write_boolean(value, buf)
    buf.seek(0)
    read_value = read_boolean(buf)
    assert read_value == value


@pytest.mark.parametrize("value", [-(2**7), -1, 0, 1, 2**7 - 1])
def test_int8(value: bool) -> None:
    buf = BytesIO()
    write_int8(value, buf)
    buf.seek(0)
    read_value = read_int8(buf)
    assert read_value == value


@pytest.mark.parametrize("value", [-(2**7) - 1, 2**7])
def test_int8_out_of_range(value: bool) -> None:
    with pytest.raises(ValueError, match=f"Value {value} is out of range for INT8"):
        write_int8(value, BytesIO())


@pytest.mark.parametrize("value", [-(2**15), -1, 0, 1, 2**15 - 1])
def test_int16(value: bool) -> None:
    buf = BytesIO()
    write_int16(value, buf)
    buf.seek(0)
    read_value = read_int16(buf)
    assert read_value == value


@pytest.mark.parametrize("value", [-(2**15) - 1, 2**15])
def test_int16_out_of_range(value: bool) -> None:
    with pytest.raises(ValueError, match=f"Value {value} is out of range for INT16"):
        write_int16(value, BytesIO())


@pytest.mark.parametrize("value", [-(2**31), -1, 0, 1, 2**31 - 1])
def test_int32(value: bool) -> None:
    buf = BytesIO()
    write_int32(value, buf)
    buf.seek(0)
    read_value = read_int32(buf)
    assert read_value == value


@pytest.mark.parametrize("value", [-(2**31) - 1, 2**31])
def test_int32_out_of_range(value: bool) -> None:
    with pytest.raises(ValueError, match=f"Value {value} is out of range for INT32"):
        write_int32(value, BytesIO())


@pytest.mark.parametrize("value", [-(2**63), -1, 0, 1, 2**63 - 1])
def test_int64(value: bool) -> None:
    buf = BytesIO()
    write_int64(value, buf)
    buf.seek(0)
    read_value = read_int64(buf)
    assert read_value == value


@pytest.mark.parametrize("value", [-(2**63) - 1, 2**63])
def test_int64_out_of_range(value: bool) -> None:
    with pytest.raises(ValueError, match=f"Value {value} is out of range for INT64"):
        write_int64(value, BytesIO())


@pytest.mark.parametrize("value", [0, 1, 2**16 - 1])
def test_uint16(value: bool) -> None:
    buf = BytesIO()
    write_uint16(value, buf)
    buf.seek(0)
    read_value = read_uint16(buf)
    assert read_value == value


@pytest.mark.parametrize("value", [-1, 2**16])
def test_uint16_out_of_range(value: bool) -> None:
    with pytest.raises(ValueError, match=f"Value {value} is out of range for UINT16"):
        write_uint16(value, BytesIO())


@pytest.mark.parametrize("value", [-3460.123, -1000, -1, 0, 1.01, 5000.9, 1000.12])
def test_float64(value: float) -> None:
    buf = BytesIO()
    write_float64(value, buf)
    buf.seek(0)
    read_value = read_float64(buf)
    assert read_value == value


@pytest.mark.parametrize("value", [0, 1, 10, 13, 12415, 2**15, 2**31 - 1])
def test_unsigned_varint(value: int) -> None:
    buf = BytesIO()
    write_unsigned_varint(value, buf)
    buf.seek(0)
    read_value = read_unsigned_varint(buf)
    assert read_value == value


@pytest.mark.parametrize(
    ("value", "expected_value"),
    [
        (0, b"\x00"),
        (1, b"\x01"),
        (127, b"\x7f"),
        (128, b"\x80\x01"),
        (129, b"\x81\x01"),
        (256, b"\x80\x02"),
        (1024, b"\x80\x08"),
        (100500, b"\x94\x91\x06"),
        (9999999, b"\xff\xac\xe2\x04"),
        (2147483647, b"\xff\xff\xff\xff\x07"),
    ],
)
def test_unsigned_varint_from_java(value: int, expected_value: bytes) -> None:
    buf = BytesIO()
    write_unsigned_varint(value, buf)
    assert buf.getvalue() == expected_value


@pytest.mark.parametrize("value", [-1, 2**31])
def test_unsigned_varint_out_of_range(value: bool) -> None:
    with pytest.raises(
        ValueError, match=f"Value {value} is out of range for UNSIGNED VARINT"
    ):
        write_unsigned_varint(value, BytesIO())


@pytest.mark.parametrize(
    "value",
    [
        None,
        UUID("45963434-3053-4af2-825c-4cc77e9aeabe"),
        UUID("1473eb8c-203a-46ee-bfc9-ebb9e30d1e97"),
        UUID("871038c6-57ee-41a5-a59f-3eabff84ebbb"),
        UUID("f91b9842-911c-4001-b476-bb8e37b99c9f"),
        UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
    ],
)
def test_uuid(value: UUID | None) -> None:
    buf = BytesIO()
    write_uuid(value, buf)
    buf.seek(0)
    read_value = read_uuid(buf)
    assert read_value == value


@pytest.mark.parametrize("compact", [True, False])
@pytest.mark.parametrize("value", ["", "abcd", "XXXXXXXX"])
def test_string(compact: bool, value: str) -> None:
    buf = BytesIO()
    write_string(value, buf, compact)
    buf.seek(0)
    read_value = read_string(buf, compact)
    assert read_value == value


@pytest.mark.parametrize("compact", [True, False])
@pytest.mark.parametrize("value", ["", "abcd", "XXXXXXXX", None])
def test_nullable_string(compact: bool, value: str | None) -> None:
    buf = BytesIO()
    write_nullable_string(value, buf, compact)
    buf.seek(0)
    read_value = read_nullable_string(buf, compact)
    assert read_value == value


@pytest.mark.parametrize("compact", [True, False])
@pytest.mark.parametrize("value", [b"", b"abcd", b"XXXXXXXX"])
def test_bytes(compact: bool, value: bytes) -> None:
    buf = BytesIO()
    write_bytes(value, buf, compact)
    buf.seek(0)
    read_value = read_bytes(buf, compact)
    assert read_value == value


@pytest.mark.parametrize("compact", [True, False])
@pytest.mark.parametrize("value", [b"", b"abcd", b"XXXXXXXX", None])
def test_nullable_bytes(compact: bool, value: bytes | None) -> None:
    buf = BytesIO()
    write_nullable_bytes(value, buf, compact)
    buf.seek(0)
    read_value = read_nullable_bytes(buf, compact)
    assert read_value == value


@pytest.mark.parametrize("compact", [True, False])
@pytest.mark.parametrize(
    "value",
    [
        [],
        [1],
        [1, 2, 3, 4, 5],
        list(range(1000)),
    ],
)
def test_array(compact: bool, value: list[int]) -> None:
    buf = BytesIO()
    write_array(value, write_int32, buf, compact)
    buf.seek(0)
    read_value = read_array(read_int32, buf, compact)
    assert read_value == value


@pytest.mark.parametrize("compact", [True, False])
@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        [1],
        [1, 2, 3, 4, 5],
        list(range(1000)),
    ],
)
def test_nullable_array(compact: bool, value: list[int] | None) -> None:
    buf = BytesIO()
    write_nullable_array(value, write_int32, buf, compact)
    buf.seek(0)
    read_value = read_nullable_array(read_int32, buf, compact)
    assert read_value == value
