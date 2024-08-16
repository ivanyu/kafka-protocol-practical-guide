from io import BytesIO

import pytest

from raw_tagged_fields import (
    RawTaggedField,
    write_unknown_tagged_fields,
    read_unknown_tagged_fields,
)


@pytest.mark.parametrize(
    "value",
    [
        RawTaggedField(tag=0, data=b""),
        RawTaggedField(tag=0, data=b"1234567"),
        RawTaggedField(tag=10, data=b"1234567890"),
    ],
)
def test_raw_tagged_fields(value: RawTaggedField) -> None:
    buf = BytesIO()
    value.write(buf)
    buf.seek(0)
    read_value = RawTaggedField.read(buf)
    assert read_value == value


@pytest.mark.parametrize(
    "value",
    [
        [],
        [
            RawTaggedField(tag=0, data=b""),
            RawTaggedField(tag=0, data=b"1234567"),
            RawTaggedField(tag=10, data=b"1234567890"),
        ],
    ],
)
def test_raw_tagged_field_array(value: list[RawTaggedField]) -> None:
    buf = BytesIO()
    write_unknown_tagged_fields(value, buf)
    buf.seek(0)
    read_value = read_unknown_tagged_fields(buf)
    assert read_value == value
