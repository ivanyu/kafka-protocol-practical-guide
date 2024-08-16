from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO

from raw_tagged_fields import (
    RawTaggedField,
    write_unknown_tagged_fields,
    read_unknown_tagged_fields,
)
from read_write import write_int16, write_int32, write_nullable_string, read_int32


@dataclass
class RequestHeaderV0:
    request_api_key: int
    request_api_version: int
    correlation_id: int

    def write(self, buffer: BinaryIO) -> None:
        write_int16(self.request_api_key, buffer)
        write_int16(self.request_api_version, buffer)
        write_int32(self.correlation_id, buffer)


@dataclass
class RequestHeaderV1:
    request_api_key: int
    request_api_version: int
    correlation_id: int
    client_id: str

    def write(self, buffer: BinaryIO) -> None:
        write_int16(self.request_api_key, buffer)
        write_int16(self.request_api_version, buffer)
        write_int32(self.correlation_id, buffer)
        write_nullable_string(self.client_id, buffer, False)


@dataclass
class RequestHeaderV2:
    request_api_key: int
    request_api_version: int
    correlation_id: int
    client_id: str
    _unknownTaggedFields: list[RawTaggedField]

    def write(self, buffer: BinaryIO) -> None:
        write_int16(self.request_api_key, buffer)
        write_int16(self.request_api_version, buffer)
        write_int32(self.correlation_id, buffer)
        write_nullable_string(self.client_id, buffer, False)
        write_unknown_tagged_fields(self._unknownTaggedFields, buffer)


@dataclass
class ResponseHeaderV0:
    correlation_id: int

    @classmethod
    def read(cls, buffer: BinaryIO) -> ResponseHeaderV0:
        return ResponseHeaderV0(correlation_id=read_int32(buffer))


@dataclass
class ResponseHeaderV1:
    correlation_id: int
    _unknownTaggedFields: list[RawTaggedField]

    @classmethod
    def read(cls, buffer: BinaryIO) -> ResponseHeaderV1:
        return ResponseHeaderV1(
            correlation_id=read_int32(buffer),
            _unknownTaggedFields=read_unknown_tagged_fields(buffer),
        )
