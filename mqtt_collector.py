#!/usr/bin/env python3
import argparse
import dataclasses
import datetime
import math
import os
import struct
import sys
import tempfile
import threading
import time
from typing import Any, Optional, TextIO
import json
import fnmatch
import re


TSDB_TAG_BYTES = b"TSDB\x00\x00\x00\x00"
TSDB_VERSION = 1

ENTRY_TYPE_TIME_ABSOLUTE = 0xF0
ENTRY_TYPE_TIME_REL_8 = 0xF1
ENTRY_TYPE_TIME_REL_16 = 0xF2
ENTRY_TYPE_TIME_REL_24 = 0xF3
ENTRY_TYPE_TIME_REL_32 = 0xF4
ENTRY_TYPE_CHANNEL_DEF_8 = 0xF5
ENTRY_TYPE_CHANNEL_DEF_16 = 0xF6
ENTRY_TYPE_CHANNEL_VALUE_16 = 0xFF
ENTRY_TYPE_EOF = 0xFE

FORMAT_FLOAT = 0x00
FORMAT_DOUBLE = 0x01
FORMAT_STRING_U8 = 0x08
FORMAT_STRING_U16 = 0x09
FORMAT_STRING_U32 = 0x0A
FORMAT_STRING_U64 = 0x0B


def _ensure_available(data: bytes, offset: int, size: int, what: str) -> None:
    if offset + size > len(data):
        raise ValueError(f"Unexpected EOF while reading {what} at offset {offset}")


def _read_u24(data: bytes, offset: int) -> tuple[int, int]:
    _ensure_available(data, offset, 3, "uint24")
    b0 = data[offset]
    b1 = data[offset + 1]
    b2 = data[offset + 2]
    return b0 | (b1 << 8) | (b2 << 16), offset + 3


def _read_i24(data: bytes, offset: int) -> tuple[int, int]:
    value, offset = _read_u24(data, offset)
    if value & 0x800000:
        value -= 1 << 24
    return value, offset


def _read_scalar(data: bytes, offset: int, byte_count: int, signed: bool) -> tuple[int, int]:
    if byte_count == 1:
        _ensure_available(data, offset, 1, "int8/uint8")
        return int.from_bytes(data[offset:offset + 1], "little", signed=signed), offset + 1
    if byte_count == 2:
        _ensure_available(data, offset, 2, "int16/uint16")
        return int.from_bytes(data[offset:offset + 2], "little", signed=signed), offset + 2
    if byte_count == 3:
        if signed:
            return _read_i24(data, offset)
        return _read_u24(data, offset)
    if byte_count == 4:
        _ensure_available(data, offset, 4, "int32/uint32")
        return int.from_bytes(data[offset:offset + 4], "little", signed=signed), offset + 4
    if byte_count == 8:
        _ensure_available(data, offset, 8, "int64/uint64")
        return int.from_bytes(data[offset:offset + 8], "little", signed=signed), offset + 8
    raise ValueError(f"Unsupported scalar byte_count={byte_count}")


def _read_format_value(data: bytes, offset: int, format_id: int) -> tuple[Any, int]:
    if format_id == FORMAT_FLOAT:
        _ensure_available(data, offset, 4, "float")
        return struct.unpack_from("<f", data, offset)[0], offset + 4
    if format_id == FORMAT_DOUBLE:
        _ensure_available(data, offset, 8, "double")
        return struct.unpack_from("<d", data, offset)[0], offset + 8
    if format_id in (FORMAT_STRING_U8, FORMAT_STRING_U16, FORMAT_STRING_U32, FORMAT_STRING_U64):
        len_size = {
            FORMAT_STRING_U8: 1,
            FORMAT_STRING_U16: 2,
            FORMAT_STRING_U32: 4,
            FORMAT_STRING_U64: 8,
        }[format_id]
        _ensure_available(data, offset, len_size, "string length")
        text_len = int.from_bytes(data[offset:offset + len_size], "little")
        offset += len_size
        _ensure_available(data, offset, text_len, "string bytes")
        return data[offset:offset + text_len].decode("utf-8"), offset + text_len

    hi = (format_id >> 4) & 0xF
    lo = format_id & 0xF
    byte_count = {0x1: 1, 0x2: 2, 0x3: 3, 0x4: 4, 0x5: 8, 0x9: 1, 0xA: 2, 0xB: 3, 0xC: 4, 0xD: 8}.get(hi)
    if byte_count is None or lo > 3:
        raise ValueError(f"Unsupported formatId 0x{format_id:02x}")
    signed = hi <= 0x5
    raw_value, offset = _read_scalar(data, offset, byte_count, signed=signed)
    scale = {0: 1.0, 1: 10.0, 2: 100.0, 3: 1000.0}[lo]
    if scale == 1.0:
        return raw_value, offset
    return raw_value / scale, offset


@dataclasses.dataclass(frozen=True)
class TimeSeriesPoint:
    timestamp_ms: int
    value: Any


class TimeSeriesDbData:
    def __init__(self) -> None:
        self._series_values: dict[str, list[TimeSeriesPoint]] = {}
        self._events: list[tuple[int, str, Any]] = []
        self._series_format_ids: dict[str, int] = {}

    def _append(self, series_name: str, timestamp_ms: int, value: Any) -> None:
        point = TimeSeriesPoint(timestamp_ms, value)
        self._series_values.setdefault(series_name, []).append(point)
        self._events.append((timestamp_ms, series_name, value))

    def list_series(self) -> list[str]:
        return sorted(self._series_values.keys())

    def get_series_values(self, series_name: str) -> list[tuple[int, Any]]:
        return [(point.timestamp_ms, point.value) for point in self._series_values.get(series_name, [])]

    def iter_events(self) -> list[tuple[int, str, Any]]:
        return list(self._events)

    def _set_series_format_id(self, series_name: str, format_id: int) -> None:
        self._series_format_ids[series_name] = format_id

    def get_series_format_id(self, series_name: str) -> Optional[int]:
        return self._series_format_ids.get(series_name)

    def dump(self, out: Optional[TextIO] = None) -> None:
        stream = out if out is not None else sys.stdout
        stream.write(f"TimeSeriesDB dump: series={len(self._series_values)} events={len(self._events)}\n")
        stream.write("Series:\n")
        for series_name in self.list_series():
            format_id = self._series_format_ids.get(series_name)
            format_text = f"0x{format_id:02x} ({_format_id_description(format_id)})" if format_id is not None else "unknown"
            stream.write(f"  - {series_name}: format={format_text}\n")
        stream.write("Events:\n")
        prev_ts: Optional[int] = None
        for idx, (timestamp_ms, series_name, value) in enumerate(self._events):
            rel_text = "ABS" if prev_ts is None or timestamp_ms < prev_ts else f"+{timestamp_ms - prev_ts}"
            prev_ts = timestamp_ms
            format_id = self._series_format_ids.get(series_name)
            format_text = f"0x{format_id:02x}" if format_id is not None else "??"
            ts_hr = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=datetime.timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )[:-3]
            stream.write(
                f"  [{idx}] ts_abs={timestamp_ms} ({ts_hr}) ts_rel={rel_text} series={series_name} "
                f"format={format_text} value={value!r}\n"
            )


def read_timeseries_db(path: str, dump_out: Optional[TextIO] = None, verbose: int = 0) -> TimeSeriesDbData:
    with open(path, "rb") as f:
        raw = f.read()
    if len(raw) < 12:
        raise ValueError(f"File too small: {path}")
    if raw[:8] != TSDB_TAG_BYTES:
        raise ValueError(f"Invalid TSDB tag in {path!r}")
    version = int.from_bytes(raw[8:12], "little")
    if version != TSDB_VERSION:
        raise ValueError(f"Unsupported TSDB version {version} in {path!r}")

    result = TimeSeriesDbData()
    channel_defs: dict[int, tuple[int, str]] = {}
    current_ts: Optional[int] = None
    stream = dump_out if dump_out is not None else None
    if stream is not None:
        stream.write("Events:\n")
    prev_event_ts: Optional[int] = None

    offset = 12
    while offset < len(raw):
        entry_start = offset
        entry_type = raw[offset]
        offset += 1

        if entry_type <= 0xEF:
            channel_id = entry_type
            if current_ts is None:
                raise ValueError("Value entry encountered before any timestamp was set")
            if channel_id not in channel_defs:
                raise ValueError(f"Undefined channel id {channel_id}")
            format_id, series_name = channel_defs[channel_id]
            value, offset = _read_format_value(raw, offset, format_id)
            entry_bytes = raw[entry_start:offset]
            if stream is not None and verbose:
                stream.write(
                    f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} "
                    f"(value ch={channel_id} format=0x{format_id:02x})\n"
                )
            result._append(series_name, current_ts, value)
            if stream is not None:
                rel_text = "ABS" if prev_event_ts is None or current_ts < prev_event_ts else f"+{current_ts - prev_event_ts}"
                prev_event_ts = current_ts
                ts_hr = datetime.datetime.fromtimestamp(current_ts / 1000.0, tz=datetime.timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                )[:-3]
                stream.write(
                    f"  [{len(result._events) - 1}] ts_abs={current_ts} ({ts_hr}) ts_rel={rel_text} "
                    f"series={series_name} format=0x{format_id:02x} value={value!r}\n"
                )
            continue

        if entry_type == ENTRY_TYPE_CHANNEL_VALUE_16:
            _ensure_available(raw, offset, 2, "16-bit channel id")
            channel_id = int.from_bytes(raw[offset:offset + 2], "little")
            offset += 2
            if current_ts is None:
                raise ValueError("16-bit value entry encountered before any timestamp was set")
            if channel_id not in channel_defs:
                raise ValueError(f"Undefined 16-bit channel id {channel_id}")
            format_id, series_name = channel_defs[channel_id]
            value, offset = _read_format_value(raw, offset, format_id)
            entry_bytes = raw[entry_start:offset]
            if stream is not None and verbose:
                stream.write(
                    f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} "
                    f"(value ch16={channel_id} format=0x{format_id:02x})\n"
                )
            result._append(series_name, current_ts, value)
            if stream is not None:
                rel_text = "ABS" if prev_event_ts is None or current_ts < prev_event_ts else f"+{current_ts - prev_event_ts}"
                prev_event_ts = current_ts
                ts_hr = datetime.datetime.fromtimestamp(current_ts / 1000.0, tz=datetime.timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                )[:-3]
                stream.write(
                    f"  [{len(result._events) - 1}] ts_abs={current_ts} ({ts_hr}) ts_rel={rel_text} "
                    f"series={series_name} format=0x{format_id:02x} value={value!r}\n"
                )
            continue

        if entry_type == ENTRY_TYPE_TIME_ABSOLUTE:
            _ensure_available(raw, offset, 8, "absolute timestamp")
            current_ts = int.from_bytes(raw[offset:offset + 8], "little")
            offset += 8
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(
                    f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} "
                    f"(ts_abs={current_ts})\n"
                )
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_8:
            _ensure_available(raw, offset, 1, "relative timestamp (8-bit)")
            rel = raw[offset]
            offset += 1
            if current_ts is None:
                raise ValueError("Relative timestamp entry encountered before any absolute timestamp")
            current_ts += rel
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(
                    f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} "
                    f"(ts_rel8=+{rel} -> {current_ts})\n"
                )
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_16:
            _ensure_available(raw, offset, 2, "relative timestamp (16-bit)")
            rel = int.from_bytes(raw[offset:offset + 2], "little")
            offset += 2
            if current_ts is None:
                raise ValueError("Relative timestamp entry encountered before any absolute timestamp")
            current_ts += rel
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(
                    f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} "
                    f"(ts_rel16=+{rel} -> {current_ts})\n"
                )
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_24:
            rel, offset = _read_u24(raw, offset)
            if current_ts is None:
                raise ValueError("Relative timestamp entry encountered before any absolute timestamp")
            current_ts += rel
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(
                    f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} "
                    f"(ts_rel24=+{rel} -> {current_ts})\n"
                )
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_32:
            _ensure_available(raw, offset, 4, "relative timestamp (32-bit)")
            rel = int.from_bytes(raw[offset:offset + 4], "little")
            offset += 4
            if current_ts is None:
                raise ValueError("Relative timestamp entry encountered before any absolute timestamp")
            current_ts += rel
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(
                    f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} "
                    f"(ts_rel32=+{rel} -> {current_ts})\n"
                )
            continue

        if entry_type == ENTRY_TYPE_CHANNEL_DEF_8:
            _ensure_available(raw, offset, 3, "8-bit channel definition")
            channel_id = raw[offset]
            format_id = raw[offset + 1]
            name_len = raw[offset + 2]
            offset += 3
            _ensure_available(raw, offset, name_len, "channel name")
            series_name = raw[offset:offset + name_len].decode("utf-8")
            offset += name_len
            channel_defs[channel_id] = (format_id, series_name)
            result._set_series_format_id(series_name, format_id)
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(
                    f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} "
                    f"(def ch8={channel_id} format=0x{format_id:02x} name={series_name!r})\n"
                )
            continue

        if entry_type == ENTRY_TYPE_CHANNEL_DEF_16:
            _ensure_available(raw, offset, 4, "16-bit channel definition")
            channel_id = int.from_bytes(raw[offset:offset + 2], "little")
            format_id = raw[offset + 2]
            name_len = raw[offset + 3]
            offset += 4
            _ensure_available(raw, offset, name_len, "channel name")
            series_name = raw[offset:offset + name_len].decode("utf-8")
            offset += name_len
            channel_defs[channel_id] = (format_id, series_name)
            result._set_series_format_id(series_name, format_id)
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(
                    f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} "
                    f"(def ch16={channel_id} format=0x{format_id:02x} name={series_name!r})\n"
                )
            continue

        if entry_type == ENTRY_TYPE_EOF:
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} (eof)\n")
            break

        raise ValueError(f"Unknown entry type 0x{entry_type:02x} at offset {offset - 1}")

    if stream is not None:
        stream.write(f"TimeSeriesDB dump: series={len(result._series_values)} events={len(result._events)}\n")
        stream.write("Series:\n")
        for series_name in result.list_series():
            format_id = result._series_format_ids.get(series_name)
            format_text = f"0x{format_id:02x} ({_format_id_description(format_id)})" if format_id is not None else "unknown"
            stream.write(f"  - {series_name}: format={format_text}\n")

    return result


class TimeSeriesDbWriter:
    def __init__(self, path: str) -> None:
        self._path = path
        self._f = open(path, "wb")
        self._f.write(TSDB_TAG_BYTES)
        self._f.write(struct.pack("<I", TSDB_VERSION))
        self._series_to_channel: dict[str, tuple[int, int]] = {}
        self._next_channel_id = 0
        self._current_timestamp_ms: Optional[int] = None
        self._closed = False

    def _write_channel_definition(self, channel_id: int, series_name: str, format_id: int) -> None:
        name_bytes = series_name.encode("utf-8")
        if len(name_bytes) > 255:
            raise ValueError(f"Series name too long ({len(name_bytes)} bytes > 255): {series_name!r}")
        if channel_id <= 0xEF:
            self._f.write(bytes([ENTRY_TYPE_CHANNEL_DEF_8, channel_id, format_id, len(name_bytes)]))
        else:
            self._f.write(bytes([ENTRY_TYPE_CHANNEL_DEF_16]))
            self._f.write(channel_id.to_bytes(2, "little"))
            self._f.write(bytes([format_id, len(name_bytes)]))
        self._f.write(name_bytes)

    def _ensure_channel(self, series_name: str, format_id: int) -> int:
        if series_name in self._series_to_channel:
            channel_id, existing_format_id = self._series_to_channel[series_name]
            if existing_format_id != format_id:
                raise ValueError(
                    f"Series {series_name!r} already defined with formatId=0x{existing_format_id:02x}, "
                    f"cannot write formatId=0x{format_id:02x}"
                )
            return channel_id
        channel_id = self._next_channel_id
        if channel_id > 0xFFFF:
            raise ValueError("Exceeded max channel id (65535)")
        self._next_channel_id += 1
        self._write_channel_definition(channel_id, series_name, format_id)
        self._series_to_channel[series_name] = (channel_id, format_id)
        return channel_id

    def _write_timestamp(self, timestamp_ms: int) -> None:
        if self._current_timestamp_ms is None or timestamp_ms < self._current_timestamp_ms:
            self._f.write(bytes([ENTRY_TYPE_TIME_ABSOLUTE]))
            self._f.write(timestamp_ms.to_bytes(8, "little"))
            self._current_timestamp_ms = timestamp_ms
            return
        delta = timestamp_ms - self._current_timestamp_ms
        if delta == 0:
            return
        if delta <= 0xFF:
            self._f.write(bytes([ENTRY_TYPE_TIME_REL_8, delta]))
        elif delta <= 0xFFFF:
            self._f.write(bytes([ENTRY_TYPE_TIME_REL_16]))
            self._f.write(delta.to_bytes(2, "little"))
        elif delta <= 0xFFFFFF:
            self._f.write(bytes([ENTRY_TYPE_TIME_REL_24]))
            self._f.write(delta.to_bytes(3, "little"))
        elif delta <= 0xFFFFFFFF:
            self._f.write(bytes([ENTRY_TYPE_TIME_REL_32]))
            self._f.write(delta.to_bytes(4, "little"))
        else:
            self._f.write(bytes([ENTRY_TYPE_TIME_ABSOLUTE]))
            self._f.write(timestamp_ms.to_bytes(8, "little"))
        self._current_timestamp_ms = timestamp_ms

    def _append_value_entry(self, channel_id: int, payload: bytes) -> None:
        if channel_id <= 0xEF:
            self._f.write(bytes([channel_id]))
        else:
            self._f.write(bytes([ENTRY_TYPE_CHANNEL_VALUE_16]))
            self._f.write(channel_id.to_bytes(2, "little"))
        self._f.write(payload)

    def add_value(self, series_name: str, value_as_double: float, timestamp_ms: Optional[int] = None) -> None:
        if self._closed:
            raise ValueError("Writer is already closed")
        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)
        if timestamp_ms < 0:
            raise ValueError(f"timestamp_ms must be >= 0, got {timestamp_ms}")

        self._write_timestamp(timestamp_ms)
        channel_id = self._ensure_channel(series_name, FORMAT_DOUBLE)
        self._append_value_entry(channel_id, struct.pack("<d", float(value_as_double)))
        self._f.flush()

    def add_string_value(self, series_name: str, value_as_string: str, timestamp_ms: Optional[int] = None) -> None:
        if self._closed:
            raise ValueError("Writer is already closed")
        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)
        if timestamp_ms < 0:
            raise ValueError(f"timestamp_ms must be >= 0, got {timestamp_ms}")

        value_bytes = value_as_string.encode("utf-8")
        payload = len(value_bytes).to_bytes(8, "little") + value_bytes
        self._write_timestamp(timestamp_ms)
        channel_id = self._ensure_channel(series_name, FORMAT_STRING_U64)
        self._append_value_entry(channel_id, payload)
        self._f.flush()

    def addValue(self, series_name: str, value_as_double: float, timestamp_ms: Optional[int] = None) -> None:
        self.add_value(series_name, value_as_double, timestamp_ms)

    def addStringValue(self, series_name: str, value_as_string: str, timestamp_ms: Optional[int] = None) -> None:
        self.add_string_value(series_name, value_as_string, timestamp_ms)

    def close(self, mark_complete: bool = False) -> None:
        if self._closed:
            return
        if mark_complete:
            self._f.write(bytes([ENTRY_TYPE_EOF]))
            self._f.flush()
        self._f.close()
        self._closed = True

    def __enter__(self) -> "TimeSeriesDbWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close(mark_complete=False)


def create_timeseries_db_writer(path: str) -> TimeSeriesDbWriter:
    return TimeSeriesDbWriter(path)


def _write_u24(value: int) -> bytes:
    return bytes((value & 0xFF, (value >> 8) & 0xFF, (value >> 16) & 0xFF))


def _signed_range(byte_count: int) -> tuple[int, int]:
    bits = byte_count * 8
    return -(1 << (bits - 1)), (1 << (bits - 1)) - 1


def _unsigned_range(byte_count: int) -> tuple[int, int]:
    bits = byte_count * 8
    return 0, (1 << bits) - 1


def _numeric_format_shape(format_id: int) -> Optional[tuple[int, bool, int]]:
    if format_id in (FORMAT_FLOAT, FORMAT_DOUBLE):
        return None
    hi = (format_id >> 4) & 0xF
    lo = format_id & 0xF
    byte_count = {0x1: 1, 0x2: 2, 0x3: 3, 0x4: 4, 0x5: 8, 0x9: 1, 0xA: 2, 0xB: 3, 0xC: 4, 0xD: 8}.get(hi)
    if byte_count is None or lo > 3:
        return None
    signed = hi <= 0x5
    scale = {0: 1, 1: 10, 2: 100, 3: 1000}[lo]
    return byte_count, signed, scale


def _c_int_type_name(byte_count: int, signed: bool) -> str:
    mapping = {
        (1, True): "int8_t",
        (2, True): "int16_t",
        (3, True): "int24_t",
        (4, True): "int32_t",
        (8, True): "int64_t",
        (1, False): "uint8_t",
        (2, False): "uint16_t",
        (3, False): "uint24_t",
        (4, False): "uint32_t",
        (8, False): "uint64_t",
    }
    return mapping[(byte_count, signed)]


def _format_id_description(format_id: int) -> str:
    if format_id == FORMAT_FLOAT:
        return "float"
    if format_id == FORMAT_DOUBLE:
        return "double"
    if format_id in (FORMAT_STRING_U8, FORMAT_STRING_U16, FORMAT_STRING_U32, FORMAT_STRING_U64):
        len_type = {
            FORMAT_STRING_U8: "uint8_t",
            FORMAT_STRING_U16: "uint16_t",
            FORMAT_STRING_U32: "uint32_t",
            FORMAT_STRING_U64: "uint64_t",
        }[format_id]
        return f"UTF-8 string with {len_type} length prefix"
    shape = _numeric_format_shape(format_id)
    if shape is None:
        return "unknown"
    byte_count, signed, scale = shape
    c_type = _c_int_type_name(byte_count, signed)
    if scale == 1:
        return c_type
    return f"{c_type} x; value = x / {scale}.0"


def _is_equal_6_digits(a: float, b: float) -> bool:
    return round(float(a), 6) == round(float(b), 6)


def _encode_value_for_format(value: Any, format_id: int) -> Optional[bytes]:
    if format_id == FORMAT_DOUBLE:
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        return struct.pack("<d", numeric)
    if format_id == FORMAT_FLOAT:
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        encoded = struct.pack("<f", numeric)
        decoded = struct.unpack("<f", encoded)[0]
        return encoded if _is_equal_6_digits(numeric, decoded) else None

    if format_id in (FORMAT_STRING_U8, FORMAT_STRING_U16, FORMAT_STRING_U32, FORMAT_STRING_U64):
        if not isinstance(value, str):
            return None
        raw = value.encode("utf-8")
        len_size = {FORMAT_STRING_U8: 1, FORMAT_STRING_U16: 2, FORMAT_STRING_U32: 4, FORMAT_STRING_U64: 8}[format_id]
        max_len = (1 << (len_size * 8)) - 1
        if len(raw) > max_len:
            return None
        return len(raw).to_bytes(len_size, "little") + raw

    shape = _numeric_format_shape(format_id)
    if shape is None:
        return None
    byte_count, signed, scale = shape
    numeric = float(value)
    if not math.isfinite(numeric):
        return None
    scaled = int(round(numeric * scale))
    low, high = (_signed_range(byte_count) if signed else _unsigned_range(byte_count))
    if scaled < low or scaled > high:
        return None
    reconstructed = scaled / scale
    if not _is_equal_6_digits(numeric, reconstructed):
        return None

    if byte_count == 1:
        return int(scaled).to_bytes(1, "little", signed=signed)
    if byte_count == 2:
        return int(scaled).to_bytes(2, "little", signed=signed)
    if byte_count == 3:
        if signed and scaled < 0:
            scaled = (1 << 24) + scaled
        return _write_u24(int(scaled))
    if byte_count == 4:
        return int(scaled).to_bytes(4, "little", signed=signed)
    if byte_count == 8:
        return int(scaled).to_bytes(8, "little", signed=signed)
    return None


def _select_best_format_for_series(values: list[Any]) -> int:
    if not values:
        raise ValueError("Cannot select a format for an empty series")

    if all(isinstance(v, str) for v in values):
        max_len = max(len(v.encode("utf-8")) for v in values)
        if max_len <= 0xFF:
            return FORMAT_STRING_U8
        if max_len <= 0xFFFF:
            return FORMAT_STRING_U16
        if max_len <= 0xFFFFFFFF:
            return FORMAT_STRING_U32
        return FORMAT_STRING_U64

    numeric_values: list[float] = []
    for value in values:
        if isinstance(value, bool) or isinstance(value, str):
            raise ValueError(f"Mixed or unsupported value types in series: {type(value).__name__}")
        numeric_values.append(float(value))

    candidates = [
        0x90, 0x91, 0x92, 0x93,
        0x10, 0x11, 0x12, 0x13,
        0xA0, 0xA1, 0xA2, 0xA3,
        0x20, 0x21, 0x22, 0x23,
        0xB0, 0xB1, 0xB2, 0xB3,
        0x30, 0x31, 0x32, 0x33,
        0xC0, 0xC1, 0xC2, 0xC3,
        0x40, 0x41, 0x42, 0x43,
        FORMAT_FLOAT,
        0xD0, 0xD1, 0xD2, 0xD3,
        0x50, 0x51, 0x52, 0x53,
        FORMAT_DOUBLE,
    ]
    for candidate in candidates:
        if all(_encode_value_for_format(v, candidate) is not None for v in numeric_values):
            return candidate
    return FORMAT_DOUBLE


def compress_timeseries_db_file(input_path: str, output_path: str) -> dict[str, int]:
    db = read_timeseries_db(input_path)
    events = db.iter_events()
    if not events:
        raise ValueError("Input TSDB file contains no values")

    per_series_values: dict[str, list[Any]] = {}
    for _ts, series_name, value in events:
        per_series_values.setdefault(series_name, []).append(value)

    chosen_formats = {name: _select_best_format_for_series(values) for name, values in per_series_values.items()}
    first_seen_order: list[str] = []
    seen: set[str] = set()
    for _ts, series_name, _value in events:
        if series_name not in seen:
            seen.add(series_name)
            first_seen_order.append(series_name)
    series_to_channel: dict[str, int] = {name: idx for idx, name in enumerate(first_seen_order)}

    current_ts: Optional[int] = None
    with open(output_path, "wb") as out:
        out.write(TSDB_TAG_BYTES)
        out.write(struct.pack("<I", TSDB_VERSION))

        for series_name in first_seen_order:
            channel_id = series_to_channel[series_name]
            format_id = chosen_formats[series_name]
            name_bytes = series_name.encode("utf-8")
            if len(name_bytes) > 255:
                raise ValueError(f"Series name too long ({len(name_bytes)} bytes > 255): {series_name!r}")
            if channel_id <= 0xEF:
                out.write(bytes([ENTRY_TYPE_CHANNEL_DEF_8, channel_id, format_id, len(name_bytes)]))
            else:
                out.write(bytes([ENTRY_TYPE_CHANNEL_DEF_16]))
                out.write(channel_id.to_bytes(2, "little"))
                out.write(bytes([format_id, len(name_bytes)]))
            out.write(name_bytes)

        for ts, series_name, value in events:
            ts_int = int(ts)
            if current_ts is None or ts_int < current_ts:
                out.write(bytes([ENTRY_TYPE_TIME_ABSOLUTE]))
                out.write(ts_int.to_bytes(8, "little"))
            else:
                delta = ts_int - current_ts
                if delta <= 0xFF:
                    if delta != 0:
                        out.write(bytes([ENTRY_TYPE_TIME_REL_8, delta]))
                elif delta <= 0xFFFF:
                    out.write(bytes([ENTRY_TYPE_TIME_REL_16]))
                    out.write(delta.to_bytes(2, "little"))
                elif delta <= 0xFFFFFF:
                    out.write(bytes([ENTRY_TYPE_TIME_REL_24]))
                    out.write(delta.to_bytes(3, "little"))
                elif delta <= 0xFFFFFFFF:
                    out.write(bytes([ENTRY_TYPE_TIME_REL_32]))
                    out.write(delta.to_bytes(4, "little"))
                else:
                    out.write(bytes([ENTRY_TYPE_TIME_ABSOLUTE]))
                    out.write(ts_int.to_bytes(8, "little"))
            current_ts = ts_int

            channel_id = series_to_channel[series_name]
            format_id = chosen_formats[series_name]
            payload = _encode_value_for_format(value, format_id)
            if payload is None:
                raise ValueError(f"Cannot encode value for series={series_name!r} with formatId=0x{format_id:02x}")

            if channel_id <= 0xEF:
                out.write(bytes([channel_id]))
            else:
                out.write(bytes([ENTRY_TYPE_CHANNEL_VALUE_16]))
                out.write(channel_id.to_bytes(2, "little"))
            out.write(payload)

        out.write(bytes([ENTRY_TYPE_EOF]))

    return chosen_formats


@dataclasses.dataclass
class _TsdbAppendState:
    series_to_channel: dict[str, int]
    series_to_format: dict[str, int]
    next_channel_id: int
    current_timestamp_ms: Optional[int]
    latest_name_values: dict[str, str]


def _scan_tsdb_state_for_append(path: str) -> _TsdbAppendState:
    if not os.path.exists(path):
        return _TsdbAppendState({}, {}, 0, None, {})
    with open(path, "rb") as f:
        raw = f.read()
    if len(raw) < 12:
        raise ValueError(f"File too small: {path}")
    if raw[:8] != TSDB_TAG_BYTES:
        raise ValueError(f"Invalid TSDB tag in {path!r}")
    version = int.from_bytes(raw[8:12], "little")
    if version != TSDB_VERSION:
        raise ValueError(f"Unsupported TSDB version {version} in {path!r}")

    channel_defs: dict[int, tuple[int, str]] = {}
    current_ts: Optional[int] = None
    latest_name_values: dict[str, str] = {}
    offset = 12
    while offset < len(raw):
        entry_type = raw[offset]
        offset += 1
        if entry_type <= 0xEF:
            channel_id = entry_type
            if channel_id not in channel_defs:
                raise ValueError(f"Undefined channel id {channel_id}")
            fmt, name = channel_defs[channel_id]
            value, offset = _read_format_value(raw, offset, fmt)
            if name.endswith("/name") and isinstance(value, str):
                latest_name_values[name] = value
            continue
        if entry_type == ENTRY_TYPE_CHANNEL_VALUE_16:
            _ensure_available(raw, offset, 2, "16-bit channel id")
            channel_id = int.from_bytes(raw[offset:offset + 2], "little")
            offset += 2
            if channel_id not in channel_defs:
                raise ValueError(f"Undefined 16-bit channel id {channel_id}")
            fmt, name = channel_defs[channel_id]
            value, offset = _read_format_value(raw, offset, fmt)
            if name.endswith("/name") and isinstance(value, str):
                latest_name_values[name] = value
            continue
        if entry_type == ENTRY_TYPE_TIME_ABSOLUTE:
            _ensure_available(raw, offset, 8, "absolute timestamp")
            current_ts = int.from_bytes(raw[offset:offset + 8], "little")
            offset += 8
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_8:
            _ensure_available(raw, offset, 1, "relative timestamp (8-bit)")
            rel = raw[offset]
            offset += 1
            if current_ts is None:
                raise ValueError("Relative timestamp before absolute timestamp")
            current_ts += rel
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_16:
            _ensure_available(raw, offset, 2, "relative timestamp (16-bit)")
            rel = int.from_bytes(raw[offset:offset + 2], "little")
            offset += 2
            if current_ts is None:
                raise ValueError("Relative timestamp before absolute timestamp")
            current_ts += rel
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_24:
            rel, offset = _read_u24(raw, offset)
            if current_ts is None:
                raise ValueError("Relative timestamp before absolute timestamp")
            current_ts += rel
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_32:
            _ensure_available(raw, offset, 4, "relative timestamp (32-bit)")
            rel = int.from_bytes(raw[offset:offset + 4], "little")
            offset += 4
            if current_ts is None:
                raise ValueError("Relative timestamp before absolute timestamp")
            current_ts += rel
            continue
        if entry_type == ENTRY_TYPE_CHANNEL_DEF_8:
            _ensure_available(raw, offset, 3, "8-bit channel definition")
            channel_id = raw[offset]
            format_id = raw[offset + 1]
            name_len = raw[offset + 2]
            offset += 3
            _ensure_available(raw, offset, name_len, "channel name")
            name = raw[offset:offset + name_len].decode("utf-8")
            offset += name_len
            channel_defs[channel_id] = (format_id, name)
            continue
        if entry_type == ENTRY_TYPE_CHANNEL_DEF_16:
            _ensure_available(raw, offset, 4, "16-bit channel definition")
            channel_id = int.from_bytes(raw[offset:offset + 2], "little")
            format_id = raw[offset + 2]
            name_len = raw[offset + 3]
            offset += 4
            _ensure_available(raw, offset, name_len, "channel name")
            name = raw[offset:offset + name_len].decode("utf-8")
            offset += name_len
            channel_defs[channel_id] = (format_id, name)
            continue
        if entry_type == ENTRY_TYPE_EOF:
            break
        raise ValueError(f"Unknown entry type 0x{entry_type:02x} at offset {offset - 1}")

    series_to_channel: dict[str, int] = {}
    series_to_format: dict[str, int] = {}
    for channel_id, (format_id, name) in channel_defs.items():
        series_to_channel[name] = channel_id
        series_to_format[name] = format_id
    next_channel_id = (max(channel_defs.keys()) + 1) if channel_defs else 0
    return _TsdbAppendState(series_to_channel, series_to_format, next_channel_id, current_ts, latest_name_values)


def _append_timestamp_entry(f: Any, current_ts: Optional[int], new_ts: int) -> int:
    if current_ts is None or new_ts < current_ts:
        f.write(bytes([ENTRY_TYPE_TIME_ABSOLUTE]))
        f.write(new_ts.to_bytes(8, "little"))
        return new_ts
    delta = new_ts - current_ts
    if delta == 0:
        return new_ts
    if delta <= 0xFF:
        f.write(bytes([ENTRY_TYPE_TIME_REL_8, delta]))
    elif delta <= 0xFFFF:
        f.write(bytes([ENTRY_TYPE_TIME_REL_16]))
        f.write(delta.to_bytes(2, "little"))
    elif delta <= 0xFFFFFF:
        f.write(bytes([ENTRY_TYPE_TIME_REL_24]))
        f.write(delta.to_bytes(3, "little"))
    elif delta <= 0xFFFFFFFF:
        f.write(bytes([ENTRY_TYPE_TIME_REL_32]))
        f.write(delta.to_bytes(4, "little"))
    else:
        f.write(bytes([ENTRY_TYPE_TIME_ABSOLUTE]))
        f.write(new_ts.to_bytes(8, "little"))
    return new_ts


class TimeSeriesDbAppender:
    def __init__(self, path: str) -> None:
        self.path = path
        state = _scan_tsdb_state_for_append(path) if os.path.exists(path) else _TsdbAppendState({}, {}, 0, None, {})
        self.series_to_channel = dict(state.series_to_channel)
        self.series_to_format = dict(state.series_to_format)
        self.next_channel_id = state.next_channel_id
        self.current_timestamp_ms = state.current_timestamp_ms
        self.latest_name_values = dict(state.latest_name_values)

    def _ensure_file_ready(self) -> None:
        if not os.path.exists(self.path):
            with open(self.path, "wb") as f:
                f.write(TSDB_TAG_BYTES)
                f.write(struct.pack("<I", TSDB_VERSION))
            return
        with open(self.path, "rb+") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0:
                f.write(TSDB_TAG_BYTES)
                f.write(struct.pack("<I", TSDB_VERSION))
                return
            f.seek(-1, os.SEEK_END)
            last = f.read(1)
            if last == bytes([ENTRY_TYPE_EOF]):
                f.seek(-1, os.SEEK_END)
                f.truncate()

    def _ensure_series_definition(self, f: Any, series_name: str, format_id: int) -> int:
        if series_name in self.series_to_channel:
            existing_fmt = self.series_to_format[series_name]
            if existing_fmt != format_id:
                raise ValueError(
                    f"Series {series_name!r} already uses formatId=0x{existing_fmt:02x}; "
                    f"cannot append formatId=0x{format_id:02x}"
                )
            return self.series_to_channel[series_name]
        channel_id = self.next_channel_id
        if channel_id > 0xFFFF:
            raise ValueError("Exceeded max channel id (65535)")
        self.next_channel_id += 1
        name_bytes = series_name.encode("utf-8")
        if len(name_bytes) > 255:
            raise ValueError(f"Series name too long ({len(name_bytes)} bytes > 255): {series_name!r}")
        if channel_id <= 0xEF:
            f.write(bytes([ENTRY_TYPE_CHANNEL_DEF_8, channel_id, format_id, len(name_bytes)]))
        else:
            f.write(bytes([ENTRY_TYPE_CHANNEL_DEF_16]))
            f.write(channel_id.to_bytes(2, "little"))
            f.write(bytes([format_id, len(name_bytes)]))
        f.write(name_bytes)
        self.series_to_channel[series_name] = channel_id
        self.series_to_format[series_name] = format_id
        return channel_id

    def append_events(self, events: list[tuple[int, str, Any]]) -> None:
        if not events:
            return
        self._ensure_file_ready()
        with open(self.path, "ab") as f:
            for timestamp_ms, series_name, value in events:
                if (
                    series_name.endswith("/name")
                    and isinstance(value, str)
                    and self.latest_name_values.get(series_name) == value
                ):
                    continue
                if isinstance(value, str):
                    format_id = FORMAT_STRING_U64
                    payload = _encode_value_for_format(value, format_id)
                else:
                    format_id = FORMAT_DOUBLE
                    payload = _encode_value_for_format(float(value), format_id)
                if payload is None:
                    raise ValueError(f"Cannot encode value for series={series_name!r}")

                channel_id = self._ensure_series_definition(f, series_name, format_id)
                self.current_timestamp_ms = _append_timestamp_entry(f, self.current_timestamp_ms, int(timestamp_ms))
                if channel_id <= 0xEF:
                    f.write(bytes([channel_id]))
                else:
                    f.write(bytes([ENTRY_TYPE_CHANNEL_VALUE_16]))
                    f.write(channel_id.to_bytes(2, "little"))
                f.write(payload)
                if series_name.endswith("/name") and isinstance(value, str):
                    self.latest_name_values[series_name] = value


def _tsdb_filename_for_utc_day(day: datetime.date) -> str:
    return f"data_{day.isoformat()}.tsdb"


def _value_from_mqtt_payload(payload: bytes) -> Any:
    try:
        text = payload.decode("utf-8-sig").strip()
    except Exception:
        return f"hex:{payload.hex()}"
    numeric = _parse_strict_float(text)
    if numeric is not None:
        return numeric
    return text


def _quantize_timestamp_ms(timestamp_ms: int, quantize_timestamps_ms: int) -> int:
    if quantize_timestamps_ms <= 0:
        return timestamp_ms
    return (timestamp_ms // quantize_timestamps_ms) * quantize_timestamps_ms


def collect_to_tsdb(server: str, subscriptions: list[str], verbose: int, quantize_timestamps_ms: int = 0) -> int:
    try:
        import paho.mqtt.client as mqtt
    except Exception:
        print("paho-mqtt is required. Install with: pip install paho-mqtt")
        return 2
    if not subscriptions:
        print("--collect requires at least one topic via --topics or config")
        return 2
    host, port = parse_host_port(server)
    if not hasattr(mqtt, "CallbackAPIVersion"):
        print("paho-mqtt v2 is required. Please upgrade paho-mqtt.")
        return 2

    queue: list[tuple[int, str, Any]] = []
    lock = threading.Lock()
    appenders: dict[datetime.date, TimeSeriesDbAppender] = {}

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

    def on_connect(client, userdata, flags, reason_code, properties=None):
        try:
            rc = int(reason_code)
        except Exception:
            rc = reason_code
        if rc != 0:
            print(f"Failed to connect to MQTT server (rc={rc})")
            return
        if verbose:
            print(f"connected to MQTT server {host}:{port}")
        for topic in subscriptions:
            client.subscribe(topic)
            if verbose:
                print(f"subscribed: {topic}")

    def on_message(client, userdata, msg):
        ts_ms = int(time.time() * 1000)
        ts_ms = _quantize_timestamp_ms(ts_ms, quantize_timestamps_ms)
        value = _value_from_mqtt_payload(msg.payload)
        if verbose >= 2:
            print(f"received: {msg.topic}={value}")
        with lock:
            queue.append((ts_ms, msg.topic, value))

    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(host, port, keepalive=30)
    except Exception as exc:
        print(f"Unable to connect to MQTT server {host}:{port}: {exc}")
        return 2

    def flush_batch(batch: list[tuple[int, str, Any]]) -> None:
        by_day: dict[datetime.date, list[tuple[int, str, Any]]] = {}
        for ts_ms, topic, value in batch:
            day = datetime.datetime.fromtimestamp(ts_ms / 1000.0, tz=datetime.timezone.utc).date()
            by_day.setdefault(day, []).append((ts_ms, topic, value))
        for day, day_events in by_day.items():
            if day not in appenders:
                path = _tsdb_filename_for_utc_day(day)
                if verbose:
                    action = "opening existing" if os.path.exists(path) else "creating new"
                    print(f"{action} TSDB file: {path}")
                appenders[day] = TimeSeriesDbAppender(path)
            appenders[day].append_events(day_events)
        if verbose and batch:
            print(f"flushed {len(batch)} events")

    client.loop_start()
    last_flush = time.monotonic()
    try:
        while True:
            now = time.monotonic()
            if now - last_flush >= 10.0:
                with lock:
                    batch = list(queue)
                    queue.clear()
                flush_batch(batch)
                last_flush = now
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        with lock:
            batch = list(queue)
            queue.clear()
        flush_batch(batch)
        client.loop_stop()
        client.disconnect()
    return 0


def _parse_strict_float(text: str) -> Optional[float]:
    if re.fullmatch(r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?", text.strip()) is None:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _decimal_places_from_literal(text: str) -> int:
    value = text.strip()
    if "e" in value.lower():
        value = value.lower().split("e", 1)[0]
    if "." not in value:
        return 0
    return len(value.split(".", 1)[1])


def _quantize_numeric(value: float, decimal_places: int) -> float:
    if decimal_places <= 0:
        return float(round(value))
    return round(value, decimal_places)


def _load_demo_series(data_txt_path: str) -> list[tuple[str, Any, bool, int]]:
    series: list[tuple[str, Any, bool, int]] = []
    with open(data_txt_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            name, value_raw = line.split("=", 1)
            value_num = _parse_strict_float(value_raw)
            if value_num is None:
                series.append((name, value_raw, False, 0))
            else:
                series.append((name, value_num, True, _decimal_places_from_literal(value_raw)))
    return series


def _metric_suffix(series_name: str) -> str:
    return series_name.split("/")[-1].lower()


def _range_for_series(series_name: str, base: float) -> tuple[float, float]:
    suffix = _metric_suffix(series_name)
    if "powerfactor" in suffix:
        return 0.85, 1.0
    if "frequency" in suffix:
        return 49.8, 50.2
    if "temperature" in suffix:
        return -5.0, 75.0
    if suffix == "voltage":
        if "/0/" in series_name or "/ac/" in series_name:
            return 210.0, 250.0
        return 10.0, 60.0
    if suffix == "current":
        return 0.0, 15.0
    if suffix == "powerdc":
        return 0.0, 2600.0
    if suffix == "power":
        return 0.0, 2500.0
    if suffix == "irradiation":
        return 0.0, 1.2
    if suffix == "efficiency":
        return 0.0, 98.0
    if suffix == "reactivepower":
        return -400.0, 400.0
    if suffix == "rssi":
        return -95.0, -20.0
    if suffix.startswith("limit_") or suffix.endswith("limit_absolute") or suffix.endswith("limit_relative"):
        return 0.0, max(2000.0, base * 1.05)
    if suffix in {"producing", "reachable", "is_valid"}:
        return 0.0, 1.0
    if suffix in {"yieldday", "yieldtotal"}:
        return 0.0, max(10.0, base)
    if suffix == "uptime":
        return max(0.0, base), base + 86400.0
    if suffix.startswith("rx_") or suffix.startswith("tx_") or suffix in {"heap/free", "heap/maxalloc", "heap/minfree", "heap/size"}:
        return max(0.0, base * 0.5), max(10.0, base * 1.5)
    if suffix in {"bootloaderversion", "fwbuildversion", "hwpartnumber", "status/last_update"}:
        return max(0.0, base), base
    low = min(base * 0.5, base * 1.5)
    high = max(base * 0.5, base * 1.5)
    if abs(base) < 1.0:
        low, high = -1.0, 1.0
    return low, high


def _bounded_sin(min_value: float, max_value: float, phase: float, periods_per_day: int, day_fraction: float) -> float:
    mid = (min_value + max_value) * 0.5
    amp = (max_value - min_value) * 0.5
    value = mid + amp * math.sin((2.0 * math.pi * periods_per_day * day_fraction) + phase)
    return min(max(value, min_value), max_value)


def generateDemoData(days: int, output_dir: str = ".", data_txt_path: Optional[str] = None) -> list[str]:
    if days <= 0:
        raise ValueError(f"days must be > 0, got {days}")
    if data_txt_path is None:
        data_txt_path = os.path.join(os.path.dirname(__file__), "data.txt")

    series = _load_demo_series(data_txt_path)
    if not series:
        raise ValueError(f"No series found in {data_txt_path!r}")

    os.makedirs(output_dir, exist_ok=True)
    steps_per_day = 24 * 12  # 5-minute intervals
    step_ms = 5 * 60 * 1000
    step_hours = 5.0 / 60.0
    day_ms = 24 * 60 * 60 * 1000

    today = datetime.datetime.now(datetime.timezone.utc).date()
    start_day = today - datetime.timedelta(days=days - 1)

    yieldtotal_series = {name for name, _base, is_num, _dp in series if is_num and _metric_suffix(name) == "yieldtotal"}
    yieldday_series = {name for name, _base, is_num, _dp in series if is_num and _metric_suffix(name) == "yieldday"}

    base_numeric: dict[str, float] = {
        name: float(base) for name, base, is_num, _dp in series if is_num
    }
    series_decimals: dict[str, int] = {
        name: dp for name, _base, is_num, dp in series if is_num
    }
    cumulative_yieldtotal: dict[str, float] = {
        name: max(0.0, base_numeric.get(name, 0.0)) for name in yieldtotal_series
    }

    produced_files: list[str] = []
    for day_index in range(days):
        day = start_day + datetime.timedelta(days=day_index)
        start_dt = datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc)
        start_ms = int(start_dt.timestamp() * 1000)
        path = os.path.join(output_dir, f"data_{day.isoformat()}.tsdb")
        produced_files.append(path)

        daily_yields: dict[str, float] = {name: 0.0 for name in yieldday_series}
        with create_timeseries_db_writer(path) as writer:
            for step_idx in range(steps_per_day):
                ts = start_ms + step_idx * step_ms
                day_fraction = step_idx / steps_per_day

                numeric_cache: dict[str, float] = {}
                for idx, (name, base_value, is_num, decimal_places) in enumerate(series):
                    if not is_num:
                        writer.addStringValue(name, str(base_value), timestamp_ms=ts)
                        continue

                    suffix = _metric_suffix(name)
                    if suffix in {"yieldday", "yieldtotal"}:
                        continue

                    periods = (idx % 24) + 1
                    phase = (idx * 0.73) + (day_index * 0.11)
                    min_v, max_v = _range_for_series(name, float(base_value))
                    if suffix in {"producing", "reachable", "is_valid"}:
                        raw = _bounded_sin(min_v, max_v, phase, periods, day_fraction)
                        value = 1.0 if raw >= 0.5 else 0.0
                    elif suffix == "uptime":
                        value = float(base_value) + (step_idx * step_hours * 3600.0)
                    else:
                        value = _bounded_sin(min_v, max_v, phase, periods, day_fraction)
                    value = _quantize_numeric(value, decimal_places)
                    numeric_cache[name] = value
                    writer.addValue(name, float(value), timestamp_ms=ts)

                for name in sorted(yieldday_series):
                    power_series = name.replace("/yieldday", "/power")
                    power_w = max(0.0, float(numeric_cache.get(power_series, 0.0)))
                    writer.addValue(name, _quantize_numeric(daily_yields[name], series_decimals.get(name, 3)), timestamp_ms=ts)
                    daily_yields[name] += (power_w * step_hours) / 1000.0

                for name in sorted(yieldtotal_series):
                    power_series = name.replace("/yieldtotal", "/power")
                    power_w = max(0.0, float(numeric_cache.get(power_series, 0.0)))
                    current_total = cumulative_yieldtotal.get(name, 0.0)
                    writer.addValue(name, _quantize_numeric(current_total, series_decimals.get(name, 3)), timestamp_ms=ts)
                    cumulative_yieldtotal[name] = current_total + (power_w * step_hours) / 1000.0

            writer.close(mark_complete=True)

    return produced_files




def read_default_server(rc_path: str) -> Optional[str]:
    if not os.path.exists(rc_path):
        return None
    try:
        try:
            import tomllib  # Python 3.11+
        except Exception:  # pragma: no cover - fallback if tomllib missing
            import toml as tomllib  # type: ignore
    except Exception:
        return None
    try:
        with open(rc_path, "rb") as f:
            data = tomllib.load(f)
    except OSError:
        return None
    except Exception:
        return None
    for key in ("mqtt_server", "mqtt-server", "server"):
        if key in data:
            return str(data[key]).strip()
    mqtt_block = data.get("mqtt")
    if isinstance(mqtt_block, dict):
        for key in ("mqtt_server", "mqtt-server", "server"):
            if key in mqtt_block:
                return str(mqtt_block[key]).strip()
    return None


def read_default_topics(rc_path: str) -> list[str]:
    if not os.path.exists(rc_path):
        return []
    try:
        try:
            import tomllib  # Python 3.11+
        except Exception:  # pragma: no cover - fallback if tomllib missing
            import toml as tomllib  # type: ignore
    except Exception:
        return []
    try:
        with open(rc_path, "rb") as f:
            data = tomllib.load(f)
    except OSError:
        return []
    except Exception:
        return []

    def normalize_topics(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str) and value.strip():
            return [value.strip()]
        return []

    for key in ("topics", "mqtt_topics", "mqtt-topics"):
        if key in data:
            topics = normalize_topics(data[key])
            if topics:
                return topics
    mqtt_block = data.get("mqtt")
    if isinstance(mqtt_block, dict):
        for key in ("topics", "mqtt_topics", "mqtt-topics"):
            if key in mqtt_block:
                topics = normalize_topics(mqtt_block[key])
                if topics:
                    return topics
    return []


def read_default_quantize_timestamps(rc_path: str) -> int:
    if not os.path.exists(rc_path):
        return 0
    try:
        try:
            import tomllib  # Python 3.11+
        except Exception:  # pragma: no cover - fallback if tomllib missing
            import toml as tomllib  # type: ignore
    except Exception:
        return 0
    try:
        with open(rc_path, "rb") as f:
            data = tomllib.load(f)
    except OSError:
        return 0
    except Exception:
        return 0

    def normalize_quantize(value: Any) -> int:
        try:
            parsed = int(value)
        except Exception:
            return 0
        return max(0, parsed)

    for key in ("quantize_timestamps", "quantize-timestamps"):
        if key in data:
            return normalize_quantize(data[key])
    mqtt_block = data.get("mqtt")
    if isinstance(mqtt_block, dict):
        for key in ("quantize_timestamps", "quantize-timestamps"):
            if key in mqtt_block:
                return normalize_quantize(mqtt_block[key])
    return 0


def parse_host_port(server: str) -> tuple[str, int]:
    if ":" in server:
        host, port_str = server.rsplit(":", 1)
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(f"Invalid port in --mqtt-server: {server}")
        return host, port
    return server, 1883


def flatten_json(raw: str) -> tuple[Optional[dict[str, str]], Optional[str]]:
    try:
        data = json.loads(raw)
    except Exception as exc:
        return None, f"{exc.__class__.__name__}: {exc}"
    if not isinstance(data, dict):
        return None, None
    flat: dict[str, str] = {}

    def walk(prefix: str, value) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                key = f"{prefix}.{k}" if prefix else str(k)
                walk(key, v)
        else:
            flat[prefix] = json.dumps(value) if not isinstance(value, str) else value

    walk("", data)
    return flat, None


def list_topics(server: str, timeout: float, verbose: int, topic_filter: Optional[str], flatten: bool, monitor: bool) -> int:
    try:
        import paho.mqtt.client as mqtt
    except Exception as exc:  # pragma: no cover - dependency presence is runtime
        print("paho-mqtt is required. Install with: pip install paho-mqtt")
        return 2

    host, port = parse_host_port(server)
    topics: set[str] = set()
    latest_message: dict[str, bytes] = {}
    latest_meta: dict[str, dict[str, str]] = {}

    if not hasattr(mqtt, "CallbackAPIVersion"):
        print("paho-mqtt v2 is required. Please upgrade paho-mqtt.")
        return 2
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

    def on_connect(client, userdata, flags, reason_code, properties=None):
        try:
            rc = int(reason_code)
        except Exception:
            rc = reason_code
        if rc != 0:
            print(f"Failed to connect to MQTT server (rc={rc})")
            client.disconnect()
            return
        client.subscribe("#")

    def on_message(client, userdata, msg):
        if topic_filter and not fnmatch.fnmatch(msg.topic, topic_filter):
            return
        topics.add(msg.topic)
        latest_message[msg.topic] = msg.payload
        if verbose:
            meta: dict[str, str] = {}
            meta["received_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + f".{int((time.time() % 1) * 1000):03d}"
            try:
                meta["qos"] = str(msg.qos)
                meta["retain"] = str(msg.retain)
                meta["dup"] = str(msg.dup)
                meta["mid"] = str(msg.mid)
            except Exception:
                pass
            try:
                meta["payload_len"] = str(len(msg.payload))
            except Exception:
                pass
            if hasattr(msg, "properties") and msg.properties is not None:
                try:
                    meta["properties"] = str(msg.properties)
                except Exception:
                    pass
            latest_meta[msg.topic] = meta
        if monitor:
            emit_topic(msg.topic, msg.payload, verbose, flatten, latest_meta.get(msg.topic))

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(host, port, keepalive=30)
    except Exception as exc:
        print(f"Unable to connect to MQTT server {host}:{port}: {exc}")
        return 2

    client.loop_start()
    if monitor:
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    else:
        time.sleep(max(0.1, timeout))
    client.loop_stop()
    client.disconnect()

    if not monitor:
        for topic in sorted(topics):
            if topic_filter and not fnmatch.fnmatch(topic, topic_filter):
                continue
            payload = latest_message.get(topic)
            if payload is None:
                print(f"{topic}=")
                continue
            emit_topic(topic, payload, verbose, flatten, latest_meta.get(topic))

    return 0


def emit_topic(topic: str, payload: bytes, verbose: int, flatten: bool, meta: Optional[dict[str, str]]) -> None:
    try:
        decoded = payload.decode("utf-8-sig")
        stripped = decoded.lstrip()
        if flatten and stripped.startswith("{"):
            try:
                flattened, error = flatten_json(decoded)
            except Exception as exc:
                print(f"{topic}={decoded}")
                print(f"{topic}._json_error={exc.__class__.__name__}: {exc}")
            else:
                if flattened is None:
                    print(f"{topic}={decoded}")
                    if error:
                        print(f"{topic}._json_error={error}")
                else:
                    for key in sorted(flattened):
                        print(f"{topic}.{key}={flattened[key]}")
        else:
            print(f"{topic}={decoded}")
    except Exception:
        print(f"{topic}=hex:{payload.hex()}")
    if verbose and meta:
        for key in sorted(meta):
            print(f"{topic}._meta.{key}={meta[key]}")


def parse_number(value: str) -> Optional[float]:
    match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", value)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def format_number(value: float) -> str:
    text = f"{value:.3f}"
    text = text.rstrip("0").rstrip(".")
    return text


def open_dtu_summary(server: str, timeout: float, topic_filter: Optional[str], verbose: int) -> int:
    try:
        import paho.mqtt.client as mqtt
    except Exception as exc:  # pragma: no cover - dependency presence is runtime
        print("paho-mqtt is required. Install with: pip install paho-mqtt")
        return 2

    host, port = parse_host_port(server)
    latest_message: dict[str, bytes] = {}

    if not hasattr(mqtt, "CallbackAPIVersion"):
        print("paho-mqtt v2 is required. Please upgrade paho-mqtt.")
        return 2
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

    def on_connect(client, userdata, flags, reason_code, properties=None):
        try:
            rc = int(reason_code)
        except Exception:
            rc = reason_code
        if rc != 0:
            print(f"Failed to connect to MQTT server (rc={rc})")
            client.disconnect()
            return
        client.subscribe("solar/#")

    def on_message(client, userdata, msg):
        if topic_filter and not fnmatch.fnmatch(msg.topic, topic_filter):
            return
        latest_message[msg.topic] = msg.payload

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(host, port, keepalive=30)
    except Exception as exc:
        print(f"Unable to connect to MQTT server {host}:{port}: {exc}")
        return 2

    client.loop_start()
    time.sleep(max(0.1, timeout))
    client.loop_stop()
    client.disconnect()

    inverter_names: dict[str, str] = {}
    inverter_yieldday: dict[str, float] = {}
    inverter_yieldtotal: dict[str, float] = {}
    ac_yieldday: Optional[float] = None
    ac_yieldtotal: Optional[float] = None
    seen_yield_topics: list[str] = []
    debug_yield_samples: list[str] = []

    for topic, payload in latest_message.items():
        if not topic.startswith("solar/"):
            continue
        try:
            decoded = payload.decode("utf-8-sig").strip()
        except Exception:
            continue

        name_match = re.match(r"^solar/([^/]+)/name$", topic)
        if name_match:
            inverter_names[name_match.group(1)] = decoded
            continue

        ac_match = re.match(r"^solar/ac/(yieldday|yieldtotal)$", topic)
        if ac_match:
            metric = ac_match.group(1)
            number = parse_number(decoded)
            if number is not None:
                if metric == "yieldday":
                    ac_yieldday = number
                else:
                    ac_yieldtotal = number
                seen_yield_topics.append(topic)
            else:
                if len(debug_yield_samples) < 5:
                    debug_yield_samples.append(f"{topic}={decoded} (ac parse_number failed)")
            continue

        inv_match = re.match(r"^solar/([^/]+)/\\d+/(yieldday|yieldtotal)$", topic)
        if inv_match:
            inverter_id = inv_match.group(1)
            metric = inv_match.group(2)
            number = parse_number(decoded)
            if number is None:
                if len(debug_yield_samples) < 5:
                    debug_yield_samples.append(f"{topic}={decoded} (inv parse_number failed)")
                continue
            seen_yield_topics.append(topic)
            if metric == "yieldday":
                inverter_yieldday[inverter_id] = inverter_yieldday.get(inverter_id, 0.0) + number
            else:
                inverter_yieldtotal[inverter_id] = inverter_yieldtotal.get(inverter_id, 0.0) + number
            continue

        parts = topic.split("/")
        if len(parts) < 2 or parts[0] != "solar":
            continue
        is_ac = parts[1] == "ac"
        inverter_id = None if is_ac else parts[1]

        keys_values: list[tuple[str, str]] = []
        if decoded.startswith("{"):
            flattened, _error = flatten_json(decoded)
            if flattened:
                keys_values.extend(flattened.items())
        else:
            keys_values.append((parts[-1], decoded))

        for key, value in keys_values:
            key_l = key.lower()
            if key_l.endswith("yieldday") or key_l.endswith("yieldtotal"):
                seen_yield_topics.append(topic)
                number = parse_number(value)
                if number is None:
                    if len(debug_yield_samples) < 5:
                        debug_yield_samples.append(f"{topic}={value} (fallback parse_number failed)")
                    continue
                if is_ac:
                    if key_l.endswith("yieldday"):
                        ac_yieldday = number
                    else:
                        ac_yieldtotal = number
                else:
                    if key_l.endswith("yieldday"):
                        inverter_yieldday[inverter_id] = inverter_yieldday.get(inverter_id, 0.0) + number
                    else:
                        inverter_yieldtotal[inverter_id] = inverter_yieldtotal.get(inverter_id, 0.0) + number

    for inverter_id in sorted(inverter_names.keys() | inverter_yieldday.keys() | inverter_yieldtotal.keys()):
        inverter_label = inverter_names.get(inverter_id, inverter_id)
        yieldday = inverter_yieldday.get(inverter_id)
        yieldtotal = inverter_yieldtotal.get(inverter_id)
        parts = [f"inverter={inverter_label}"]
        if yieldday is not None:
            parts.append(f"yieldday={format_number(yieldday)}")
        if yieldtotal is not None:
            parts.append(f"yieldtotal={format_number(yieldtotal)}")
        print(" ".join(parts))

    summary_parts = ["ac"]
    if ac_yieldday is not None:
        summary_parts.append(f"yieldday={format_number(ac_yieldday)}")
    if ac_yieldtotal is not None:
        summary_parts.append(f"yieldtotal={format_number(ac_yieldtotal)}")
    print(" ".join(summary_parts))
    if verbose:
        solar_topics = [t for t in latest_message if t.startswith("solar/")]
        print(f"debug: received {len(solar_topics)} solar topics, {len(seen_yield_topics)} yield topics")
        if not seen_yield_topics:
            for topic in sorted(solar_topics)[:20]:
                print(f"debug: topic={topic}")
        if debug_yield_samples:
            for sample in debug_yield_samples:
                print(f"debug: yield_sample={sample}")
        if ac_yieldday is None or ac_yieldtotal is None:
            print(f"debug: ac_yieldday={ac_yieldday} ac_yieldtotal={ac_yieldtotal}")

    return 0


def persist_server(rc_path: str, server: str) -> None:
    data = {}
    if os.path.exists(rc_path):
        try:
            try:
                import tomllib  # Python 3.11+
            except Exception:  # pragma: no cover - fallback if tomllib missing
                import toml as tomllib  # type: ignore
            with open(rc_path, "rb") as f:
                data = tomllib.load(f) or {}
        except Exception:
            data = {}
    data.pop("server", None)
    data["mqtt_server"] = server
    try:
        import toml  # type: ignore
        with open(rc_path, "w", encoding="utf-8") as f:
            f.write(toml.dumps(data))
        return
    except Exception:
        pass
    try:
        with open(rc_path, "w", encoding="utf-8") as f:
            f.write(f'mqtt_server = "{server}"\n')
    except OSError:
        pass


def _resolve_config_path_for_read() -> str:
    return os.path.expanduser("~/.mqtt_collector.toml")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="MQTT collector utilities.",
    )
    parser.add_argument(
        "--mqtt-server",
        help="MQTT server host[:port]. If omitted, read from ~/.mqtt_collector.toml",
    )
    parser.add_argument("--list-topics", action="store_true", help="List available topics as a flat list with hierarchical names")
    parser.add_argument("--monitor", action="store_true", help="Monitor topics and print messages as they arrive")
    parser.add_argument("--open-dtu-summary", action="store_true", help="Print OpenDTU inverter summary and totals (expects 'solar' root topic)")
    parser.add_argument("-c", "--collect", action="store_true", help="Collect subscribed MQTT topics into current TSDB files")
    parser.add_argument("--topics", action="append", default=[], help="MQTT subscription topic filter (repeatable)")
    parser.add_argument("--dump-tsdb", help="Dump a TimeSeriesDB file in human-readable format")
    parser.add_argument("--generate-demo-db", type=int, metavar="DAYS", help="Generate demo TSDB files for DAYS days")
    parser.add_argument("--compress", metavar="DBFILE", help="Compress a TSDB file in place")
    parser.add_argument("--timeout", type=float, default=1.0, help="Seconds to listen for topics when listing (default: 1.0)")
    parser.add_argument(
        "--quantize-timestamps",
        type=int,
        default=None,
        metavar="MS",
        help="Quantize collect timestamps to MS milliseconds (0 disables). If omitted, read quantize_timestamps from ~/.mqtt_collector.toml.",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity (can be repeated)")
    parser.add_argument("--filter", help="fnmatch pattern to filter topics (e.g. sensors/*)")
    parser.add_argument("--flatten", action="store_true", help="Flatten JSON payloads when listing topics")
    args = parser.parse_args()

    if args.dump_tsdb:
        try:
            if args.verbose:
                read_timeseries_db(args.dump_tsdb, dump_out=sys.stdout, verbose=args.verbose)
                return 0
            db = read_timeseries_db(args.dump_tsdb)
        except Exception as exc:
            print(f"Failed to read DB file {args.dump_tsdb!r}: {exc}")
            return 2
        db.dump()
        return 0
    if args.generate_demo_db is not None:
        try:
            paths = generateDemoData(args.generate_demo_db)
        except Exception as exc:
            print(f"Failed to generate demo DB files: {exc}")
            return 2
        for path in paths:
            print(path)
        return 0
    if args.compress:
        source = args.compress
        if not os.path.exists(source):
            print(f"DB file not found: {source}")
            return 2
        old_size = os.path.getsize(source)
        temp_path = ""
        try:
            with tempfile.NamedTemporaryFile(prefix=".tsdb_compress_", suffix=".tmp", dir=os.path.dirname(source) or ".", delete=False) as tmp:
                temp_path = tmp.name
            compress_timeseries_db_file(source, temp_path)
            new_size = os.path.getsize(temp_path)
            os.replace(temp_path, source)
            if args.verbose:
                gained = old_size - new_size
                gained_pct = (gained / old_size * 100.0) if old_size > 0 else 0.0
                print(f"old_size={old_size} new_size={new_size} gained={gained_pct:.2f}%")
            return 0
        except Exception as exc:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass
            print(f"Failed to compress DB file {source!r}: {exc}")
            return 2

    rc_path = _resolve_config_path_for_read()
    if args.mqtt_server:
        persist_server(rc_path, args.mqtt_server)
    default_server = read_default_server(rc_path)
    default_topics = read_default_topics(rc_path)
    default_quantize_timestamps = read_default_quantize_timestamps(rc_path)
    server = args.mqtt_server or default_server
    quantize_timestamps_ms = args.quantize_timestamps if args.quantize_timestamps is not None else default_quantize_timestamps
    quantize_timestamps_ms = max(0, quantize_timestamps_ms)

    if not server:
        print("MQTT server not specified. Use --mqtt-server or set config in ~/.mqtt_collector.toml")
        return 2

    no_cli_options = len(sys.argv) == 1
    default_to_collect = no_cli_options and bool(default_server)

    if args.list_topics:
        return list_topics(server, args.timeout, args.verbose, args.filter, args.flatten, False)
    if args.monitor:
        return list_topics(server, args.timeout, args.verbose, args.filter, args.flatten, True)
    if args.open_dtu_summary:
        return open_dtu_summary(server, args.timeout, args.filter, args.verbose)
    if args.collect or default_to_collect:
        topics = args.topics if args.topics else default_topics
        return collect_to_tsdb(server, topics, args.verbose, quantize_timestamps_ms=quantize_timestamps_ms)

    print("No action specified. Use --list-topics, --open-dtu-summary, --collect, --dump-tsdb, --generate-demo-db, or --compress.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
