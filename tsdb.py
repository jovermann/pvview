import dataclasses
import datetime
import math
import os
import struct
import sys
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, TextIO, Tuple


TSDB_TAG_BYTES = b"TSDB\x00\x00\x00\x00"
TSDB_VERSION = 1

ENTRY_TYPE_TIME_ABSOLUTE = 0xF0
ENTRY_TYPE_TIME_REL_8 = 0xF1
ENTRY_TYPE_TIME_REL_16 = 0xF2
ENTRY_TYPE_TIME_REL_24 = 0xF3
ENTRY_TYPE_TIME_REL_32 = 0xF4
ENTRY_TYPE_CHANNEL_DEF_8 = 0xF5
ENTRY_TYPE_CHANNEL_DEF_16 = 0xF6
ENTRY_TYPE_META_INFO = 0xF7
ENTRY_TYPE_SERIES_ARRAY = 0xF8
ENTRY_TYPE_VALUE_16 = 0xFF
ENTRY_TYPE_CHANNEL_VALUE_16 = ENTRY_TYPE_VALUE_16

FORMAT_FLOAT = 0x00
FORMAT_DOUBLE = 0x01
FORMAT_DOUBLE_DEC1 = 0x02
FORMAT_DOUBLE_DEC2 = 0x03
FORMAT_DOUBLE_DEC3 = 0x04
FORMAT_DOUBLE_DEC4 = 0x05
FORMAT_DOUBLE_DEC5 = 0x06
FORMAT_DOUBLE_DEC6PLUS = 0x07
FORMAT_STRING_U8 = 0x08
FORMAT_STRING_U16 = 0x09
FORMAT_STRING_U32 = 0x0A
FORMAT_STRING_U64 = 0x0B


@dataclasses.dataclass(frozen=True)
class Event:
    timestamp_ms: int
    value: Any


@dataclasses.dataclass(frozen=True)
class TimeSeriesPoint:
    timestamp_ms: int
    value: Any


class TsdbParseError(ValueError):
    pass


@dataclasses.dataclass
class CachedTsdbFile:
    mtime_ns: int
    size: int
    parsed_offset: int
    current_ts: Optional[int]
    channel_defs: Dict[int, Tuple[int, str]]
    series_format_ids: Dict[str, int]
    series_events: Dict[str, List[Event]]
    meta_info: Dict[str, Any]
    ds_bucket_ms: Optional[int]


_TSDB_CACHE_LOCK = threading.Lock()
_TSDB_FILE_CACHE: Dict[str, CachedTsdbFile] = {}


def invalidate_tsdb_cache(path: Optional[str] = None) -> None:
    with _TSDB_CACHE_LOCK:
        if path is None:
            _TSDB_FILE_CACHE.clear()
        else:
            _TSDB_FILE_CACHE.pop(path, None)


def _ensure_available(data: bytes, offset: int, size: int, what: str) -> None:
    if offset + size > len(data):
        raise TsdbParseError(f"Unexpected EOF while reading {what} at offset {offset}")


def _read_u24(data: bytes, offset: int) -> Tuple[int, int]:
    _ensure_available(data, offset, 3, "uint24")
    b0 = data[offset]
    b1 = data[offset + 1]
    b2 = data[offset + 2]
    return b0 | (b1 << 8) | (b2 << 16), offset + 3


def _read_i24(data: bytes, offset: int) -> Tuple[int, int]:
    value, offset = _read_u24(data, offset)
    if value & 0x800000:
        value -= 1 << 24
    return value, offset


def _read_scalar(data: bytes, offset: int, byte_count: int, signed: bool) -> Tuple[int, int]:
    if byte_count in (1, 2, 4, 8):
        _ensure_available(data, offset, byte_count, f"{byte_count * 8}-bit integer")
        raw = int.from_bytes(data[offset:offset + byte_count], "little", signed=signed)
        return raw, offset + byte_count
    if byte_count == 3:
        return _read_i24(data, offset) if signed else _read_u24(data, offset)
    raise TsdbParseError(f"Unsupported scalar byte_count={byte_count}")


def _read_uleb128(data: bytes, offset: int) -> Tuple[int, int]:
    shift = 0
    value = 0
    while True:
        _ensure_available(data, offset, 1, "ULEB128")
        b = data[offset]
        offset += 1
        value |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return value, offset
        shift += 7
        if shift > 70:
            raise TsdbParseError("ULEB128 too large")


def _write_uleb128(value: int) -> bytes:
    if value < 0:
        raise ValueError("ULEB128 value must be >= 0")
    out = bytearray()
    remaining = int(value)
    while True:
        b = remaining & 0x7F
        remaining >>= 7
        if remaining:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _zigzag_encode(value: int) -> int:
    return value * 2 if value >= 0 else ((-value) * 2 - 1)


def _zigzag_decode(value: int) -> int:
    return (value >> 1) ^ -(value & 1)


def _read_zigzag_leb128(data: bytes, offset: int) -> Tuple[int, int]:
    value, offset = _read_uleb128(data, offset)
    return _zigzag_decode(value), offset


def _write_zigzag_leb128(value: int) -> bytes:
    return _write_uleb128(_zigzag_encode(int(value)))


def read_format_value(data: bytes, offset: int, format_id: int) -> Tuple[Any, int]:
    if format_id == FORMAT_FLOAT:
        _ensure_available(data, offset, 4, "float")
        return struct.unpack_from("<f", data, offset)[0], offset + 4
    if format_id in (
        FORMAT_DOUBLE,
        FORMAT_DOUBLE_DEC1,
        FORMAT_DOUBLE_DEC2,
        FORMAT_DOUBLE_DEC3,
        FORMAT_DOUBLE_DEC4,
        FORMAT_DOUBLE_DEC5,
        FORMAT_DOUBLE_DEC6PLUS,
    ):
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
    byte_count = {
        0x1: 1,
        0x2: 2,
        0x3: 3,
        0x4: 4,
        0x5: 8,
        0x9: 1,
        0xA: 2,
        0xB: 3,
        0xC: 4,
        0xD: 8,
    }.get(hi)
    if byte_count is None or lo > 3:
        raise TsdbParseError(f"Unsupported formatId 0x{format_id:02x}")

    signed = hi <= 0x5
    raw_value, offset = _read_scalar(data, offset, byte_count, signed)
    scale = {0: 1.0, 1: 10.0, 2: 100.0, 3: 1000.0}[lo]
    if scale == 1.0:
        return raw_value, offset
    return raw_value / scale, offset


def _is_incomplete_parse_error(exc: TsdbParseError) -> bool:
    return str(exc).startswith("Unexpected EOF while reading")


def _parse_tsdb_chunk_into_cache(raw: bytes, base_offset: int, cache: CachedTsdbFile, path: str) -> int:
    offset = 0
    while offset < len(raw):
        entry_start = offset
        entry_type = raw[offset]
        offset += 1

        try:
            if entry_type <= 0xEF:
                channel_id = entry_type
                if cache.current_ts is None:
                    raise TsdbParseError("Value entry encountered before timestamp")
                if channel_id not in cache.channel_defs:
                    raise TsdbParseError(f"Undefined channel id {channel_id}")
                format_id, series_name = cache.channel_defs[channel_id]
                if cache.ds_bucket_ms is not None and is_numeric_format_id(format_id):
                    v_min, offset = read_format_value(raw, offset, format_id)
                    v_avg, offset = read_format_value(raw, offset, format_id)
                    v_max, offset = read_format_value(raw, offset, format_id)
                    value = {"min": v_min, "avg": v_avg, "max": v_max}
                else:
                    value, offset = read_format_value(raw, offset, format_id)
                cache.series_events.setdefault(series_name, []).append(Event(cache.current_ts, value))
                continue

            if entry_type == ENTRY_TYPE_VALUE_16:
                _ensure_available(raw, offset, 2, "16-bit channel id")
                channel_id = int.from_bytes(raw[offset:offset + 2], "little")
                offset += 2
                if cache.current_ts is None:
                    raise TsdbParseError("16-bit value entry encountered before timestamp")
                if channel_id not in cache.channel_defs:
                    raise TsdbParseError(f"Undefined 16-bit channel id {channel_id}")
                format_id, series_name = cache.channel_defs[channel_id]
                if cache.ds_bucket_ms is not None and is_numeric_format_id(format_id):
                    v_min, offset = read_format_value(raw, offset, format_id)
                    v_avg, offset = read_format_value(raw, offset, format_id)
                    v_max, offset = read_format_value(raw, offset, format_id)
                    value = {"min": v_min, "avg": v_avg, "max": v_max}
                else:
                    value, offset = read_format_value(raw, offset, format_id)
                cache.series_events.setdefault(series_name, []).append(Event(cache.current_ts, value))
                continue

            if entry_type == ENTRY_TYPE_TIME_ABSOLUTE:
                _ensure_available(raw, offset, 8, "absolute timestamp")
                cache.current_ts = int.from_bytes(raw[offset:offset + 8], "little")
                offset += 8
                continue
            if entry_type == ENTRY_TYPE_TIME_REL_8:
                _ensure_available(raw, offset, 1, "relative timestamp (8-bit)")
                if cache.current_ts is None:
                    raise TsdbParseError("Relative timestamp before absolute timestamp")
                cache.current_ts += raw[offset]
                offset += 1
                continue
            if entry_type == ENTRY_TYPE_TIME_REL_16:
                _ensure_available(raw, offset, 2, "relative timestamp (16-bit)")
                if cache.current_ts is None:
                    raise TsdbParseError("Relative timestamp before absolute timestamp")
                cache.current_ts += int.from_bytes(raw[offset:offset + 2], "little")
                offset += 2
                continue
            if entry_type == ENTRY_TYPE_TIME_REL_24:
                rel, offset = _read_u24(raw, offset)
                if cache.current_ts is None:
                    raise TsdbParseError("Relative timestamp before absolute timestamp")
                cache.current_ts += rel
                continue
            if entry_type == ENTRY_TYPE_TIME_REL_32:
                _ensure_available(raw, offset, 4, "relative timestamp (32-bit)")
                if cache.current_ts is None:
                    raise TsdbParseError("Relative timestamp before absolute timestamp")
                cache.current_ts += int.from_bytes(raw[offset:offset + 4], "little")
                offset += 4
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
                cache.channel_defs[channel_id] = (format_id, series_name)
                cache.series_format_ids.setdefault(series_name, format_id)
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
                cache.channel_defs[channel_id] = (format_id, series_name)
                cache.series_format_ids.setdefault(series_name, format_id)
                continue

            if entry_type == ENTRY_TYPE_META_INFO:
                _ensure_available(raw, offset, 1, "meta-info key length")
                key_len = raw[offset]
                offset += 1
                _ensure_available(raw, offset, key_len, "meta-info key")
                key = raw[offset:offset + key_len].decode("utf-8")
                offset += key_len
                _ensure_available(raw, offset, 1, "meta-info format id")
                format_id = raw[offset]
                offset += 1
                value, offset = read_format_value(raw, offset, format_id)
                cache.meta_info[key] = value
                if key == "dsBucketMs":
                    try:
                        cache.ds_bucket_ms = int(value)
                    except Exception:
                        cache.ds_bucket_ms = None
                continue

            if entry_type == ENTRY_TYPE_SERIES_ARRAY:
                if cache.channel_defs or cache.current_ts is not None:
                    raise TsdbParseError("Series Array entries must not be mixed with regular TSDB entries")
                offset = _parse_series_array_entry(raw, entry_start, offset, cache, path)
                continue

            raise TsdbParseError(f"Unknown entry type 0x{entry_type:02x} at offset {base_offset + offset - 1}")
        except TsdbParseError as exc:
            if _is_incomplete_parse_error(exc):
                offset = entry_start
                break
            offset = entry_start
            break

    return offset


def _build_cache_from_scratch(path: str, st: os.stat_result) -> CachedTsdbFile:
    with open(path, "rb") as f:
        raw = f.read()
    if len(raw) < 12:
        raise TsdbParseError(f"File too small: {path}")
    if raw[:8] != TSDB_TAG_BYTES:
        raise TsdbParseError(f"Invalid TSDB tag in {path}")
    version = int.from_bytes(raw[8:12], "little")
    if version != TSDB_VERSION:
        raise TsdbParseError(f"Unsupported TSDB version {version} in {path}")

    cache = CachedTsdbFile(
        mtime_ns=st.st_mtime_ns,
        size=st.st_size,
        parsed_offset=12,
        current_ts=None,
        channel_defs={},
        series_format_ids={},
        series_events={},
        meta_info={},
        ds_bucket_ms=None,
    )
    consumed = _parse_tsdb_chunk_into_cache(raw[12:], 12, cache, path)
    cache.parsed_offset = 12 + consumed
    return cache


def _day_from_tsdb_path(path: str) -> datetime.date:
    base = os.path.basename(path)
    for prefix in ("data_", "dsda_"):
        if base.startswith(prefix):
            rest = base[len(prefix):]
            if len(rest) >= 10:
                try:
                    return datetime.date.fromisoformat(rest[:10])
                except ValueError:
                    pass
    raise TsdbParseError(f"Cannot derive UTC day from TSDB filename: {path!r}")


def _slot_center_timestamp_ms(day: datetime.date, num_elements: int, index: int) -> int:
    if num_elements <= 0 or 86_400_000 % num_elements != 0:
        raise TsdbParseError(f"Invalid numElements in series array: {num_elements}")
    slot_ms = 86_400_000 // num_elements
    day_start = int(datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    return day_start + index * slot_ms + (slot_ms // 2)


def _format_id_from_n_decimals(n_decimals: int) -> int:
    n = max(0, min(6, int(n_decimals)))
    return {
        0: FORMAT_DOUBLE,
        1: FORMAT_DOUBLE_DEC1,
        2: FORMAT_DOUBLE_DEC2,
        3: FORMAT_DOUBLE_DEC3,
        4: FORMAT_DOUBLE_DEC4,
        5: FORMAT_DOUBLE_DEC5,
        6: FORMAT_DOUBLE_DEC6PLUS,
    }[n]


def _parse_series_array_entry(raw: bytes, entry_start: int, offset: int, cache: CachedTsdbFile, path: str) -> int:
    entry_size, offset = _read_uleb128(raw, offset)
    entry_end = entry_start + entry_size
    if entry_end > len(raw):
        raise TsdbParseError(f"Series Array entry exceeds file size at offset {entry_start}")
    name_len, offset = _read_uleb128(raw, offset)
    _ensure_available(raw, offset, name_len, "series array name")
    series_name = raw[offset:offset + name_len].decode("utf-8")
    offset += name_len
    num_elements, offset = _read_uleb128(raw, offset)
    _ensure_available(raw, offset, 2, "series array header")
    n_decimals = raw[offset]
    elem_size = raw[offset + 1]
    offset += 2
    if elem_size not in (1, 3):
        raise TsdbParseError(f"Unsupported elemSize {elem_size} in series array")
    void_element, offset = _read_uleb128(raw, offset)
    day = _day_from_tsdb_path(path)
    slot_ms = 86_400_000 // num_elements if num_elements > 0 and (86_400_000 % num_elements == 0) else 0
    if slot_ms <= 0:
        raise TsdbParseError(f"Invalid numElements {num_elements} in series array")
    cache.series_format_ids.setdefault(series_name, _format_id_from_n_decimals(n_decimals))
    cache.ds_bucket_ms = slot_ms
    cache.meta_info.setdefault("dsBucketMs", slot_ms)

    element_index = 0
    last_value = 0
    while offset < entry_end:
        chunk_len, offset = _read_zigzag_leb128(raw, offset)
        if chunk_len == 0:
            raise TsdbParseError("Series array chunkLen must not be 0")
        if chunk_len < 0:
            element_index += -chunk_len
            continue
        for _ in range(chunk_len):
            if element_index >= num_elements:
                raise TsdbParseError("Series array element index exceeds numElements")
            decoded_values: List[int] = []
            for _j in range(elem_size):
                delta, offset = _read_zigzag_leb128(raw, offset)
                last_value += delta
                decoded_values.append(last_value)
            is_void = all(v == void_element for v in decoded_values)
            if not is_void:
                ts_ms = _slot_center_timestamp_ms(day, num_elements, element_index)
                scale = 10 ** int(n_decimals)
                if elem_size == 1:
                    value: Any = decoded_values[0] / scale
                else:
                    value = {
                        "min": decoded_values[0] / scale,
                        "avg": decoded_values[1] / scale,
                        "max": decoded_values[2] / scale,
                    }
                cache.series_events.setdefault(series_name, []).append(Event(ts_ms, value))
            element_index += 1
    if element_index != num_elements:
        raise TsdbParseError(f"Series array elements {element_index} != numElements {num_elements}")
    return entry_end


def _refresh_cache_incremental(path: str, st: os.stat_result, cache: CachedTsdbFile) -> CachedTsdbFile:
    parse_from = cache.parsed_offset
    if parse_from >= st.st_size:
        cache.mtime_ns = st.st_mtime_ns
        cache.size = st.st_size
        return cache

    with open(path, "rb") as f:
        f.seek(parse_from)
        raw = f.read(st.st_size - parse_from)

    consumed = _parse_tsdb_chunk_into_cache(raw, parse_from, cache, path)
    cache.parsed_offset = parse_from + consumed
    cache.mtime_ns = st.st_mtime_ns
    cache.size = st.st_size
    return cache


def get_cached_tsdb_file(path: str) -> CachedTsdbFile:
    st = os.stat(path)
    series_array_file = os.path.basename(path).startswith("dsda_")
    with _TSDB_CACHE_LOCK:
        cache = _TSDB_FILE_CACHE.get(path)
        if cache is None:
            cache = _build_cache_from_scratch(path, st)
            _TSDB_FILE_CACHE[path] = cache
            return cache
        if st.st_mtime_ns == cache.mtime_ns and st.st_size == cache.size:
            return cache
        if series_array_file:
            cache = _build_cache_from_scratch(path, st)
            _TSDB_FILE_CACHE[path] = cache
            return cache
        if st.st_size < cache.parsed_offset:
            cache = _build_cache_from_scratch(path, st)
            _TSDB_FILE_CACHE[path] = cache
            return cache
        cache = _refresh_cache_incremental(path, st, cache)
        _TSDB_FILE_CACHE[path] = cache
        return cache


def get_series_format_id_in_file(path: str, series_name: str) -> Optional[int]:
    cache = get_cached_tsdb_file(path)
    return cache.series_format_ids.get(series_name)


def decimal_places_from_format_id(format_id: Optional[int]) -> int:
    if format_id is None:
        return 3
    if format_id == FORMAT_DOUBLE:
        return 0
    if format_id == FORMAT_DOUBLE_DEC1:
        return 1
    if format_id == FORMAT_DOUBLE_DEC2:
        return 2
    if format_id == FORMAT_DOUBLE_DEC3:
        return 3
    if format_id == FORMAT_DOUBLE_DEC4:
        return 4
    if format_id == FORMAT_DOUBLE_DEC5:
        return 5
    if format_id == FORMAT_DOUBLE_DEC6PLUS:
        return 6
    if format_id == FORMAT_FLOAT:
        return 3
    lo = format_id & 0xF
    hi = (format_id >> 4) & 0xF
    if hi in {0x1, 0x2, 0x3, 0x4, 0x5, 0x9, 0xA, 0xB, 0xC, 0xD} and 0 <= lo <= 3:
        return lo
    return 3


def is_numeric_format_id(format_id: Optional[int]) -> bool:
    if format_id is None:
        return False
    if format_id in {
        FORMAT_FLOAT,
        FORMAT_DOUBLE,
        FORMAT_DOUBLE_DEC1,
        FORMAT_DOUBLE_DEC2,
        FORMAT_DOUBLE_DEC3,
        FORMAT_DOUBLE_DEC4,
        FORMAT_DOUBLE_DEC5,
        FORMAT_DOUBLE_DEC6PLUS,
    }:
        return True
    hi = (format_id >> 4) & 0xF
    lo = format_id & 0xF
    return hi in {0x1, 0x2, 0x3, 0x4, 0x5, 0x9, 0xA, 0xB, 0xC, 0xD} and 0 <= lo <= 3


def is_string_format_id(format_id: Optional[int]) -> bool:
    return format_id in {FORMAT_STRING_U8, FORMAT_STRING_U16, FORMAT_STRING_U32, FORMAT_STRING_U64}


def read_tsdb_events_for_series(path: str, target_series: str, start_ms: int, end_ms: int) -> List[Event]:
    cache = get_cached_tsdb_file(path)
    events = cache.series_events.get(target_series, [])
    if not events:
        return []
    if start_ms <= events[0].timestamp_ms and events[-1].timestamp_ms <= end_ms:
        return list(events)
    return [e for e in events if start_ms <= e.timestamp_ms <= end_ms]


def list_series_in_file(path: str) -> List[str]:
    try:
        cache = get_cached_tsdb_file(path)
    except (TsdbParseError, OSError):
        return []
    return sorted(cache.series_events.keys())


class TimeSeriesDbData:
    def __init__(self) -> None:
        self._series_values: dict[str, list[TimeSeriesPoint]] = {}
        self._events: list[tuple[int, str, Any]] = []
        self._series_format_ids: dict[str, int] = {}
        self._meta_info: dict[str, Any] = {}

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

    def set_meta_info(self, key: str, value: Any) -> None:
        self._meta_info[key] = value

    def get_meta_info(self, key: str) -> Any:
        return self._meta_info.get(key)

    def dump(self, out: Optional[TextIO] = None) -> None:
        stream = out if out is not None else sys.stdout
        stream.write(f"TimeSeriesDB dump: series={len(self._series_values)} events={len(self._events)}\n")
        stream.write("Series:\n")
        for series_name in self.list_series():
            format_id = self._series_format_ids.get(series_name)
            format_text = f"0x{format_id:02x} ({format_id_description(format_id)})" if format_id is not None else "unknown"
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


@dataclasses.dataclass(frozen=True)
class TsdbFormatStatsRow:
    format_id: int
    format_name: str
    count: int
    value_size_text: str
    total_value_bytes: int


@dataclasses.dataclass(frozen=True)
class TsdbFileStats:
    total_bytes: int
    value_count: int
    value_bytes: int
    timestamp_count: int
    timestamp_bytes: int
    channel_definition_count: int
    channel_definition_bytes: int
    other_count: int
    other_bytes: int
    per_format: List[TsdbFormatStatsRow]


def _scan_format_value_size(data: bytes, offset: int, format_id: int) -> Tuple[int, int]:
    start = offset
    _value, offset = read_format_value(data, offset, format_id)
    return offset - start, offset


def _dump_bytes_chunk(out: TextIO, chunk: bytes, text: str) -> None:
    if not chunk:
        return
    first = True
    for i in range(0, len(chunk), 8):
        part = chunk[i:i + 8]
        hex_col = " ".join(f"{b:02x}" for b in part)
        right = text if first else ""
        out.write(f"{hex_col:<23}  {right}\n")
        first = False


def _dump_bytes_value_text(value: Any) -> str:
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, float):
        return _format_float_plain(value, decimals=12)
    return repr(value)


def _ascii_bytes_text(data: bytes) -> str:
    chars: List[str] = []
    for b in data:
        if 32 <= b <= 126:
            chars.append(chr(b))
        else:
            chars.append(f"\\x{b:02x}")
    return "".join(chars)


def _format_float_plain(value: float, decimals: int = 6) -> str:
    s = f"{float(value):.{max(0, int(decimals))}f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    if "." not in s:
        s = s + ".0"
    return s


def _format_float_fixed(value: float, decimals: int) -> str:
    return f"{float(value):.{max(0, int(decimals))}f}"


def _describe_numeric_payload(raw_value_bytes: bytes, format_id: int, decoded_value: Any) -> str:
    if format_id == FORMAT_FLOAT:
        bits = int.from_bytes(raw_value_bytes, "little")
        return f"type=float32le bits=0x{bits:08x} value={_format_float_plain(float(decoded_value), 12)}"
    if format_id in (
        FORMAT_DOUBLE,
        FORMAT_DOUBLE_DEC1,
        FORMAT_DOUBLE_DEC2,
        FORMAT_DOUBLE_DEC3,
        FORMAT_DOUBLE_DEC4,
        FORMAT_DOUBLE_DEC5,
        FORMAT_DOUBLE_DEC6PLUS,
    ):
        bits = int.from_bytes(raw_value_bytes, "little")
        decimals = decimal_places_from_format_id(format_id)
        return f"type=float64le bits=0x{bits:016x} value={_format_float_fixed(float(decoded_value), decimals)}"
    shape = _numeric_format_shape(format_id)
    if shape is not None:
        byte_count, signed, scale = shape
        raw_int, _ = _read_scalar(raw_value_bytes, 0, byte_count, signed)
        signed_name = "i" if signed else "u"
        return (
            f"type={signed_name}{byte_count * 8}le(scale=/{scale}) "
            f"raw={raw_int} value={_format_float_plain(float(decoded_value), 12)}"
        )
    return f"type=unknown_numeric fmt=0x{format_id:02x} value={_dump_bytes_value_text(decoded_value)}"


def _describe_value_payload(raw_payload: bytes, format_id: int, decoded_value: Any) -> str:
    if is_string_format_id(format_id):
        len_size = {
            FORMAT_STRING_U8: 1,
            FORMAT_STRING_U16: 2,
            FORMAT_STRING_U32: 4,
            FORMAT_STRING_U64: 8,
        }[format_id]
        text_len = int.from_bytes(raw_payload[:len_size], "little")
        return f"type=utf8(len=u{len_size * 8}le={text_len}) value={_dump_bytes_value_text(decoded_value)}"
    if is_numeric_format_id(format_id):
        return _describe_numeric_payload(raw_payload, format_id, decoded_value)
    return f"type=fmt0x{format_id:02x} value={_dump_bytes_value_text(decoded_value)}"


def dump_timeseries_db_bytes(path: str, out: Optional[TextIO] = None) -> None:
    stream = out if out is not None else sys.stdout
    with open(path, "rb") as f:
        raw = f.read()

    channel_defs: Dict[int, Tuple[int, str]] = {}
    current_ts: Optional[int] = None
    ds_bucket_ms: Optional[int] = None
    offset = 0

    try:
        if len(raw) >= 8:
            _dump_bytes_chunk(stream, raw[0:8], f"tag ascii='{_ascii_bytes_text(raw[0:8])}'")
            offset = 8
        else:
            _dump_bytes_chunk(stream, raw, "truncated tag")
            return

        if len(raw) >= 12:
            version = int.from_bytes(raw[8:12], "little")
            _dump_bytes_chunk(stream, raw[8:12], f"version u32le={version}")
            offset = 12
        else:
            _dump_bytes_chunk(stream, raw[offset:], "truncated version")
            return

        while offset < len(raw):
            entry_start = offset
            entry_type = raw[offset]
            offset += 1

            if entry_type <= 0xEF:
                if entry_type not in channel_defs:
                    raise TsdbParseError(f"Undefined channel id {entry_type}")
                format_id, series_name = channel_defs[entry_type]
                _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], f"value8 type ch={entry_type}")
                if ds_bucket_ms is not None and is_numeric_format_id(format_id):
                    for label in ("min", "avg", "max"):
                        value_start = offset
                        value, offset = read_format_value(raw, offset, format_id)
                        payload = raw[value_start:offset]
                        _dump_bytes_chunk(
                            stream,
                            payload,
                            f"{series_name} {label} { _describe_value_payload(payload, format_id, value) } "
                            f"ts={current_ts} ds_bucket_ms={ds_bucket_ms}",
                        )
                else:
                    value_start = offset
                    value, offset = read_format_value(raw, offset, format_id)
                    payload = raw[value_start:offset]
                    _dump_bytes_chunk(
                        stream,
                        payload,
                        f"{series_name} { _describe_value_payload(payload, format_id, value) } ts={current_ts}",
                    )
                continue

            if entry_type == ENTRY_TYPE_VALUE_16:
                _ensure_available(raw, offset, 2, "16-bit channel id")
                channel_id = int.from_bytes(raw[offset:offset + 2], "little")
                id_start = offset
                offset += 2
                if channel_id not in channel_defs:
                    raise TsdbParseError(f"Undefined 16-bit channel id {channel_id}")
                format_id, series_name = channel_defs[channel_id]
                _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], "value16 type")
                _dump_bytes_chunk(stream, raw[id_start:id_start + 2], f"value16 channel=u16le {channel_id}")
                if ds_bucket_ms is not None and is_numeric_format_id(format_id):
                    for label in ("min", "avg", "max"):
                        value_start = offset
                        value, offset = read_format_value(raw, offset, format_id)
                        payload = raw[value_start:offset]
                        _dump_bytes_chunk(
                            stream,
                            payload,
                            f"{series_name} {label} { _describe_value_payload(payload, format_id, value) } "
                            f"ts={current_ts} ds_bucket_ms={ds_bucket_ms}",
                        )
                else:
                    value_start = offset
                    value, offset = read_format_value(raw, offset, format_id)
                    payload = raw[value_start:offset]
                    _dump_bytes_chunk(
                        stream,
                        payload,
                        f"{series_name} { _describe_value_payload(payload, format_id, value) } ts={current_ts}",
                    )
                continue

            if entry_type == ENTRY_TYPE_TIME_ABSOLUTE:
                _ensure_available(raw, offset, 8, "absolute timestamp")
                ts = int.from_bytes(raw[offset:offset + 8], "little")
                current_ts = ts
                _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], "time abs type")
                _dump_bytes_chunk(stream, raw[offset:offset + 8], f"ts_abs u64le={ts} ({ts / 1000.0:.3f}s)")
                offset += 8
                continue

            if entry_type == ENTRY_TYPE_TIME_REL_8:
                _ensure_available(raw, offset, 1, "relative timestamp (8-bit)")
                delta = raw[offset]
                current_ts = (current_ts or 0) + delta
                _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], "time rel8 type")
                _dump_bytes_chunk(stream, raw[offset:offset + 1], f"delta u8=+{delta} (+{delta / 1000.0:.3f}s) ts={current_ts}")
                offset += 1
                continue

            if entry_type == ENTRY_TYPE_TIME_REL_16:
                _ensure_available(raw, offset, 2, "relative timestamp (16-bit)")
                delta = int.from_bytes(raw[offset:offset + 2], "little")
                current_ts = (current_ts or 0) + delta
                _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], "time rel16 type")
                _dump_bytes_chunk(stream, raw[offset:offset + 2], f"delta u16le=+{delta} (+{delta / 1000.0:.3f}s) ts={current_ts}")
                offset += 2
                continue

            if entry_type == ENTRY_TYPE_TIME_REL_24:
                delta, next_off = _read_u24(raw, offset)
                current_ts = (current_ts or 0) + delta
                _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], "time rel24 type")
                _dump_bytes_chunk(stream, raw[offset:next_off], f"delta u24le=+{delta} (+{delta / 1000.0:.3f}s) ts={current_ts}")
                offset = next_off
                continue

            if entry_type == ENTRY_TYPE_TIME_REL_32:
                _ensure_available(raw, offset, 4, "relative timestamp (32-bit)")
                delta = int.from_bytes(raw[offset:offset + 4], "little")
                current_ts = (current_ts or 0) + delta
                _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], "time rel32 type")
                _dump_bytes_chunk(stream, raw[offset:offset + 4], f"delta u32le=+{delta} (+{delta / 1000.0:.3f}s) ts={current_ts}")
                offset += 4
                continue

            if entry_type == ENTRY_TYPE_CHANNEL_DEF_8:
                _ensure_available(raw, offset, 3, "channel definition (8-bit)")
                channel_id = raw[offset]
                format_id = raw[offset + 1]
                name_len = raw[offset + 2]
                hdr_start = offset
                offset += 3
                _ensure_available(raw, offset, name_len, "channel name")
                name_bytes = raw[offset:offset + name_len]
                series_name = name_bytes.decode("utf-8", errors="replace")
                channel_defs[channel_id] = (format_id, series_name)
                _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], "channel_def8 type")
                _dump_bytes_chunk(
                    stream,
                    raw[hdr_start:hdr_start + 3],
                    f"channel u8={channel_id} format u8=0x{format_id:02x} name_len u8={name_len}",
                )
                _dump_bytes_chunk(stream, name_bytes, f"series utf8='{series_name}'")
                offset += name_len
                continue

            if entry_type == ENTRY_TYPE_CHANNEL_DEF_16:
                _ensure_available(raw, offset, 5, "channel definition (16-bit)")
                channel_id = int.from_bytes(raw[offset:offset + 2], "little")
                format_id = raw[offset + 2]
                name_len = int.from_bytes(raw[offset + 3:offset + 5], "little")
                hdr_start = offset
                offset += 5
                _ensure_available(raw, offset, name_len, "channel name")
                name_bytes = raw[offset:offset + name_len]
                series_name = name_bytes.decode("utf-8", errors="replace")
                channel_defs[channel_id] = (format_id, series_name)
                _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], "channel_def16 type")
                _dump_bytes_chunk(
                    stream,
                    raw[hdr_start:hdr_start + 5],
                    f"channel u16le={channel_id} format u8=0x{format_id:02x} name_len u16le={name_len}",
                )
                _dump_bytes_chunk(stream, name_bytes, f"series utf8='{series_name}'")
                offset += name_len
                continue

            if entry_type == ENTRY_TYPE_META_INFO:
                _ensure_available(raw, offset, 3, "meta info header")
                key_len = raw[offset]
                value_len = int.from_bytes(raw[offset + 1:offset + 3], "little")
                hdr_start = offset
                offset += 3
                _ensure_available(raw, offset, key_len + value_len, "meta info payload")
                key_bytes = raw[offset:offset + key_len]
                key = key_bytes.decode("utf-8", errors="replace")
                offset += key_len
                val_bytes = raw[offset:offset + value_len]
                value = val_bytes.decode("utf-8", errors="replace")
                _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], "meta type")
                _dump_bytes_chunk(stream, raw[hdr_start:hdr_start + 3], f"meta key_len u8={key_len} value_len u16le={value_len}")
                _dump_bytes_chunk(stream, key_bytes, f"meta key utf8='{key}'")
                _dump_bytes_chunk(stream, val_bytes, f"meta value utf8='{value}'")
                if key == "dsBucketMs":
                    try:
                        ds_bucket_ms = int(value.strip())
                    except Exception:
                        pass
                offset += value_len
                continue

            if entry_type == ENTRY_TYPE_SERIES_ARRAY:
                _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], "series_array type")
                size_start = offset
                entry_size, offset = _read_uleb128(raw, offset)
                entry_end = entry_start + entry_size
                if entry_end > len(raw):
                    raise TsdbParseError("Series array entry exceeds file size")
                _dump_bytes_chunk(stream, raw[size_start:offset], f"entry_size uleb128={entry_size}")

                name_len_start = offset
                name_len, offset = _read_uleb128(raw, offset)
                _dump_bytes_chunk(stream, raw[name_len_start:offset], f"name_len uleb128={name_len}")
                _ensure_available(raw, offset, name_len, "series array name")
                series_name_bytes = raw[offset:offset + name_len]
                series_name = series_name_bytes.decode("utf-8", errors="replace")
                _dump_bytes_chunk(stream, series_name_bytes, f"series utf8='{series_name}'")
                offset += name_len

                num_el_start = offset
                num_elements, offset = _read_uleb128(raw, offset)
                _dump_bytes_chunk(stream, raw[num_el_start:offset], f"{series_name} num_elements uleb128={num_elements}")
                _ensure_available(raw, offset, 2, "series array header")
                n_decimals = raw[offset]
                elem_size = raw[offset + 1]
                _dump_bytes_chunk(stream, raw[offset:offset + 2], f"{series_name} decimals u8={n_decimals} elem_size u8={elem_size}")
                offset += 2
                scale = float(10 ** int(n_decimals))

                void_start = offset
                void_element, offset = _read_uleb128(raw, offset)
                _dump_bytes_chunk(stream, raw[void_start:offset], f"{series_name} void_element uleb128={void_element}")

                chunk_index = 0
                element_index = 0
                last_value = 0
                comp_names = ["value"] if int(elem_size) == 1 else ["min", "avg", "max"]
                while offset < entry_end:
                    chunk_start = offset
                    chunk_len, offset = _read_zigzag_leb128(raw, offset)
                    chunk_kind = "data_chunk" if chunk_len > 0 else "repeat_chunk" if chunk_len < 0 else "invalid_chunk"
                    _dump_bytes_chunk(
                        stream,
                        raw[chunk_start:offset],
                        f"{series_name} c{chunk_index} {chunk_kind} len zigzag={chunk_len:>8d}",
                    )
                    if chunk_len < 0:
                        element_index += -chunk_len
                    if chunk_len > 0:
                        for _ in range(chunk_len):
                            for comp_i in range(max(1, int(elem_size))):
                                d_start = offset
                                delta_int, offset = _read_zigzag_leb128(raw, offset)
                                last_value += delta_int
                                delta_value = float(delta_int) / scale
                                actual_value = float(last_value) / scale
                                comp = comp_names[comp_i] if comp_i < len(comp_names) else f"c{comp_i}"
                                _dump_bytes_chunk(
                                    stream,
                                    raw[d_start:offset],
                                    f"{series_name} c{chunk_index} e{element_index:>4d} {comp} "
                                    f"zigzag={delta_int:>8d} delta={_format_float_fixed(delta_value, int(n_decimals)):>9} "
                                    f"value={_format_float_fixed(actual_value, int(n_decimals)):>12}",
                                )
                            element_index += 1
                    chunk_index += 1
                if offset != entry_end:
                    raise TsdbParseError("Series array decode did not end at entry boundary")
                continue

            _dump_bytes_chunk(stream, raw[entry_start:entry_start + 1], f"unknown entry type 0x{entry_type:02x}")
    except TsdbParseError as exc:
        if offset < len(raw):
            _dump_bytes_chunk(stream, raw[offset:], f"unparsed tail ({exc})")
        else:
            stream.write(f"{'':23}  parse error: {exc}\n")


def stat_timeseries_db(path: str) -> TsdbFileStats:
    with open(path, "rb") as f:
        raw = f.read()
    if len(raw) < 12:
        raise TsdbParseError(f"File too small: {path}")
    if raw[:8] != TSDB_TAG_BYTES:
        raise TsdbParseError(f"Invalid TSDB tag in {path!r}")
    version = int.from_bytes(raw[8:12], "little")
    if version != TSDB_VERSION:
        raise TsdbParseError(f"Unsupported TSDB version {version} in {path!r}")

    offset = 12
    channel_defs: Dict[int, Tuple[int, str]] = {}
    ds_bucket_ms: Optional[int] = None
    value_count = 0
    value_bytes = 0
    timestamp_count = 0
    timestamp_bytes = 0
    channel_definition_count = 0
    channel_definition_bytes = 0
    other_count = 1  # file header
    per_format_counts: Dict[int, int] = {}
    per_format_total_bytes: Dict[int, int] = {}
    per_format_value_sizes: Dict[int, set[int]] = {}

    while offset < len(raw):
        entry_start = offset
        entry_type = raw[offset]
        offset += 1

        if entry_type <= 0xEF:
            channel_id = entry_type
            if channel_id not in channel_defs:
                raise TsdbParseError(f"Undefined channel id {channel_id}")
            format_id, _series_name = channel_defs[channel_id]
            payload_size = 0
            if ds_bucket_ms is not None and is_numeric_format_id(format_id):
                for _ in range(3):
                    size, offset = _scan_format_value_size(raw, offset, format_id)
                    payload_size += size
            else:
                payload_size, offset = _scan_format_value_size(raw, offset, format_id)
            entry_size = 1 + payload_size
            value_count += 1
            value_bytes += entry_size
            per_format_counts[format_id] = per_format_counts.get(format_id, 0) + 1
            per_format_total_bytes[format_id] = per_format_total_bytes.get(format_id, 0) + entry_size
            per_format_value_sizes.setdefault(format_id, set()).add(payload_size)
            continue

        if entry_type == ENTRY_TYPE_VALUE_16:
            _ensure_available(raw, offset, 2, "16-bit channel id")
            channel_id = int.from_bytes(raw[offset:offset + 2], "little")
            offset += 2
            if channel_id not in channel_defs:
                raise TsdbParseError(f"Undefined 16-bit channel id {channel_id}")
            format_id, _series_name = channel_defs[channel_id]
            payload_size = 0
            if ds_bucket_ms is not None and is_numeric_format_id(format_id):
                for _ in range(3):
                    size, offset = _scan_format_value_size(raw, offset, format_id)
                    payload_size += size
            else:
                payload_size, offset = _scan_format_value_size(raw, offset, format_id)
            entry_size = 3 + payload_size
            value_count += 1
            value_bytes += entry_size
            per_format_counts[format_id] = per_format_counts.get(format_id, 0) + 1
            per_format_total_bytes[format_id] = per_format_total_bytes.get(format_id, 0) + entry_size
            per_format_value_sizes.setdefault(format_id, set()).add(payload_size)
            continue

        if entry_type == ENTRY_TYPE_TIME_ABSOLUTE:
            _ensure_available(raw, offset, 8, "absolute timestamp")
            offset += 8
            timestamp_count += 1
            timestamp_bytes += offset - entry_start
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_8:
            _ensure_available(raw, offset, 1, "relative timestamp (8-bit)")
            offset += 1
            timestamp_count += 1
            timestamp_bytes += offset - entry_start
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_16:
            _ensure_available(raw, offset, 2, "relative timestamp (16-bit)")
            offset += 2
            timestamp_count += 1
            timestamp_bytes += offset - entry_start
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_24:
            _rel, offset = _read_u24(raw, offset)
            timestamp_count += 1
            timestamp_bytes += offset - entry_start
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_32:
            _ensure_available(raw, offset, 4, "relative timestamp (32-bit)")
            offset += 4
            timestamp_count += 1
            timestamp_bytes += offset - entry_start
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
            channel_definition_count += 1
            channel_definition_bytes += offset - entry_start
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
            channel_definition_count += 1
            channel_definition_bytes += offset - entry_start
            continue
        if entry_type == ENTRY_TYPE_META_INFO:
            _ensure_available(raw, offset, 1, "meta-info key length")
            key_len = raw[offset]
            offset += 1
            _ensure_available(raw, offset, key_len, "meta-info key")
            key = raw[offset:offset + key_len].decode("utf-8")
            offset += key_len
            _ensure_available(raw, offset, 1, "meta-info format id")
            format_id = raw[offset]
            offset += 1
            _value, offset = read_format_value(raw, offset, format_id)
            if key == "dsBucketMs":
                try:
                    ds_bucket_ms = int(_value)
                except Exception:
                    ds_bucket_ms = None
            other_count += 1
            continue
        if entry_type == ENTRY_TYPE_SERIES_ARRAY:
            entry_size, offset = _read_uleb128(raw, offset)
            entry_end = entry_start + entry_size
            if entry_end > len(raw):
                raise TsdbParseError(f"Series Array entry exceeds file size at offset {entry_start}")
            name_len, offset = _read_uleb128(raw, offset)
            _ensure_available(raw, offset, name_len, "series array name")
            offset += name_len
            num_elements, offset = _read_uleb128(raw, offset)
            _ensure_available(raw, offset, 2, "series array header")
            n_decimals = raw[offset]
            elem_size = raw[offset + 1]
            offset += 2
            if elem_size not in (1, 3):
                raise TsdbParseError(f"Unsupported elemSize {elem_size} in series array")
            void_element, offset = _read_uleb128(raw, offset)
            value_format_id = _format_id_from_n_decimals(n_decimals)
            non_void_count = 0
            last_value = 0
            while offset < entry_end:
                chunk_len, offset = _read_zigzag_leb128(raw, offset)
                if chunk_len == 0:
                    raise TsdbParseError("Series array chunkLen must not be 0")
                if chunk_len < 0:
                    continue
                for _ in range(chunk_len):
                    decoded_values: List[int] = []
                    for _ in range(elem_size):
                        delta, offset = _read_zigzag_leb128(raw, offset)
                        last_value += delta
                        decoded_values.append(last_value)
                    if not all(v == void_element for v in decoded_values):
                        non_void_count += 1
            if offset != entry_end:
                raise TsdbParseError(f"Series array entry parsing did not end on entry boundary at {entry_start}")
            value_payload_size = elem_size * 8
            per_format_counts[value_format_id] = per_format_counts.get(value_format_id, 0) + non_void_count
            per_format_total_bytes[value_format_id] = per_format_total_bytes.get(value_format_id, 0) + non_void_count * value_payload_size
            per_format_value_sizes.setdefault(value_format_id, set()).add(value_payload_size)
            value_count += non_void_count
            value_bytes += non_void_count * value_payload_size
            other_count += 1
            continue
        raise TsdbParseError(f"Unknown entry type 0x{entry_type:02x} at offset {entry_start}")

    total_bytes = len(raw)
    other_bytes = total_bytes - value_bytes - timestamp_bytes - channel_definition_bytes
    rows: List[TsdbFormatStatsRow] = []
    for format_id in sorted(per_format_counts.keys()):
        sizes = per_format_value_sizes.get(format_id, set())
        value_size_text = str(next(iter(sizes))) if len(sizes) == 1 else "var"
        rows.append(
            TsdbFormatStatsRow(
                format_id=format_id,
                format_name=format_id_description(format_id),
                count=per_format_counts[format_id],
                value_size_text=value_size_text,
                total_value_bytes=per_format_total_bytes[format_id],
            )
        )
    return TsdbFileStats(
        total_bytes=total_bytes,
        value_count=value_count,
        value_bytes=value_bytes,
        timestamp_count=timestamp_count,
        timestamp_bytes=timestamp_bytes,
        channel_definition_count=channel_definition_count,
        channel_definition_bytes=channel_definition_bytes,
        other_count=other_count,
        other_bytes=other_bytes,
        per_format=rows,
    )


@dataclasses.dataclass(frozen=True)
class NumericWithDecimals:
    value: float
    decimals: int


def read_timeseries_db(path: str, dump_out: Optional[TextIO] = None, verbose: int = 0) -> TimeSeriesDbData:
    with open(path, "rb") as f:
        raw = f.read()
    if len(raw) < 12:
        raise TsdbParseError(f"File too small: {path}")
    if raw[:8] != TSDB_TAG_BYTES:
        raise TsdbParseError(f"Invalid TSDB tag in {path!r}")
    version = int.from_bytes(raw[8:12], "little")
    if version != TSDB_VERSION:
        raise TsdbParseError(f"Unsupported TSDB version {version} in {path!r}")
    if len(raw) > 12 and raw[12] == ENTRY_TYPE_SERIES_ARRAY:
        cache = get_cached_tsdb_file(path)
        result = TimeSeriesDbData()
        for key, value in cache.meta_info.items():
            result.set_meta_info(key, value)
        events: List[Tuple[int, str, Any]] = []
        for series_name, series_events in cache.series_events.items():
            format_id = cache.series_format_ids.get(series_name)
            if format_id is not None:
                result._set_series_format_id(series_name, format_id)
            for ev in series_events:
                events.append((ev.timestamp_ms, series_name, ev.value))
        events.sort(key=lambda item: (item[0], item[1]))
        for ts_ms, series_name, value in events:
            result._append(series_name, ts_ms, value)
        if dump_out is not None:
            result.dump(dump_out)
        return result

    result = TimeSeriesDbData()
    channel_defs: dict[int, tuple[int, str]] = {}
    current_ts: Optional[int] = None
    ds_bucket_ms: Optional[int] = None
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
                raise TsdbParseError("Value entry encountered before any timestamp was set")
            if channel_id not in channel_defs:
                break
            format_id, series_name = channel_defs[channel_id]
            if ds_bucket_ms is not None and is_numeric_format_id(format_id):
                v_min, offset = read_format_value(raw, offset, format_id)
                v_avg, offset = read_format_value(raw, offset, format_id)
                v_max, offset = read_format_value(raw, offset, format_id)
                value = {"min": v_min, "avg": v_avg, "max": v_max}
            else:
                value, offset = read_format_value(raw, offset, format_id)
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
                raise TsdbParseError("16-bit value entry encountered before any timestamp was set")
            if channel_id not in channel_defs:
                break
            format_id, series_name = channel_defs[channel_id]
            if ds_bucket_ms is not None and is_numeric_format_id(format_id):
                v_min, offset = read_format_value(raw, offset, format_id)
                v_avg, offset = read_format_value(raw, offset, format_id)
                v_max, offset = read_format_value(raw, offset, format_id)
                value = {"min": v_min, "avg": v_avg, "max": v_max}
            else:
                value, offset = read_format_value(raw, offset, format_id)
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
                stream.write(f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} (ts_abs={current_ts})\n")
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_8:
            _ensure_available(raw, offset, 1, "relative timestamp (8-bit)")
            rel = raw[offset]
            offset += 1
            if current_ts is None:
                raise TsdbParseError("Relative timestamp entry encountered before any absolute timestamp")
            current_ts += rel
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} (ts_rel8=+{rel} -> {current_ts})\n")
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_16:
            _ensure_available(raw, offset, 2, "relative timestamp (16-bit)")
            rel = int.from_bytes(raw[offset:offset + 2], "little")
            offset += 2
            if current_ts is None:
                raise TsdbParseError("Relative timestamp entry encountered before any absolute timestamp")
            current_ts += rel
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} (ts_rel16=+{rel} -> {current_ts})\n")
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_24:
            rel, offset = _read_u24(raw, offset)
            if current_ts is None:
                raise TsdbParseError("Relative timestamp entry encountered before any absolute timestamp")
            current_ts += rel
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} (ts_rel24=+{rel} -> {current_ts})\n")
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_32:
            _ensure_available(raw, offset, 4, "relative timestamp (32-bit)")
            rel = int.from_bytes(raw[offset:offset + 4], "little")
            offset += 4
            if current_ts is None:
                raise TsdbParseError("Relative timestamp entry encountered before any absolute timestamp")
            current_ts += rel
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} (ts_rel32=+{rel} -> {current_ts})\n")
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

        if entry_type == ENTRY_TYPE_META_INFO:
            _ensure_available(raw, offset, 1, "meta-info key length")
            key_len = raw[offset]
            offset += 1
            _ensure_available(raw, offset, key_len, "meta-info key")
            key = raw[offset:offset + key_len].decode("utf-8")
            offset += key_len
            _ensure_available(raw, offset, 1, "meta-info format id")
            format_id = raw[offset]
            offset += 1
            value, offset = read_format_value(raw, offset, format_id)
            result.set_meta_info(key, value)
            if key == "dsBucketMs":
                try:
                    ds_bucket_ms = int(value)
                except Exception:
                    ds_bucket_ms = None
            if stream is not None and verbose:
                entry_bytes = raw[entry_start:offset]
                stream.write(
                    f"        @{entry_start:08x}: {' '.join(f'{b:02x}' for b in entry_bytes)} "
                    f"(meta key={key!r} format=0x{format_id:02x} value={value!r})\n"
                )
            continue

        break

    if stream is not None:
        stream.write(f"TimeSeriesDB dump: series={len(result._series_values)} events={len(result._events)}\n")
        stream.write("Series:\n")
        for series_name in result.list_series():
            format_id = result._series_format_ids.get(series_name)
            format_text = f"0x{format_id:02x} ({format_id_description(format_id)})" if format_id is not None else "unknown"
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
        invalidate_tsdb_cache(path)

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
        invalidate_tsdb_cache(self._path)

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
        invalidate_tsdb_cache(self._path)

    def addValue(self, series_name: str, value_as_double: float, timestamp_ms: Optional[int] = None) -> None:
        self.add_value(series_name, value_as_double, timestamp_ms)

    def addStringValue(self, series_name: str, value_as_string: str, timestamp_ms: Optional[int] = None) -> None:
        self.add_string_value(series_name, value_as_string, timestamp_ms)

    def close(self) -> None:
        if self._closed:
            return
        self._f.close()
        self._closed = True
        invalidate_tsdb_cache(self._path)

    def __enter__(self) -> "TimeSeriesDbWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


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


def format_id_description(format_id: int) -> str:
    if format_id == FORMAT_FLOAT:
        return "float"
    if format_id == FORMAT_DOUBLE:
        return "double (display hint: 0 decimals)"
    if format_id == FORMAT_DOUBLE_DEC1:
        return "double (display hint: 1 decimal)"
    if format_id == FORMAT_DOUBLE_DEC2:
        return "double (display hint: 2 decimals)"
    if format_id == FORMAT_DOUBLE_DEC3:
        return "double (display hint: 3 decimals)"
    if format_id == FORMAT_DOUBLE_DEC4:
        return "double (display hint: 4 decimals)"
    if format_id == FORMAT_DOUBLE_DEC5:
        return "double (display hint: 5 decimals)"
    if format_id == FORMAT_DOUBLE_DEC6PLUS:
        return "double (display hint: 6+ decimals)"
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


def encode_value_for_format(value: Any, format_id: int) -> Optional[bytes]:
    if format_id in (
        FORMAT_DOUBLE,
        FORMAT_DOUBLE_DEC1,
        FORMAT_DOUBLE_DEC2,
        FORMAT_DOUBLE_DEC3,
        FORMAT_DOUBLE_DEC4,
        FORMAT_DOUBLE_DEC5,
        FORMAT_DOUBLE_DEC6PLUS,
    ):
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


def _best_integer_meta_format(value: int) -> int:
    if value < 0:
        if -(1 << 7) <= value <= (1 << 7) - 1:
            return 0x10
        if -(1 << 15) <= value <= (1 << 15) - 1:
            return 0x20
        if -(1 << 23) <= value <= (1 << 23) - 1:
            return 0x30
        if -(1 << 31) <= value <= (1 << 31) - 1:
            return 0x40
        return 0x50
    if value <= 0xFF:
        return 0x90
    if value <= 0xFFFF:
        return 0xA0
    if value <= 0xFFFFFF:
        return 0xB0
    if value <= 0xFFFFFFFF:
        return 0xC0
    return 0xD0


def _write_meta_info_entry(f: Any, key: str, value: Any) -> None:
    key_bytes = key.encode("utf-8")
    if len(key_bytes) > 255:
        raise ValueError(f"Meta-info key too long: {key!r}")
    if isinstance(value, bool):
        value = int(value)
    if isinstance(value, int):
        format_id = _best_integer_meta_format(value)
    elif isinstance(value, str):
        raw = value.encode("utf-8")
        if len(raw) <= 0xFF:
            format_id = FORMAT_STRING_U8
        elif len(raw) <= 0xFFFF:
            format_id = FORMAT_STRING_U16
        elif len(raw) <= 0xFFFFFFFF:
            format_id = FORMAT_STRING_U32
        else:
            format_id = FORMAT_STRING_U64
    elif isinstance(value, float):
        format_id = FORMAT_DOUBLE
    else:
        raise ValueError(f"Unsupported meta-info value type for key={key!r}")
    payload = encode_value_for_format(value, format_id)
    if payload is None:
        raise ValueError(f"Cannot encode meta-info value for key={key!r}")
    f.write(bytes([ENTRY_TYPE_META_INFO, len(key_bytes)]))
    f.write(key_bytes)
    f.write(bytes([format_id]))
    f.write(payload)


def select_best_format_for_series(values: list[Any]) -> int:
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
        if all(encode_value_for_format(v, candidate) is not None for v in numeric_values):
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
    chosen_formats = {name: select_best_format_for_series(values) for name, values in per_series_values.items()}

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
            payload = encode_value_for_format(value, format_id)
            if payload is None:
                raise ValueError(f"Cannot encode value for series={series_name!r} with formatId=0x{format_id:02x}")

            if channel_id <= 0xEF:
                out.write(bytes([channel_id]))
            else:
                out.write(bytes([ENTRY_TYPE_CHANNEL_VALUE_16]))
                out.write(channel_id.to_bytes(2, "little"))
            out.write(payload)

    invalidate_tsdb_cache(output_path)
    return chosen_formats


def write_series_array_timeseries_db(
    path: str,
    day: datetime.date,
    bucket_ms: int,
    series_order: List[str],
    series_decimals: Dict[str, int],
    series_points: Dict[str, List[Tuple[int, Any]]],
    elem_size: int,
) -> None:
    if elem_size not in (1, 3):
        raise ValueError(f"elem_size must be 1 or 3, got {elem_size}")
    if bucket_ms <= 0 or 86_400_000 % bucket_ms != 0:
        raise ValueError(f"bucket_ms must be a whole fraction of 1 day, got {bucket_ms}")
    num_elements = 86_400_000 // bucket_ms
    day_start_ms = int(datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)

    with open(path, "wb") as f:
        f.write(TSDB_TAG_BYTES)
        f.write(struct.pack("<I", TSDB_VERSION))

        for series_name in series_order:
            decimals = max(0, min(3, int(series_decimals.get(series_name, 0))))
            scale = 10 ** decimals
            entries = series_points.get(series_name, [])
            element_values: List[Optional[List[int]]] = [None] * num_elements
            used_values: set[int] = set()
            for ts_ms, value in entries:
                idx = (int(ts_ms) - day_start_ms) // bucket_ms
                if idx < 0 or idx >= num_elements:
                    continue
                if elem_size == 1:
                    if isinstance(value, dict):
                        numeric = float(value.get("avg"))
                    else:
                        numeric = float(value)
                    encoded_values = [int(round(numeric * scale))]
                else:
                    if not isinstance(value, dict):
                        numeric = float(value)
                        encoded_values = [int(round(numeric * scale))] * 3
                    else:
                        encoded_values = [
                            int(round(float(value["min"]) * scale)),
                            int(round(float(value["avg"]) * scale)),
                            int(round(float(value["max"]) * scale)),
                        ]
                element_values[idx] = encoded_values
                used_values.update(encoded_values)
            void_element = max([0] + [abs(v) for v in used_values]) + 1
            while void_element in used_values:
                void_element += 1

            payload = bytearray()
            name_bytes = series_name.encode("utf-8")
            payload.extend(_write_uleb128(len(name_bytes)))
            payload.extend(name_bytes)
            payload.extend(_write_uleb128(num_elements))
            payload.append(decimals)
            payload.append(elem_size)
            payload.extend(_write_uleb128(void_element))

            last_value = 0
            idx = 0
            while idx < num_elements:
                if element_values[idx] is None:
                    run = 1
                    while idx + run < num_elements and element_values[idx + run] is None:
                        run += 1
                    if run >= 2:
                        payload.extend(_write_zigzag_leb128(-run))
                        idx += run
                        continue
                    chunk_values = [void_element] * elem_size
                    payload.extend(_write_zigzag_leb128(1))
                    for v in chunk_values:
                        payload.extend(_write_zigzag_leb128(v - last_value))
                        last_value = v
                    idx += 1
                    continue
                run_values: List[List[int]] = []
                while idx < num_elements and element_values[idx] is not None:
                    run_values.append(element_values[idx] or [])
                    idx += 1
                payload.extend(_write_zigzag_leb128(len(run_values)))
                for values in run_values:
                    for v in values:
                        payload.extend(_write_zigzag_leb128(v - last_value))
                        last_value = v

            entry_size_bytes = b""
            while True:
                entry_size = 1 + len(entry_size_bytes) + len(payload)
                next_bytes = _write_uleb128(entry_size)
                if len(next_bytes) == len(entry_size_bytes) and next_bytes == entry_size_bytes:
                    break
                entry_size_bytes = next_bytes
            f.write(bytes([ENTRY_TYPE_SERIES_ARRAY]))
            f.write(entry_size_bytes)
            f.write(payload)
    invalidate_tsdb_cache(path)


def downsample_series_points(
    series_values: List[Tuple[int, Any]],
    day: datetime.date,
    target_bucket_ms: int,
    target_elem_size: int,
    decimals: int,
) -> List[Tuple[int, Any]]:
    if target_bucket_ms <= 0 or 86_400_000 % target_bucket_ms != 0:
        raise ValueError(f"Invalid target_bucket_ms={target_bucket_ms}")
    if target_elem_size not in (1, 3):
        raise ValueError(f"Invalid target_elem_size={target_elem_size}")
    scale_decimals = max(0, min(3, int(decimals)))
    day_start_ms = int(datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    buckets: Dict[int, Dict[str, Any]] = {}
    for ts_ms, value in series_values:
        idx = (int(ts_ms) - day_start_ms) // target_bucket_ms
        if idx < 0:
            continue
        if isinstance(value, dict):
            v_min = float(value["min"])
            v_avg = float(value["avg"])
            v_max = float(value["max"])
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            v_min = v_avg = v_max = float(value)
        else:
            continue
        bucket = buckets.get(idx)
        if bucket is None:
            buckets[idx] = {"count": 1, "sum_avg": v_avg, "min": v_min, "max": v_max}
        else:
            bucket["count"] += 1
            bucket["sum_avg"] += v_avg
            if v_min < bucket["min"]:
                bucket["min"] = v_min
            if v_max > bucket["max"]:
                bucket["max"] = v_max
    out: List[Tuple[int, Any]] = []
    for idx in sorted(buckets.keys()):
        bucket = buckets[idx]
        bucket_start = day_start_ms + idx * target_bucket_ms
        ts_ms = bucket_start + (target_bucket_ms // 2)
        avg = round(bucket["sum_avg"] / bucket["count"], scale_decimals)
        if target_elem_size == 1:
            out.append((ts_ms, avg))
        else:
            out.append(
                (
                    ts_ms,
                    {
                        "min": round(bucket["min"], scale_decimals),
                        "avg": avg,
                        "max": round(bucket["max"], scale_decimals),
                    },
                )
            )
    return out


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
        raise TsdbParseError(f"File too small: {path}")
    if raw[:8] != TSDB_TAG_BYTES:
        raise TsdbParseError(f"Invalid TSDB tag in {path!r}")
    version = int.from_bytes(raw[8:12], "little")
    if version != TSDB_VERSION:
        raise TsdbParseError(f"Unsupported TSDB version {version} in {path!r}")

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
                raise TsdbParseError(f"Undefined channel id {channel_id}")
            fmt, name = channel_defs[channel_id]
            value, offset = read_format_value(raw, offset, fmt)
            if name.endswith("/name") and isinstance(value, str):
                latest_name_values[name] = value
            continue
        if entry_type == ENTRY_TYPE_CHANNEL_VALUE_16:
            _ensure_available(raw, offset, 2, "16-bit channel id")
            channel_id = int.from_bytes(raw[offset:offset + 2], "little")
            offset += 2
            if channel_id not in channel_defs:
                raise TsdbParseError(f"Undefined 16-bit channel id {channel_id}")
            fmt, name = channel_defs[channel_id]
            value, offset = read_format_value(raw, offset, fmt)
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
            if current_ts is None:
                raise TsdbParseError("Relative timestamp before absolute timestamp")
            current_ts += raw[offset]
            offset += 1
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_16:
            _ensure_available(raw, offset, 2, "relative timestamp (16-bit)")
            if current_ts is None:
                raise TsdbParseError("Relative timestamp before absolute timestamp")
            current_ts += int.from_bytes(raw[offset:offset + 2], "little")
            offset += 2
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_24:
            rel, offset = _read_u24(raw, offset)
            if current_ts is None:
                raise TsdbParseError("Relative timestamp before absolute timestamp")
            current_ts += rel
            continue
        if entry_type == ENTRY_TYPE_TIME_REL_32:
            _ensure_available(raw, offset, 4, "relative timestamp (32-bit)")
            if current_ts is None:
                raise TsdbParseError("Relative timestamp before absolute timestamp")
            current_ts += int.from_bytes(raw[offset:offset + 4], "little")
            offset += 4
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
        raise TsdbParseError(f"Unknown entry type 0x{entry_type:02x} at offset {offset - 1}")

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


def double_format_id_for_decimals(decimals: int) -> int:
    if decimals <= 0:
        return FORMAT_DOUBLE
    if decimals == 1:
        return FORMAT_DOUBLE_DEC1
    if decimals == 2:
        return FORMAT_DOUBLE_DEC2
    if decimals == 3:
        return FORMAT_DOUBLE_DEC3
    if decimals == 4:
        return FORMAT_DOUBLE_DEC4
    if decimals == 5:
        return FORMAT_DOUBLE_DEC5
    return FORMAT_DOUBLE_DEC6PLUS


def write_downsampled_timeseries_db(
    path: str,
    bucket_ms: int,
    series_order: List[str],
    series_to_format: Dict[str, int],
    numeric_points: Dict[str, List[Tuple[int, Any, Any, Any]]],
    string_points: Dict[str, List[Tuple[int, str]]],
) -> None:
    if bucket_ms <= 0:
        raise ValueError("bucket_ms must be > 0")

    series_names = [name for name in series_order if name in series_to_format]
    series_to_channel: Dict[str, int] = {name: idx for idx, name in enumerate(series_names)}
    per_timestamp: Dict[int, List[Tuple[int, bytes]]] = {}

    for series_name in series_names:
        format_id = series_to_format[series_name]
        channel_id = series_to_channel[series_name]
        if is_numeric_format_id(format_id):
            for timestamp_ms, v_min, v_avg, v_max in numeric_points.get(series_name, []):
                p_min = encode_value_for_format(v_min, format_id)
                p_avg = encode_value_for_format(v_avg, format_id)
                p_max = encode_value_for_format(v_max, format_id)
                if p_min is None or p_avg is None or p_max is None:
                    raise ValueError(f"Cannot encode downsampled numeric value for series={series_name!r}")
                per_timestamp.setdefault(timestamp_ms, []).append((channel_id, p_min + p_avg + p_max))
        elif is_string_format_id(format_id):
            for timestamp_ms, value in string_points.get(series_name, []):
                payload = encode_value_for_format(value, format_id)
                if payload is None:
                    raise ValueError(f"Cannot encode downsampled string value for series={series_name!r}")
                per_timestamp.setdefault(timestamp_ms, []).append((channel_id, payload))

    with open(path, "wb") as f:
        f.write(TSDB_TAG_BYTES)
        f.write(struct.pack("<I", TSDB_VERSION))
        _write_meta_info_entry(f, "dsBucketMs", int(bucket_ms))

        for series_name in series_names:
            channel_id = series_to_channel[series_name]
            format_id = series_to_format[series_name]
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

        current_ts: Optional[int] = None
        for timestamp_ms in sorted(per_timestamp.keys()):
            current_ts = _append_timestamp_entry(f, current_ts, int(timestamp_ms))
            for channel_id, payload in sorted(per_timestamp[timestamp_ms], key=lambda item: item[0]):
                if channel_id <= 0xEF:
                    f.write(bytes([channel_id]))
                else:
                    f.write(bytes([ENTRY_TYPE_CHANNEL_VALUE_16]))
                    f.write(channel_id.to_bytes(2, "little"))
                f.write(payload)
    invalidate_tsdb_cache(path)


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
            invalidate_tsdb_cache(self.path)
            return
        with open(self.path, "rb+") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0:
                f.write(TSDB_TAG_BYTES)
                f.write(struct.pack("<I", TSDB_VERSION))
                invalidate_tsdb_cache(self.path)
            return

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
                if series_name.endswith("/name") and isinstance(value, str) and self.latest_name_values.get(series_name) == value:
                    continue
                if isinstance(value, str):
                    format_id = FORMAT_STRING_U64
                    payload = encode_value_for_format(value, format_id)
                else:
                    decimals_hint = 0
                    numeric_value = value
                    if isinstance(value, NumericWithDecimals):
                        numeric_value = value.value
                        decimals_hint = value.decimals
                    format_id = self.series_to_format.get(series_name)
                    if format_id is None:
                        format_id = double_format_id_for_decimals(decimals_hint)
                    payload = encode_value_for_format(float(numeric_value), format_id)
                if payload is None:
                    raise ValueError(f"Cannot encode value for series={series_name!r}")

                channel_id = self._ensure_series_definition(f, series_name, format_id)
                ts_int = int(timestamp_ms)
                if self.current_timestamp_ms is not None and ts_int < self.current_timestamp_ms:
                    ts_int = self.current_timestamp_ms
                self.current_timestamp_ms = _append_timestamp_entry(f, self.current_timestamp_ms, ts_int)
                if channel_id <= 0xEF:
                    f.write(bytes([channel_id]))
                else:
                    f.write(bytes([ENTRY_TYPE_CHANNEL_VALUE_16]))
                    f.write(channel_id.to_bytes(2, "little"))
                f.write(payload)
                if series_name.endswith("/name") and isinstance(value, str):
                    self.latest_name_values[series_name] = value
        invalidate_tsdb_cache(self.path)
