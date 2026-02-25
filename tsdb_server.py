#!/usr/bin/env python3
"""Simple TSDB REST server using built-in Python modules only.

API:
- GET /health
- GET /series?start=<ts>&end=<ts>
- GET /events?series=<name>&start=<ts>&end=<ts>&maxEvents=<n>
  (or repeated series params for batched response)
- GET /stats?series=<name>&start=<ts>&end=<ts>
- GET /virtual-series
- PUT /virtual-series
- GET /dashboards
- GET /dashboards/<name>
- PUT /dashboards/<name>
- POST /dashboards/<name>/rename
- DELETE /dashboards/<name>
- GET /settings
- PUT /settings

Time parameters support either:
- Unix milliseconds (e.g. 1707000000000)
- ISO-8601 datetime (e.g. 2026-02-15T11:00:00Z)
  Naive timestamps are treated as UTC.

Response for /events:
{
  "series": "...",
  "start": <ms>,
  "end": <ms>,
  "requestedMaxEvents": <n>,
  "returnedPoints": <n>,
  "downsampled": <bool>,
  "points": [...]
}

When not downsampled, points are:
  {"timestamp": <ms>, "value": <number|string>}
When downsampled, points are:
  {"timestamp": <bucket-center-ms>, "start": <bucket-start-ms>, "end": <bucket-end-ms>,
   "count": <n>, "min": <x>, "avg": <x>, "max": <x>}
"""

import argparse
import datetime
import json
import mimetypes
import os
import struct
import threading
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

TSDB_TAG_BYTES = b"TSDB\x00\x00\x00\x00"
TSDB_VERSION = 1
API_VERSION = 9  # Increment when API endpoints or payload schemas change.
SERVER_VERSION = f"tsdb_server.py api-v{API_VERSION}"

ENTRY_TYPE_TIME_ABSOLUTE = 0xF0
ENTRY_TYPE_TIME_REL_8 = 0xF1
ENTRY_TYPE_TIME_REL_16 = 0xF2
ENTRY_TYPE_TIME_REL_24 = 0xF3
ENTRY_TYPE_TIME_REL_32 = 0xF4
ENTRY_TYPE_CHANNEL_DEF_8 = 0xF5
ENTRY_TYPE_CHANNEL_DEF_16 = 0xF6
ENTRY_TYPE_EOF = 0xFE
ENTRY_TYPE_VALUE_16 = 0xFF

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


@dataclass
class Event:
    timestamp_ms: int
    value: Any


class TsdbParseError(ValueError):
    pass


@dataclass
class CachedTsdbFile:
    mtime_ns: int
    size: int
    parsed_offset: int
    current_ts: Optional[int]
    channel_defs: Dict[int, Tuple[int, str]]
    series_format_ids: Dict[str, int]
    series_events: Dict[str, List[Event]]
    ended_with_eof: bool


_TSDB_CACHE_LOCK = threading.Lock()
_TSDB_FILE_CACHE: Dict[str, CachedTsdbFile] = {}


@dataclass
class VirtualSeriesDef:
    name: str
    left: str
    op: str
    right: str


@dataclass
class VirtualSeriesCacheEntry:
    definition: Tuple[str, str, str, str]
    left_sig: Tuple[Any, ...]
    right_sig: Tuple[Any, ...]
    events: List[Event]
    decimal_places: int
    files: List[str]


_VIRTUAL_SERIES_CACHE_LOCK = threading.Lock()
_VIRTUAL_SERIES_RESULT_CACHE: Dict[Tuple[str, str], VirtualSeriesCacheEntry] = {}


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


def _read_format_value(data: bytes, offset: int, format_id: int) -> Tuple[Any, int]:
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


def _parse_tsdb_chunk_into_cache(
    raw: bytes,
    base_offset: int,
    cache: CachedTsdbFile,
) -> Tuple[int, bool]:
    offset = 0
    ended_with_eof = False
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
                value, offset = _read_format_value(raw, offset, format_id)
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
                value, offset = _read_format_value(raw, offset, format_id)
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

            if entry_type == ENTRY_TYPE_EOF:
                ended_with_eof = True
                break

            raise TsdbParseError(f"Unknown entry type 0x{entry_type:02x} at offset {base_offset + offset - 1}")
        except TsdbParseError as exc:
            if _is_incomplete_parse_error(exc):
                offset = entry_start
                break
            raise

    return offset, ended_with_eof


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
        ended_with_eof=False,
    )
    consumed, ended_with_eof = _parse_tsdb_chunk_into_cache(raw[12:], 12, cache)
    cache.parsed_offset = 12 + consumed
    cache.ended_with_eof = ended_with_eof
    return cache


def _refresh_cache_incremental(path: str, st: os.stat_result, cache: CachedTsdbFile) -> CachedTsdbFile:
    parse_from = cache.parsed_offset
    if cache.ended_with_eof and parse_from > 12:
        parse_from -= 1

    if parse_from >= st.st_size:
        cache.mtime_ns = st.st_mtime_ns
        cache.size = st.st_size
        return cache

    with open(path, "rb") as f:
        f.seek(parse_from)
        raw = f.read(st.st_size - parse_from)

    consumed, ended_with_eof = _parse_tsdb_chunk_into_cache(raw, parse_from, cache)
    cache.parsed_offset = parse_from + consumed
    cache.ended_with_eof = ended_with_eof
    cache.mtime_ns = st.st_mtime_ns
    cache.size = st.st_size
    return cache


def _get_cached_tsdb_file(path: str) -> CachedTsdbFile:
    st = os.stat(path)
    with _TSDB_CACHE_LOCK:
        cache = _TSDB_FILE_CACHE.get(path)
        if cache is None:
            cache = _build_cache_from_scratch(path, st)
            _TSDB_FILE_CACHE[path] = cache
            return cache

        if st.st_mtime_ns == cache.mtime_ns and st.st_size == cache.size:
            return cache

        if st.st_size < cache.parsed_offset:
            cache = _build_cache_from_scratch(path, st)
            _TSDB_FILE_CACHE[path] = cache
            return cache

        cache = _refresh_cache_incremental(path, st, cache)
        _TSDB_FILE_CACHE[path] = cache
        return cache


def get_series_format_id_in_file(path: str, series_name: str) -> Optional[int]:
    cache = _get_cached_tsdb_file(path)
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


def read_tsdb_events_for_series(path: str, target_series: str, start_ms: int, end_ms: int) -> List[Event]:
    cache = _get_cached_tsdb_file(path)
    events = cache.series_events.get(target_series, [])
    if not events:
        return []
    if start_ms <= events[0].timestamp_ms and events[-1].timestamp_ms <= end_ms:
        return list(events)
    return [e for e in events if start_ms <= e.timestamp_ms <= end_ms]


def list_series_in_file(path: str) -> List[str]:
    try:
        cache = _get_cached_tsdb_file(path)
    except TsdbParseError:
        return []
    except OSError:
        return []
    return sorted(cache.series_events.keys())


def parse_timestamp(value: str) -> int:
    value = value.strip()
    if not value:
        raise ValueError("timestamp value is empty")

    # Epoch seconds or milliseconds as integer.
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        n = int(value)
        if abs(n) < 10_000_000_000:
            return n * 1000
        return n

    iso = value
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    dt = datetime.datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def day_range_utc(start_ms: int, end_ms: int) -> Iterable[datetime.date]:
    start_day = datetime.datetime.fromtimestamp(start_ms / 1000.0, tz=datetime.timezone.utc).date()
    end_day = datetime.datetime.fromtimestamp(end_ms / 1000.0, tz=datetime.timezone.utc).date()
    day = start_day
    while day <= end_day:
        yield day
        day += datetime.timedelta(days=1)


def find_candidate_files(data_dir: str, start_ms: int, end_ms: int) -> List[str]:
    files: List[str] = []
    for day in day_range_utc(start_ms, end_ms):
        p = os.path.join(data_dir, f"data_{day.isoformat()}.tsdb")
        if os.path.isfile(p):
            files.append(p)

    fallback = os.path.join(data_dir, "data.tsdb")
    if not files and os.path.isfile(fallback):
        files.append(fallback)

    return files


def downsample_numeric_events(
    events: List[Event],
    max_events: int,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
    decimal_places: Optional[int] = None,
) -> Tuple[bool, List[Dict[str, Any]]]:
    if max_events <= 0:
        raise ValueError("maxEvents must be > 0")
    if len(events) <= max_events:
        points = [{"timestamp": e.timestamp_ms, "value": e.value} for e in events]
        return False, points

    if not events:
        return False, []

    start_ts = events[0].timestamp_ms if start_ms is None else start_ms
    end_ts = events[-1].timestamp_ms if end_ms is None else end_ms
    if end_ts < start_ts:
        end_ts = start_ts
    span = max(1, end_ts - start_ts + 1)
    bucket_width = max(1, (span + max_events - 1) // max_events)

    buckets: List[List[Event]] = [[] for _ in range(max_events)]
    for e in events:
        idx = (e.timestamp_ms - start_ts) // bucket_width
        if idx < 0:
            idx = 0
        if idx >= max_events:
            idx = max_events - 1
        buckets[idx].append(e)

    points: List[Dict[str, Any]] = []
    for i, bucket in enumerate(buckets):
        if not bucket:
            continue
        values = [float(ev.value) for ev in bucket]
        b_start = start_ts + i * bucket_width
        b_end = min(end_ts, b_start + bucket_width - 1)
        points.append(
            {
                "timestamp": (b_start + b_end) // 2,
                "start": b_start,
                "end": b_end,
                "count": len(bucket),
                "min": round(min(values), decimal_places) if decimal_places is not None else min(values),
                "avg": round(sum(values) / len(values), decimal_places) if decimal_places is not None else (sum(values) / len(values)),
                "max": round(max(values), decimal_places) if decimal_places is not None else max(values),
            }
        )

    return True, points


def build_error(status: int, code: str, message: str) -> Tuple[int, Dict[str, Any]]:
    return status, {"error": {"code": code, "message": message}}


def _dashboards_file_path(data_dir: str) -> str:
    return os.path.join(data_dir, "dashboards.json")


def load_dashboards(data_dir: str) -> Dict[str, Dict[str, Any]]:
    path = _dashboards_file_path(data_dir)
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        return {}
    dashboards = raw.get("dashboards", raw)
    if not isinstance(dashboards, dict):
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for name, value in dashboards.items():
        if isinstance(name, str) and isinstance(value, dict):
            result[name] = value
    return result


def save_dashboards(data_dir: str, dashboards: Dict[str, Dict[str, Any]]) -> None:
    path = _dashboards_file_path(data_dir)
    tmp = f"{path}.tmp"
    payload = {"dashboards": dashboards}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), indent=2)
        f.write("\n")
    os.replace(tmp, path)


def _settings_file_path(data_dir: str) -> str:
    return os.path.join(data_dir, "settings.json")


def load_settings(data_dir: str) -> Dict[str, Any]:
    path = _settings_file_path(data_dir)
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        return {}
    settings = raw.get("settings", raw)
    if not isinstance(settings, dict):
        return {}
    return settings


def save_settings(data_dir: str, settings: Dict[str, Any]) -> None:
    path = _settings_file_path(data_dir)
    tmp = f"{path}.tmp"
    payload = {"settings": settings}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), indent=2)
        f.write("\n")
    os.replace(tmp, path)


def _virtual_series_file_path(data_dir: str) -> str:
    return os.path.join(data_dir, "virtual_series.json")


def _normalize_virtual_series_def(obj: Any) -> Optional[VirtualSeriesDef]:
    if not isinstance(obj, dict):
        return None
    name = str(obj.get("name", "")).strip()
    left = str(obj.get("left", "")).strip()
    op = str(obj.get("op", "")).strip()
    right = str(obj.get("right", "")).strip()
    if not name or not left or op not in {"+", "-", "*", "/", "today"}:
        return None
    if op != "today" and not right:
        return None
    return VirtualSeriesDef(name=name, left=left, op=op, right=right)


def _normalize_unit_override_def(obj: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(obj, dict):
        return None
    suffix = str(obj.get("suffix", "")).strip().strip("/")
    unit = str(obj.get("unit", "")).strip()
    scale_op = str(obj.get("scaleOp", obj.get("op", "*"))).strip()
    try:
        scale = float(obj.get("scale", 1))
        decimals = int(obj.get("decimals"))
    except Exception:
        return None
    if not suffix or decimals < 0 or decimals > 6 or scale <= 0 or scale_op not in {"*", "/"}:
        return None
    return {"suffix": suffix, "unit": unit, "scale": scale, "scaleOp": scale_op, "decimals": decimals}


def load_virtual_series_config(data_dir: str) -> Tuple[List[VirtualSeriesDef], List[Dict[str, Any]]]:
    path = _virtual_series_file_path(data_dir)
    if not os.path.isfile(path):
        return [], []
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, list):
        items = raw
        overrides_raw = []
    elif isinstance(raw, dict):
        items = raw.get("virtualSeries", [])
        overrides_raw = raw.get("unitOverrides", raw.get("decimalOverrides", []))
    else:
        return [], []
    if not isinstance(items, list):
        items = []
    if not isinstance(overrides_raw, list):
        overrides_raw = []
    defs: List[VirtualSeriesDef] = []
    seen_names: set[str] = set()
    for item in items:
        d = _normalize_virtual_series_def(item)
        if d is None or d.name in seen_names:
            continue
        seen_names.add(d.name)
        defs.append(d)
    overrides: List[Dict[str, Any]] = []
    seen_suffixes: set[str] = set()
    for item in overrides_raw:
        d = _normalize_unit_override_def(item)
        if d is None:
            continue
        key = str(d["suffix"]).lower()
        if key in seen_suffixes:
            continue
        seen_suffixes.add(key)
        overrides.append(d)
    return defs, overrides


def load_virtual_series_defs(data_dir: str) -> List[VirtualSeriesDef]:
    defs, _overrides = load_virtual_series_config(data_dir)
    return defs


def save_virtual_series_config(data_dir: str, defs: List[VirtualSeriesDef], unit_overrides: List[Dict[str, Any]]) -> None:
    path = _virtual_series_file_path(data_dir)
    tmp = f"{path}.tmp"
    payload = {
        "virtualSeries": [
            {"name": d.name, "left": d.left, "op": d.op, "right": d.right}
            for d in defs
        ],
        "unitOverrides": [
            {
                "suffix": str(d["suffix"]),
                "unit": str(d.get("unit", "")),
                "scale": float(d.get("scale", 1)),
                "scaleOp": str(d.get("scaleOp", "*")),
                "decimals": int(d["decimals"]),
            }
            for d in unit_overrides
        ],
    }
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), indent=2)
        f.write("\n")
    os.replace(tmp, path)
    with _VIRTUAL_SERIES_CACHE_LOCK:
        _VIRTUAL_SERIES_RESULT_CACHE.clear()


def save_virtual_series_defs(data_dir: str, defs: List[VirtualSeriesDef]) -> None:
    _defs, overrides = load_virtual_series_config(data_dir)
    save_virtual_series_config(data_dir, defs, overrides)


def _all_tsdb_files(data_dir: str) -> List[str]:
    try:
        names = os.listdir(data_dir)
    except OSError:
        return []
    files: List[str] = []
    for name in names:
        if name == "data.tsdb" or (name.startswith("data_") and name.endswith(".tsdb")):
            path = os.path.join(data_dir, name)
            if os.path.isfile(path):
                files.append(path)
    files.sort()
    return files


def _read_series_all_files(data_dir: str, series_name: str) -> Tuple[List[Event], int, List[str], Tuple[Any, ...]]:
    events: List[Event] = []
    max_decimal_places = 3
    files_used: List[str] = []
    signature_parts: List[Any] = []
    for path in _all_tsdb_files(data_dir):
        part = read_tsdb_events_for_series(path, series_name, 0, 2**63 - 1)
        if part:
            events.extend(part)
            base = os.path.basename(path)
            files_used.append(base)
            signature_parts.append((base, len(part), part[-1].timestamp_ms))
        fmt = get_series_format_id_in_file(path, series_name)
        if is_numeric_format_id(fmt):
            max_decimal_places = max(max_decimal_places, decimal_places_from_format_id(fmt))
    events.sort(key=lambda e: e.timestamp_ms)
    return events, max_decimal_places, files_used, tuple(signature_parts)


def _compute_virtual_events(left_events: List[Event], right_events: List[Event], op: str) -> List[Event]:
    result: List[Event] = []
    i = 0
    j = 0
    while i < len(left_events) and j < len(right_events):
        lt = left_events[i].timestamp_ms
        rt = right_events[j].timestamp_ms
        if lt < rt:
            i += 1
            continue
        if rt < lt:
            j += 1
            continue
        lv = left_events[i].value
        rv = right_events[j].value
        if isinstance(lv, (int, float)) and not isinstance(lv, bool) and isinstance(rv, (int, float)) and not isinstance(rv, bool):
            a = float(lv)
            b = float(rv)
            out: Optional[float]
            if op == "+":
                out = a + b
            elif op == "-":
                out = a - b
            elif op == "*":
                out = a * b
            elif op == "/":
                out = None if b == 0 else (a / b)
            else:
                out = None
            if out is not None and out == out and abs(out) != float("inf"):
                result.append(Event(lt, out))
        i += 1
        j += 1
    return result


def _compute_virtual_events_today(events: List[Event]) -> List[Event]:
    result: List[Event] = []
    day_start_value: Dict[Tuple[int, int, int], float] = {}
    for ev in events:
        v = ev.value
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            continue
        dt = datetime.datetime.fromtimestamp(ev.timestamp_ms / 1000.0).astimezone()
        key = (dt.year, dt.month, dt.day)
        fv = day_start_value.get(key)
        cur = float(v)
        if fv is None:
            day_start_value[key] = cur
            out = 0.0
        else:
            out = cur - fv
            if out < 0:
                out = 0.0
        if out == out and abs(out) != float("inf"):
            result.append(Event(ev.timestamp_ms, out))
    return result


def _parse_virtual_constant(value: str) -> Optional[float]:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        n = float(s)
    except (TypeError, ValueError):
        return None
    if n != n or abs(n) == float("inf"):
        return None
    return n


def _compute_virtual_events_with_constant(events: List[Event], constant: float, op: str, constant_on_left: bool) -> List[Event]:
    result: List[Event] = []
    for ev in events:
        v = ev.value
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            continue
        a = constant if constant_on_left else float(v)
        b = float(v) if constant_on_left else constant
        out: Optional[float]
        if op == "+":
            out = a + b
        elif op == "-":
            out = a - b
        elif op == "*":
            out = a * b
        elif op == "/":
            out = None if b == 0 else (a / b)
        else:
            out = None
        if out is not None and out == out and abs(out) != float("inf"):
            result.append(Event(ev.timestamp_ms, out))
    return result


def _virtual_decimal_places(op: str, left_dp: int, right_dp: int) -> int:
    if op == "today":
        return left_dp
    if op in {"+", "-"}:
        return max(left_dp, right_dp)
    if op == "*":
        return min(6, max(3, left_dp + right_dp))
    if op == "/":
        return max(3, left_dp)
    return 3


def _virtual_defs_map(data_dir: str) -> Dict[str, VirtualSeriesDef]:
    return {d.name: d for d in load_virtual_series_defs(data_dir)}


def get_virtual_series_events_cached(data_dir: str, series_name: str) -> Optional[Tuple[List[Event], int, List[str]]]:
    defs = _virtual_defs_map(data_dir)
    d = defs.get(series_name)
    if d is None:
        return None
    if d.op == "today":
        left_const = _parse_virtual_constant(d.left)
        if left_const is not None:
            return [], 3, []
        left_events, left_dp, left_files, left_sig = _read_series_all_files(data_dir, d.left)
        cache_key = (os.path.abspath(data_dir), d.name)
        definition = (d.name, d.left, d.op, d.right)
        with _VIRTUAL_SERIES_CACHE_LOCK:
            entry = _VIRTUAL_SERIES_RESULT_CACHE.get(cache_key)
            if (
                entry is not None
                and entry.definition == definition
                and entry.left_sig == left_sig
                and entry.right_sig == ("today",)
            ):
                return list(entry.events), entry.decimal_places, list(entry.files)
        events = _compute_virtual_events_today(left_events)
        decimal_places = _virtual_decimal_places(d.op, left_dp, 0)
        files = sorted(set(left_files))
        cache_entry = VirtualSeriesCacheEntry(
            definition=definition,
            left_sig=left_sig,
            right_sig=("today",),
            events=list(events),
            decimal_places=decimal_places,
            files=list(files),
        )
        with _VIRTUAL_SERIES_CACHE_LOCK:
            _VIRTUAL_SERIES_RESULT_CACHE[cache_key] = cache_entry
        return list(events), decimal_places, list(files)
    left_const = _parse_virtual_constant(d.left)
    right_const = _parse_virtual_constant(d.right)
    left_is_const = left_const is not None
    right_is_const = right_const is not None
    if left_is_const and right_is_const:
        return [], 3, []
    if left_is_const:
        right_events, right_dp, right_files, right_sig = _read_series_all_files(data_dir, d.right)
        left_events = []
        left_dp = 3
        left_files = []
        left_sig = ("const", left_const)
    else:
        left_events, left_dp, left_files, left_sig = _read_series_all_files(data_dir, d.left)
    if right_is_const:
        right_events = []
        right_dp = 3
        right_files = []
        right_sig = ("const", right_const)
    elif not left_is_const:
        right_events, right_dp, right_files, right_sig = _read_series_all_files(data_dir, d.right)
    cache_key = (os.path.abspath(data_dir), d.name)
    definition = (d.name, d.left, d.op, d.right)
    with _VIRTUAL_SERIES_CACHE_LOCK:
        entry = _VIRTUAL_SERIES_RESULT_CACHE.get(cache_key)
        if (
            entry is not None
            and entry.definition == definition
            and entry.left_sig == left_sig
            and entry.right_sig == right_sig
        ):
            return list(entry.events), entry.decimal_places, list(entry.files)
    if left_is_const:
        events = _compute_virtual_events_with_constant(right_events, float(left_const), d.op, True)
    elif right_is_const:
        events = _compute_virtual_events_with_constant(left_events, float(right_const), d.op, False)
    else:
        events = _compute_virtual_events(left_events, right_events, d.op)
    decimal_places = _virtual_decimal_places(d.op, left_dp, right_dp)
    files = sorted(set(left_files + right_files))
    cache_entry = VirtualSeriesCacheEntry(
        definition=definition,
        left_sig=left_sig,
        right_sig=right_sig,
        events=list(events),
        decimal_places=decimal_places,
        files=list(files),
    )
    with _VIRTUAL_SERIES_CACHE_LOCK:
        _VIRTUAL_SERIES_RESULT_CACHE[cache_key] = cache_entry
    return list(events), decimal_places, list(files)


class TsdbRequestHandler(BaseHTTPRequestHandler):
    server_version = "TSDBServer/1.0"

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            return

    def _query_param(self, params: Dict[str, List[str]], name: str, required: bool = False) -> Optional[str]:
        values = params.get(name)
        if not values:
            if required:
                raise ValueError(f"Missing required query parameter: {name}")
            return None
        return values[0]

    def _send_bytes(self, status: int, body: bytes, content_type: str) -> None:
        try:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            return

    def _handle_static(self, path: str) -> bool:
        ui_dir = self.server.ui_dir  # type: ignore[attr-defined]
        if not ui_dir:
            return False

        rel = ""
        if path in ("/", "/index.html"):
            rel = "index.html"
        elif path.startswith("/static/"):
            rel = path[len("/static/"):]
        else:
            return False

        rel = unquote(rel).lstrip("/")
        full_path = os.path.abspath(os.path.join(ui_dir, rel))
        ui_dir_abs = os.path.abspath(ui_dir)
        if not (full_path == ui_dir_abs or full_path.startswith(ui_dir_abs + os.sep)):
            status, payload = build_error(400, "bad_request", "Invalid static path")
            self._send_json(status, payload)
            return True
        if not os.path.isfile(full_path):
            status, payload = build_error(404, "not_found", f"Static file not found: {rel}")
            self._send_json(status, payload)
            return True

        with open(full_path, "rb") as f:
            body = f.read()
        mime, _ = mimetypes.guess_type(full_path)
        self._send_bytes(200, body, mime or "application/octet-stream")
        return True

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query, keep_blank_values=False)

        try:
            if self._handle_static(path):
                return
            if path == "/health":
                self._send_json(200, {"ok": True, "apiVersion": API_VERSION, "serverVersion": SERVER_VERSION})
                return
            if path == "/series":
                self._handle_series(params)
                return
            if path == "/events":
                self._handle_events(params)
                return
            if path == "/stats":
                self._handle_stats(params)
                return
            if path == "/virtual-series":
                self._handle_virtual_series_get()
                return
            if path == "/dashboards":
                self._handle_dashboards_list()
                return
            if path.startswith("/dashboards/"):
                self._handle_dashboards_get(path)
                return
            if path == "/settings":
                self._handle_settings_get()
                return

            status, payload = build_error(404, "not_found", f"Unknown endpoint: {path}")
            self._send_json(status, payload)
        except ValueError as exc:
            status, payload = build_error(400, "bad_request", str(exc))
            self._send_json(status, payload)
        except TsdbParseError as exc:
            status, payload = build_error(500, "tsdb_parse_error", str(exc))
            self._send_json(status, payload)
        except OSError as exc:
            status, payload = build_error(500, "io_error", str(exc))
            self._send_json(status, payload)
        except Exception as exc:  # safety net
            status, payload = build_error(500, "internal_error", str(exc))
            self._send_json(status, payload)

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path.startswith("/dashboards/"):
                self._handle_dashboards_put(path)
                return
            if path == "/settings":
                self._handle_settings_put()
                return
            if path == "/virtual-series":
                self._handle_virtual_series_put()
                return
            status, payload = build_error(404, "not_found", f"Unknown endpoint: {path}")
            self._send_json(status, payload)
        except ValueError as exc:
            status, payload = build_error(400, "bad_request", str(exc))
            self._send_json(status, payload)
        except OSError as exc:
            status, payload = build_error(500, "io_error", str(exc))
            self._send_json(status, payload)
        except Exception as exc:
            status, payload = build_error(500, "internal_error", str(exc))
            self._send_json(status, payload)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path.startswith("/dashboards/") and path.endswith("/rename"):
                self._handle_dashboards_rename(path)
                return
            status, payload = build_error(404, "not_found", f"Unknown endpoint: {path}")
            self._send_json(status, payload)
        except ValueError as exc:
            status, payload = build_error(400, "bad_request", str(exc))
            self._send_json(status, payload)
        except OSError as exc:
            status, payload = build_error(500, "io_error", str(exc))
            self._send_json(status, payload)
        except Exception as exc:
            status, payload = build_error(500, "internal_error", str(exc))
            self._send_json(status, payload)

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path.startswith("/dashboards/"):
                self._handle_dashboards_delete(path)
                return
            status, payload = build_error(404, "not_found", f"Unknown endpoint: {path}")
            self._send_json(status, payload)
        except ValueError as exc:
            status, payload = build_error(400, "bad_request", str(exc))
            self._send_json(status, payload)
        except OSError as exc:
            status, payload = build_error(500, "io_error", str(exc))
            self._send_json(status, payload)
        except Exception as exc:
            status, payload = build_error(500, "internal_error", str(exc))
            self._send_json(status, payload)

    def _handle_series(self, params: Dict[str, List[str]]) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]

        start_raw = self._query_param(params, "start")
        end_raw = self._query_param(params, "end")
        start_ms = parse_timestamp(start_raw) if start_raw else 0
        end_ms = parse_timestamp(end_raw) if end_raw else int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
        if end_ms < start_ms:
            raise ValueError("end must be >= start")

        files = find_candidate_files(data_dir, start_ms, end_ms)
        names = set()
        for path in files:
            for name in list_series_in_file(path):
                names.add(name)
        for d in load_virtual_series_defs(data_dir):
            names.add(d.name)

        self._send_json(
            200,
            {
                "start": start_ms,
                "end": end_ms,
                "files": [os.path.basename(p) for p in files],
                "series": sorted(names),
            },
        )

    def _handle_events(self, params: Dict[str, List[str]]) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        series_values = [str(s) for s in params.get("series", []) if str(s)]
        if not series_values:
            raise ValueError("Missing required query parameter: series")
        start_raw = self._query_param(params, "start", required=True)
        end_raw = self._query_param(params, "end", required=True)
        max_events_raw = self._query_param(params, "maxEvents", required=True)
        assert start_raw is not None and end_raw is not None and max_events_raw is not None

        start_ms = parse_timestamp(start_raw)
        end_ms = parse_timestamp(end_raw)
        max_events = int(max_events_raw)

        if end_ms < start_ms:
            raise ValueError("end must be >= start")
        if max_events <= 0:
            raise ValueError("maxEvents must be > 0")

        if len(series_values) == 1:
            self._send_json(200, self._events_for_series(data_dir, series_values[0], start_ms, end_ms, max_events))
            return
        items = [self._events_for_series(data_dir, s, start_ms, end_ms, max_events) for s in series_values]
        self._send_json(
            200,
            {
                "start": start_ms,
                "end": end_ms,
                "requestedMaxEvents": max_events,
                "requestedSeries": series_values,
                "events": items,
            },
        )

    def _events_for_series(self, data_dir: str, series: str, start_ms: int, end_ms: int, max_events: int) -> Dict[str, Any]:
        files = find_candidate_files(data_dir, start_ms, end_ms)
        events: List[Event] = []
        max_decimal_places: Optional[int] = None
        virtual = get_virtual_series_events_cached(data_dir, series)
        if virtual is not None:
            all_events, max_decimal_places, virtual_files = virtual
            events = [e for e in all_events if start_ms <= e.timestamp_ms <= end_ms]
            files = [os.path.join(data_dir, f) for f in virtual_files]
        else:
            for path in files:
                events.extend(read_tsdb_events_for_series(path, series, start_ms, end_ms))
                fmt = get_series_format_id_in_file(path, series)
                if is_numeric_format_id(fmt):
                    d = decimal_places_from_format_id(fmt)
                    max_decimal_places = d if max_decimal_places is None else max(max_decimal_places, d)

        events.sort(key=lambda e: e.timestamp_ms)

        all_numeric = all(isinstance(ev.value, (int, float)) and not isinstance(ev.value, bool) for ev in events)
        if all_numeric:
            downsampled, points = downsample_numeric_events(
                events,
                max_events,
                start_ms=start_ms,
                end_ms=end_ms,
                decimal_places=max_decimal_places if max_decimal_places is not None else 3,
            )
        else:
            downsampled = False
            points = [{"timestamp": e.timestamp_ms, "value": e.value} for e in events[:max_events]]

        response: Dict[str, Any] = {
            "series": series,
            "start": start_ms,
            "end": end_ms,
            "requestedMaxEvents": max_events,
            "returnedPoints": len(points),
            "downsampled": downsampled,
            "files": [os.path.basename(p) for p in files],
            "points": points,
        }
        if max_decimal_places is not None:
            response["decimalPlaces"] = max_decimal_places
        if not all_numeric and len(events) > max_events:
            response["note"] = "Series is non-numeric; returned first maxEvents without min/avg/max aggregation."
        return response

    def _handle_stats(self, params: Dict[str, List[str]]) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        start_raw = self._query_param(params, "start", required=True)
        end_raw = self._query_param(params, "end", required=True)
        series_values = [str(s) for s in params.get("series", []) if str(s)]
        if not series_values:
            raise ValueError("Missing required query parameter: series")
        assert start_raw is not None and end_raw is not None

        start_ms = parse_timestamp(start_raw)
        end_ms = parse_timestamp(end_raw)
        if end_ms < start_ms:
            raise ValueError("end must be >= start")
        if len(series_values) == 1:
            self._send_json(200, self._stats_for_series(data_dir, series_values[0], start_ms, end_ms))
            return
        stats_list = [self._stats_for_series(data_dir, s, start_ms, end_ms) for s in series_values]
        self._send_json(
            200,
            {
                "start": start_ms,
                "end": end_ms,
                "requestedSeries": series_values,
                "stats": stats_list,
            },
        )

    def _stats_for_series(self, data_dir: str, series: str, start_ms: int, end_ms: int) -> Dict[str, Any]:
        files = find_candidate_files(data_dir, start_ms, end_ms)
        events: List[Event] = []
        max_decimal_places: Optional[int] = None
        virtual = get_virtual_series_events_cached(data_dir, series)
        if virtual is not None:
            all_events, max_decimal_places, virtual_files = virtual
            events = [e for e in all_events if start_ms <= e.timestamp_ms <= end_ms]
            files = [os.path.join(data_dir, f) for f in virtual_files]
        else:
            for path in files:
                events.extend(read_tsdb_events_for_series(path, series, start_ms, end_ms))
                fmt = get_series_format_id_in_file(path, series)
                if is_numeric_format_id(fmt):
                    d = decimal_places_from_format_id(fmt)
                    max_decimal_places = d if max_decimal_places is None else max(max_decimal_places, d)
        events.sort(key=lambda e: e.timestamp_ms)

        current_ts: Optional[int] = None
        current_value: Any = None
        max_value: Optional[float] = None
        numeric_count = 0
        for ev in events:
            current_ts = ev.timestamp_ms
            current_value = ev.value
            if isinstance(ev.value, (int, float)) and not isinstance(ev.value, bool):
                numeric_count += 1
                v = float(ev.value)
                if max_value is None or v > max_value:
                    max_value = v

        return {
            "series": series,
            "start": start_ms,
            "end": end_ms,
            "count": len(events),
            "numericCount": numeric_count,
            "isNumeric": numeric_count == len(events) and len(events) > 0,
            "currentTimestamp": current_ts,
            "currentValue": current_value,
            "maxValue": max_value,
            "decimalPlaces": max_decimal_places,
            "files": [os.path.basename(p) for p in files],
        }

    def _handle_virtual_series_get(self) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        defs, overrides = load_virtual_series_config(data_dir)
        self._send_json(
            200,
            {
                "virtualSeries": [
                    {"name": d.name, "left": d.left, "op": d.op, "right": d.right}
                    for d in defs
                ],
                "unitOverrides": overrides,
            },
        )

    def _handle_virtual_series_put(self) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        length_raw = self.headers.get("Content-Length", "").strip()
        if not length_raw:
            raise ValueError("Missing Content-Length")
        try:
            length = int(length_raw)
        except ValueError:
            raise ValueError("Invalid Content-Length")
        if length <= 0:
            raise ValueError("Empty request body")
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            raise ValueError("Invalid JSON body")
        if not isinstance(payload, dict):
            raise ValueError("virtual-series payload must be an object")
        items = payload.get("virtualSeries", [])
        overrides_raw = payload.get("unitOverrides", payload.get("decimalOverrides", []))
        if not isinstance(items, list):
            raise ValueError("virtualSeries payload must be a list")
        if not isinstance(overrides_raw, list):
            raise ValueError("unitOverrides payload must be a list")
        defs: List[VirtualSeriesDef] = []
        seen: set[str] = set()
        for item in items:
            d = _normalize_virtual_series_def(item)
            if d is None:
                raise ValueError("Each virtual series must include name,left,op and valid operator (+ - * / today); right is optional for today")
            if d.name in seen:
                raise ValueError(f"Duplicate virtual series name: {d.name}")
            seen.add(d.name)
            defs.append(d)
        overrides: List[Dict[str, Any]] = []
        seen_suffixes: set[str] = set()
        for item in overrides_raw:
            d = _normalize_unit_override_def(item)
            if d is None:
                raise ValueError("Each unit override must include suffix, unit, scale, scaleOp, decimals")
            key = str(d["suffix"]).lower()
            if key in seen_suffixes:
                raise ValueError(f"Duplicate unit override suffix: {d['suffix']}")
            seen_suffixes.add(key)
            overrides.append(d)
        save_virtual_series_config(data_dir, defs, overrides)
        self._send_json(200, {"ok": True, "count": len(defs), "unitOverrideCount": len(overrides)})

    def _dashboard_name_from_path(self, path: str) -> str:
        raw = path[len("/dashboards/"):]
        name = unquote(raw).strip()
        return self._validate_dashboard_name(name)

    def _validate_dashboard_name(self, name: str) -> str:
        if not name:
            raise ValueError("Dashboard name must not be empty")
        if "/" in name:
            raise ValueError("Dashboard name must not contain '/'")
        if name == "Default":
            raise ValueError("'Default' is reserved and synthesized; save under a different name")
        return name

    def _handle_dashboards_list(self) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        dashboards = load_dashboards(data_dir)
        self._send_json(200, {"dashboards": sorted(dashboards.keys())})

    def _handle_dashboards_get(self, path: str) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        name = self._dashboard_name_from_path(path)
        dashboards = load_dashboards(data_dir)
        dashboard = dashboards.get(name)
        if dashboard is None:
            status, payload = build_error(404, "not_found", f"Dashboard not found: {name}")
            self._send_json(status, payload)
            return
        self._send_json(200, {"name": name, "dashboard": dashboard})

    def _handle_dashboards_put(self, path: str) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        name = self._dashboard_name_from_path(path)
        length_raw = self.headers.get("Content-Length", "").strip()
        if not length_raw:
            raise ValueError("Missing Content-Length")
        try:
            length = int(length_raw)
        except ValueError:
            raise ValueError("Invalid Content-Length")
        if length <= 0:
            raise ValueError("Empty request body")
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            raise ValueError("Invalid JSON body")
        dashboard = payload.get("dashboard", payload) if isinstance(payload, dict) else None
        if not isinstance(dashboard, dict):
            raise ValueError("Dashboard payload must be an object")

        dashboards = load_dashboards(data_dir)
        dashboards[name] = dashboard
        save_dashboards(data_dir, dashboards)
        self._send_json(200, {"ok": True, "name": name})

    def _handle_dashboards_delete(self, path: str) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        name = self._dashboard_name_from_path(path)
        dashboards = load_dashboards(data_dir)
        if name not in dashboards:
            status, payload = build_error(404, "not_found", f"Dashboard not found: {name}")
            self._send_json(status, payload)
            return
        del dashboards[name]
        save_dashboards(data_dir, dashboards)
        self._send_json(200, {"ok": True, "name": name, "deleted": True})

    def _handle_dashboards_rename(self, path: str) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        old_raw = path[len("/dashboards/"):-len("/rename")]
        old_name = self._validate_dashboard_name(unquote(old_raw).strip().rstrip("/"))
        length_raw = self.headers.get("Content-Length", "").strip()
        if not length_raw:
            raise ValueError("Missing Content-Length")
        try:
            length = int(length_raw)
        except ValueError:
            raise ValueError("Invalid Content-Length")
        if length <= 0:
            raise ValueError("Empty request body")
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            raise ValueError("Invalid JSON body")
        if not isinstance(payload, dict):
            raise ValueError("Rename payload must be an object")
        new_name_raw = payload.get("newName")
        if not isinstance(new_name_raw, str):
            raise ValueError("Rename payload must include string newName")
        new_name = self._validate_dashboard_name(new_name_raw.strip())
        dashboards = load_dashboards(data_dir)
        if old_name not in dashboards:
            status, payload = build_error(404, "not_found", f"Dashboard not found: {old_name}")
            self._send_json(status, payload)
            return
        if new_name in dashboards and new_name != old_name:
            status, payload = build_error(409, "conflict", f"Dashboard already exists: {new_name}")
            self._send_json(status, payload)
            return
        dashboards[new_name] = dashboards.pop(old_name)
        save_dashboards(data_dir, dashboards)
        self._send_json(200, {"ok": True, "oldName": old_name, "newName": new_name})

    def _handle_settings_get(self) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        settings = load_settings(data_dir)
        self._send_json(200, {"settings": settings})

    def _handle_settings_put(self) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        length_raw = self.headers.get("Content-Length", "").strip()
        if not length_raw:
            raise ValueError("Missing Content-Length")
        try:
            length = int(length_raw)
        except ValueError:
            raise ValueError("Invalid Content-Length")
        if length <= 0:
            raise ValueError("Empty request body")
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            raise ValueError("Invalid JSON body")
        settings = payload.get("settings", payload) if isinstance(payload, dict) else None
        if not isinstance(settings, dict):
            raise ValueError("Settings payload must be an object")
        save_settings(data_dir, settings)
        self._send_json(200, {"ok": True})

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep plain stderr logging with timestamp from BaseHTTPRequestHandler.
        super().log_message(fmt, *args)


class TsdbHttpServer(ThreadingHTTPServer):
    def __init__(self, server_address: Tuple[str, int], data_dir: str, ui_dir: Optional[str]):
        super().__init__(server_address, TsdbRequestHandler)
        self.data_dir = data_dir
        self.ui_dir = ui_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TSDB REST server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Bind port (default: 8080)")
    parser.add_argument(
        "--data-dir",
        default=".",
        help="Directory containing TSDB files like data_YYYY-MM-DD.tsdb (default: current dir)",
    )
    parser.add_argument(
        "--ui-dir",
        default=os.path.join(os.path.dirname(__file__), "dashboard_ui"),
        help="Directory containing frontend assets (default: ./dashboard_ui next to tsdb_server.py)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = os.path.abspath(args.data_dir)
    ui_dir = os.path.abspath(args.ui_dir) if args.ui_dir else None

    if not os.path.isdir(data_dir):
        raise SystemExit(f"Data directory not found: {data_dir}")
    if ui_dir is not None and not os.path.isdir(ui_dir):
        raise SystemExit(f"UI directory not found: {ui_dir}")
    if not (1 <= args.port <= 65535):
        raise SystemExit("--port must be in range 1..65535")

    httpd = TsdbHttpServer((args.host, args.port), data_dir, ui_dir)
    print(
        f"Serving TSDB REST API on http://{args.host}:{args.port} "
        f"(data_dir={data_dir}, ui_dir={ui_dir})"
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
