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
import bisect
import datetime
import json
import mimetypes
import os
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

API_VERSION = 14  # Increment when API endpoints or payload schemas change.
SERVER_VERSION = f"tsdb_server.py api-v{API_VERSION}"
STATS_MAX_EVENTS = 1200

from tsdb import (
    Event,
    TsdbParseError,
    decimal_places_from_format_id,
    get_cached_tsdb_file,
    get_series_format_id_in_file,
    is_numeric_format_id,
    is_string_format_id,
    list_series_in_file,
    read_tsdb_events_for_series,
    write_downsampled_timeseries_db,
)


@dataclass
class VirtualSeriesDef:
    name: str
    left: str
    op: str
    right: str


@dataclass
class VirtualSeriesCacheEntry:
    definition: Tuple[Any, ...]
    left_sig: Tuple[Any, ...]
    right_sig: Tuple[Any, ...]
    events: List[Event]
    decimal_places: int
    files: List[str]


@dataclass
class SeriesAllFilesCacheEntry:
    file_signatures: Tuple[Tuple[Any, ...], ...]
    events: List[Event]
    decimal_places: int
    files: List[str]


_VIRTUAL_SERIES_CACHE_LOCK = threading.Lock()
_VIRTUAL_SERIES_RESULT_CACHE: Dict[Tuple[str, str], VirtualSeriesCacheEntry] = {}

_SERIES_ALL_FILES_CACHE_LOCK = threading.Lock()
_SERIES_ALL_FILES_CACHE: Dict[Tuple[str, str], SeriesAllFilesCacheEntry] = {}
_REQUEST_TRACE = threading.local()

_DOWNSAMPLE_BUCKETS: List[Tuple[int, str]] = [
    (5_000, "5s"),
    (15_000, "15s"),
    (60_000, "1m"),
    (300_000, "5m"),
    (900_000, "15m"),
]


@dataclass
class DownsampledDayCacheEntry:
    source_signature: Tuple[Any, ...]
    bucket_ms: int
    day: datetime.date
    numeric_series: Dict[str, List[Dict[str, Any]]]
    string_series: Dict[str, List[Dict[str, Any]]]
    series_format_ids: Dict[str, int]
    files: List[str]
    raw_parsed_offset: int = 0
    raw_series_counts: Optional[Dict[str, int]] = None
    pending_numeric: Optional[Dict[str, Dict[int, Dict[str, Any]]]] = None
    pending_string: Optional[Dict[str, Dict[int, str]]] = None


_DOWNSAMPLE_DAY_CACHE_LOCK = threading.Lock()
_DOWNSAMPLE_DAY_CACHE: Dict[Tuple[str, int], DownsampledDayCacheEntry] = {}


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


def _original_file_for_day(data_dir: str, day: datetime.date) -> str:
    return os.path.join(data_dir, f"data_{day.isoformat()}.tsdb")


def _downsampled_file_for_day(data_dir: str, day: datetime.date, bucket_ms: int) -> str:
    label = next((name for ms, name in _DOWNSAMPLE_BUCKETS if ms == bucket_ms), None)
    if label is None:
        raise ValueError(f"Unsupported bucket size: {bucket_ms}")
    return os.path.join(data_dir, f"data{label}_{day.isoformat()}.tsdb")


def _day_start_ms(day: datetime.date) -> int:
    dt = datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def _bucket_center_ms(bucket_start_ms: int, bucket_ms: int) -> int:
    return bucket_start_ms + (bucket_ms // 2)


def _choose_fixed_bucket_ms(start_ms: int, end_ms: int, max_events: int) -> int:
    span = max(1, end_ms - start_ms + 1)
    for bucket_ms, _name in _DOWNSAMPLE_BUCKETS:
        if (span + bucket_ms - 1) // bucket_ms <= max_events:
            return bucket_ms
    return _DOWNSAMPLE_BUCKETS[-1][0]


def _build_downsampled_day_cache_from_original(path: str, day: datetime.date, bucket_ms: int) -> DownsampledDayCacheEntry:
    cache = get_cached_tsdb_file(path)
    day_start_ms = _day_start_ms(day)
    day_end_ms = day_start_ms + 86_400_000 - 1
    numeric_series: Dict[str, List[Dict[str, Any]]] = {}
    string_series: Dict[str, List[Dict[str, Any]]] = {}
    series_format_ids = dict(cache.series_format_ids)

    channel_order: List[str] = []
    for channel_id in sorted(cache.channel_defs.keys()):
        _fmt, series_name = cache.channel_defs[channel_id]
        if series_name not in channel_order:
            channel_order.append(series_name)

    for series_name in channel_order:
        format_id = cache.series_format_ids.get(series_name)
        events = cache.series_events.get(series_name, [])
        if not events:
            continue
        if is_numeric_format_id(format_id):
            buckets: Dict[int, Dict[str, Any]] = {}
            for ev in events:
                ts = ev.timestamp_ms
                if ts < day_start_ms or ts > day_end_ms:
                    continue
                if not isinstance(ev.value, (int, float)) or isinstance(ev.value, bool):
                    continue
                bucket_idx = (ts - day_start_ms) // bucket_ms
                bucket = buckets.get(bucket_idx)
                value = float(ev.value)
                if bucket is None:
                    buckets[bucket_idx] = {"count": 1, "sum": value, "min": value, "max": value}
                else:
                    bucket["count"] += 1
                    bucket["sum"] += value
                    if value < bucket["min"]:
                        bucket["min"] = value
                    if value > bucket["max"]:
                        bucket["max"] = value
            points: List[Dict[str, Any]] = []
            for bucket_idx in sorted(buckets.keys()):
                bucket = buckets[bucket_idx]
                bucket_start = day_start_ms + bucket_idx * bucket_ms
                bucket_end = min(day_end_ms, bucket_start + bucket_ms - 1)
                points.append(
                    {
                        "timestamp": _bucket_center_ms(bucket_start, bucket_ms),
                        "start": bucket_start,
                        "end": bucket_end,
                        "count": int(bucket["count"]),
                        "min": bucket["min"],
                        "avg": bucket["sum"] / bucket["count"],
                        "max": bucket["max"],
                    }
                )
            if points:
                numeric_series[series_name] = points
        elif is_string_format_id(format_id):
            buckets: Dict[int, str] = {}
            for ev in events:
                ts = ev.timestamp_ms
                if ts < day_start_ms or ts > day_end_ms or not isinstance(ev.value, str):
                    continue
                bucket_idx = (ts - day_start_ms) // bucket_ms
                buckets[bucket_idx] = ev.value
            points = []
            for bucket_idx in sorted(buckets.keys()):
                bucket_start = day_start_ms + bucket_idx * bucket_ms
                points.append({"timestamp": _bucket_center_ms(bucket_start, bucket_ms), "value": buckets[bucket_idx]})
            if points:
                string_series[series_name] = points

    return DownsampledDayCacheEntry(
        source_signature=(path, cache.mtime_ns, cache.size),
        bucket_ms=bucket_ms,
        day=day,
        numeric_series=numeric_series,
        string_series=string_series,
        series_format_ids=series_format_ids,
        files=[os.path.basename(path)],
    )


def _new_incremental_downsampled_day_cache_entry(path: str, day: datetime.date, bucket_ms: int) -> DownsampledDayCacheEntry:
    cache = get_cached_tsdb_file(path)
    return DownsampledDayCacheEntry(
        source_signature=(path, cache.mtime_ns, cache.size, cache.parsed_offset),
        bucket_ms=bucket_ms,
        day=day,
        numeric_series={},
        string_series={},
        series_format_ids=dict(cache.series_format_ids),
        files=[os.path.basename(path)],
        raw_parsed_offset=0,
        raw_series_counts={},
        pending_numeric={},
        pending_string={},
    )


def _append_numeric_bucket_point(
    entry: DownsampledDayCacheEntry,
    series_name: str,
    bucket_idx: int,
    bucket: Dict[str, Any],
) -> None:
    day_start_ms = _day_start_ms(entry.day)
    bucket_start = day_start_ms + bucket_idx * entry.bucket_ms
    bucket_end = min(day_start_ms + 86_400_000 - 1, bucket_start + entry.bucket_ms - 1)
    entry.numeric_series.setdefault(series_name, []).append(
        {
            "timestamp": _bucket_center_ms(bucket_start, entry.bucket_ms),
            "start": bucket_start,
            "end": bucket_end,
            "count": int(bucket["count"]),
            "min": bucket["min"],
            "avg": bucket["sum"] / bucket["count"],
            "max": bucket["max"],
        }
    )


def _append_string_bucket_point(
    entry: DownsampledDayCacheEntry,
    series_name: str,
    bucket_idx: int,
    value: str,
) -> None:
    day_start_ms = _day_start_ms(entry.day)
    bucket_start = day_start_ms + bucket_idx * entry.bucket_ms
    entry.string_series.setdefault(series_name, []).append(
        {"timestamp": _bucket_center_ms(bucket_start, entry.bucket_ms), "value": value}
    )


def _update_current_day_downsampled_cache_from_original(
    path: str,
    day: datetime.date,
    bucket_ms: int,
    entry: Optional[DownsampledDayCacheEntry],
) -> DownsampledDayCacheEntry:
    cache = get_cached_tsdb_file(path)
    if (
        entry is None
        or entry.bucket_ms != bucket_ms
        or entry.day != day
        or entry.raw_series_counts is None
        or entry.pending_numeric is None
        or entry.pending_string is None
        or cache.parsed_offset < entry.raw_parsed_offset
    ):
        entry = _new_incremental_downsampled_day_cache_entry(path, day, bucket_ms)

    if cache.parsed_offset == entry.raw_parsed_offset and entry.series_format_ids == cache.series_format_ids:
        entry.source_signature = (path, cache.mtime_ns, cache.size, cache.parsed_offset)
        return entry

    entry.series_format_ids = dict(cache.series_format_ids)
    day_start_ms = _day_start_ms(day)
    day_end_ms = day_start_ms + 86_400_000 - 1

    for series_name, format_id in cache.series_format_ids.items():
        events = cache.series_events.get(series_name, [])
        prev_count = entry.raw_series_counts.get(series_name, 0)
        if prev_count > len(events):
            entry = _new_incremental_downsampled_day_cache_entry(path, day, bucket_ms)
            prev_count = 0
        if is_numeric_format_id(format_id):
            series_pending = entry.pending_numeric.setdefault(series_name, {})
            for ev in events[prev_count:]:
                ts = ev.timestamp_ms
                if ts < day_start_ms or ts > day_end_ms:
                    continue
                if not isinstance(ev.value, (int, float)) or isinstance(ev.value, bool):
                    continue
                bucket_idx = (ts - day_start_ms) // bucket_ms
                bucket = series_pending.get(bucket_idx)
                value = float(ev.value)
                if bucket is None:
                    series_pending[bucket_idx] = {"count": 1, "sum": value, "min": value, "max": value}
                else:
                    bucket["count"] += 1
                    bucket["sum"] += value
                    if value < bucket["min"]:
                        bucket["min"] = value
                    if value > bucket["max"]:
                        bucket["max"] = value
        elif is_string_format_id(format_id):
            series_pending = entry.pending_string.setdefault(series_name, {})
            for ev in events[prev_count:]:
                ts = ev.timestamp_ms
                if ts < day_start_ms or ts > day_end_ms or not isinstance(ev.value, str):
                    continue
                bucket_idx = (ts - day_start_ms) // bucket_ms
                series_pending[bucket_idx] = ev.value
        entry.raw_series_counts[series_name] = len(events)

    latest_ts = cache.current_ts
    if latest_ts is not None and latest_ts >= day_start_ms:
        current_bucket_idx = min((latest_ts - day_start_ms) // bucket_ms, (86_400_000 - 1) // bucket_ms)
        complete_before_idx = current_bucket_idx
        for series_name, series_pending in entry.pending_numeric.items():
            for bucket_idx in sorted([idx for idx in series_pending.keys() if idx < complete_before_idx]):
                bucket = series_pending.pop(bucket_idx)
                _append_numeric_bucket_point(entry, series_name, bucket_idx, bucket)
        for series_name, series_pending in entry.pending_string.items():
            for bucket_idx in sorted([idx for idx in series_pending.keys() if idx < complete_before_idx]):
                value = series_pending.pop(bucket_idx)
                _append_string_bucket_point(entry, series_name, bucket_idx, value)

    entry.raw_parsed_offset = cache.parsed_offset
    entry.source_signature = (path, cache.mtime_ns, cache.size, cache.parsed_offset)
    return entry


def _write_downsampled_day_cache(path: str, entry: DownsampledDayCacheEntry) -> None:
    series_order = sorted(entry.series_format_ids.keys())
    numeric_points = {
        name: [(int(p["timestamp"]), p["min"], p["avg"], p["max"]) for p in points]
        for name, points in entry.numeric_series.items()
    }
    string_points = {
        name: [(int(p["timestamp"]), str(p["value"])) for p in points]
        for name, points in entry.string_series.items()
    }
    write_downsampled_timeseries_db(path, entry.bucket_ms, series_order, entry.series_format_ids, numeric_points, string_points)


def _downsampled_points_from_day_cache(
    entry: DownsampledDayCacheEntry,
    series_name: str,
    start_ms: int,
    end_ms: int,
) -> Tuple[bool, List[Dict[str, Any]]]:
    if series_name in entry.numeric_series:
        decimals = decimal_places_from_format_id(entry.series_format_ids.get(series_name))
        points = []
        for p in entry.numeric_series[series_name]:
            if not (start_ms <= int(p["timestamp"]) <= end_ms):
                continue
            points.append(
                {
                    "timestamp": p["timestamp"],
                    "start": p["start"],
                    "end": p["end"],
                    "count": p["count"],
                    "min": round(float(p["min"]), decimals),
                    "avg": round(float(p["avg"]), decimals),
                    "max": round(float(p["max"]), decimals),
                }
            )
        return True, points
    if series_name in entry.string_series:
        points = [p for p in entry.string_series[series_name] if start_ms <= int(p["timestamp"]) <= end_ms]
        return True, points
    return True, []


def _downsampled_points_from_ds_file(
    path: str,
    day: datetime.date,
    bucket_ms: int,
    series_name: str,
    start_ms: int,
    end_ms: int,
) -> Tuple[bool, List[Dict[str, Any]]]:
    events = read_tsdb_events_for_series(path, series_name, start_ms, end_ms)
    if not events:
        return True, []
    day_start_ms = _day_start_ms(day)
    day_end_ms = day_start_ms + 86_400_000 - 1
    decimals = decimal_places_from_format_id(get_series_format_id_in_file(path, series_name))
    points: List[Dict[str, Any]] = []
    for ev in events:
        bucket_idx = max(0, (ev.timestamp_ms - day_start_ms) // bucket_ms)
        bucket_start = day_start_ms + bucket_idx * bucket_ms
        bucket_end = min(day_end_ms, bucket_start + bucket_ms - 1)
        if isinstance(ev.value, dict) and {"min", "avg", "max"} <= set(ev.value.keys()):
            points.append(
                {
                    "timestamp": ev.timestamp_ms,
                    "start": bucket_start,
                    "end": bucket_end,
                    "min": round(float(ev.value["min"]), decimals),
                    "avg": round(float(ev.value["avg"]), decimals),
                    "max": round(float(ev.value["max"]), decimals),
                }
            )
        else:
            points.append({"timestamp": ev.timestamp_ms, "value": ev.value})
    return True, points


def _get_or_build_downsampled_day_points(
    data_dir: str,
    day: datetime.date,
    bucket_ms: int,
    series_name: str,
    start_ms: int,
    end_ms: int,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    original_path = _original_file_for_day(data_dir, day)
    if not os.path.isfile(original_path):
        return [], []

    today = datetime.datetime.now(datetime.timezone.utc).date()
    if day == today:
        key = (original_path, bucket_ms)
        with _DOWNSAMPLE_DAY_CACHE_LOCK:
            entry = _DOWNSAMPLE_DAY_CACHE.get(key)
            entry = _update_current_day_downsampled_cache_from_original(original_path, day, bucket_ms, entry)
            _DOWNSAMPLE_DAY_CACHE[key] = entry
        _downsampled, points = _downsampled_points_from_day_cache(entry, series_name, start_ms, end_ms)
        return entry.files, points

    ds_path = _downsampled_file_for_day(data_dir, day, bucket_ms)
    original_stat = os.stat(original_path)
    if os.path.isfile(ds_path):
        ds_stat = os.stat(ds_path)
        if ds_stat.st_mtime_ns >= original_stat.st_mtime_ns:
            _downsampled, points = _downsampled_points_from_ds_file(ds_path, day, bucket_ms, series_name, start_ms, end_ms)
            return [os.path.basename(ds_path)], points

    entry = _build_downsampled_day_cache_from_original(original_path, day, bucket_ms)
    _write_downsampled_day_cache(ds_path, entry)
    _downsampled, points = _downsampled_points_from_day_cache(entry, series_name, start_ms, end_ms)
    return [os.path.basename(ds_path)], points


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


def _downsample_fixed_numeric_events(
    events: List[Event],
    bucket_ms: int,
    start_ms: int,
    end_ms: int,
    decimal_places: Optional[int] = None,
) -> List[Dict[str, Any]]:
    buckets: Dict[Tuple[datetime.date, int], Dict[str, Any]] = {}
    for ev in events:
        ts = ev.timestamp_ms
        if ts < start_ms or ts > end_ms:
            continue
        if not isinstance(ev.value, (int, float)) or isinstance(ev.value, bool):
            continue
        dt = datetime.datetime.fromtimestamp(ts / 1000.0, tz=datetime.timezone.utc)
        day = dt.date()
        day_start_ms = _day_start_ms(day)
        bucket_idx = (ts - day_start_ms) // bucket_ms
        key = (day, bucket_idx)
        bucket = buckets.get(key)
        value = float(ev.value)
        if bucket is None:
            buckets[key] = {"count": 1, "sum": value, "min": value, "max": value}
        else:
            bucket["count"] += 1
            bucket["sum"] += value
            if value < bucket["min"]:
                bucket["min"] = value
            if value > bucket["max"]:
                bucket["max"] = value

    points: List[Dict[str, Any]] = []
    for (day, bucket_idx) in sorted(buckets.keys()):
        bucket = buckets[(day, bucket_idx)]
        day_start_ms = _day_start_ms(day)
        bucket_start = day_start_ms + bucket_idx * bucket_ms
        bucket_end = min(day_start_ms + 86_400_000 - 1, bucket_start + bucket_ms - 1)
        avg = bucket["sum"] / bucket["count"]
        min_value = round(bucket["min"], decimal_places) if decimal_places is not None else bucket["min"]
        avg_value = round(avg, decimal_places) if decimal_places is not None else avg
        max_value = round(bucket["max"], decimal_places) if decimal_places is not None else bucket["max"]
        points.append(
            {
                "timestamp": _bucket_center_ms(bucket_start, bucket_ms),
                "start": bucket_start,
                "end": bucket_end,
                "count": int(bucket["count"]),
                "min": min_value,
                "avg": avg_value,
                "max": max_value,
            }
        )
    return points


def _point_numeric_stats(point: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(point, dict):
        return None
    if "avg" in point and isinstance(point.get("avg"), (int, float)) and not isinstance(point.get("avg"), bool):
        return {
            "timestamp": int(point.get("timestamp", 0)),
            "start": int(point.get("start", point.get("timestamp", 0))),
            "end": int(point.get("end", point.get("timestamp", 0))),
            "count": int(point.get("count", 1) or 1),
            "min": float(point.get("min", point["avg"])),
            "avg": float(point["avg"]),
            "max": float(point.get("max", point["avg"])),
        }
    if "value" in point and isinstance(point.get("value"), (int, float)) and not isinstance(point.get("value"), bool):
        value = float(point["value"])
        ts = int(point.get("timestamp", 0))
        return {"timestamp": ts, "start": ts, "end": ts, "count": 1, "min": value, "avg": value, "max": value}
    return None


def _apply_numeric_op(op: str, a: float, b: float) -> Optional[float]:
    if op == "+":
        return a + b
    if op == "-":
        return a - b
    if op == "*":
        return a * b
    if op == "/":
        return None if b == 0 else (a / b)
    return None


def _combine_numeric_extrema(op: str, left: Dict[str, Any], right: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    candidates: List[float] = []
    for a in (float(left["min"]), float(left["max"])):
        for b in (float(right["min"]), float(right["max"])):
            out = _apply_numeric_op(op, a, b)
            if out is None or out != out or abs(out) == float("inf"):
                continue
            candidates.append(out)
    if not candidates:
        return None, None
    return min(candidates), max(candidates)


def _combine_numeric_points(
    left_points: List[Dict[str, Any]],
    right_points: List[Dict[str, Any]],
    op: str,
    align_window_ms: int,
    decimal_places: int,
) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    right_stats: List[Dict[str, Any]] = []
    right_ts: List[int] = []
    for point in right_points:
        stats = _point_numeric_stats(point)
        if stats is None:
            continue
        right_stats.append(stats)
        right_ts.append(int(stats["timestamp"]))
    if not right_stats:
        return result
    window = max(0, int(align_window_ms))
    for point in left_points:
        left = _point_numeric_stats(point)
        if left is None:
            continue
        lt = int(left["timestamp"])
        pos = bisect.bisect_left(right_ts, lt)
        best_idx: Optional[int] = None
        best_dist: Optional[int] = None
        for cand in (pos - 1, pos):
            if cand < 0 or cand >= len(right_stats):
                continue
            rt = right_ts[cand]
            dist = abs(rt - lt)
            if dist > window:
                continue
            if best_dist is None or dist < best_dist or (dist == best_dist and rt < right_ts[best_idx]):
                best_idx = cand
                best_dist = dist
        if best_idx is None:
            continue
        right = right_stats[best_idx]
        avg_out = _apply_numeric_op(op, float(left["avg"]), float(right["avg"]))
        if avg_out is None or avg_out != avg_out or abs(avg_out) == float("inf"):
            continue
        min_out, max_out = _combine_numeric_extrema(op, left, right)
        if min_out is None or max_out is None:
            min_out = avg_out
            max_out = avg_out
        result.append(
            {
                "timestamp": lt,
                "start": int(left["start"]),
                "end": int(left["end"]),
                "count": min(int(left["count"]), int(right["count"])),
                "min": round(min_out, decimal_places),
                "avg": round(avg_out, decimal_places),
                "max": round(max_out, decimal_places),
            }
        )
    return result


def _combine_numeric_points_with_constant(
    points: List[Dict[str, Any]],
    constant: float,
    op: str,
    constant_on_left: bool,
    decimal_places: int,
) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for point in points:
        stats = _point_numeric_stats(point)
        if stats is None:
            continue
        a_avg = constant if constant_on_left else float(stats["avg"])
        b_avg = float(stats["avg"]) if constant_on_left else constant
        avg_out = _apply_numeric_op(op, a_avg, b_avg)
        if avg_out is None or avg_out != avg_out or abs(avg_out) == float("inf"):
            continue
        left_stats = {"min": constant, "max": constant} if constant_on_left else stats
        right_stats = stats if constant_on_left else {"min": constant, "max": constant}
        min_out, max_out = _combine_numeric_extrema(op, left_stats, right_stats)
        if min_out is None or max_out is None:
            min_out = avg_out
            max_out = avg_out
        result.append(
            {
                "timestamp": int(stats["timestamp"]),
                "start": int(stats["start"]),
                "end": int(stats["end"]),
                "count": int(stats["count"]),
                "min": round(min_out, decimal_places),
                "avg": round(avg_out, decimal_places),
                "max": round(max_out, decimal_places),
            }
        )
    return result


def _compute_virtual_points_today(points: List[Dict[str, Any]], decimal_places: int) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    day_start_value: Dict[Tuple[int, int, int], float] = {}
    for point in points:
        stats = _point_numeric_stats(point)
        if stats is None:
            continue
        dt = datetime.datetime.fromtimestamp(int(stats["timestamp"]) / 1000.0).astimezone()
        key = (dt.year, dt.month, dt.day)
        start_value = day_start_value.get(key)
        baseline = float(stats["min"])
        if start_value is None:
            day_start_value[key] = baseline
            min_out = avg_out = max_out = 0.0
        else:
            min_out = max(0.0, float(stats["min"]) - start_value)
            avg_out = max(0.0, float(stats["avg"]) - start_value)
            max_out = max(0.0, float(stats["max"]) - start_value)
        result.append(
            {
                "timestamp": int(stats["timestamp"]),
                "start": int(stats["start"]),
                "end": int(stats["end"]),
                "count": int(stats["count"]),
                "min": round(min_out, decimal_places),
                "avg": round(avg_out, decimal_places),
                "max": round(max_out, decimal_places),
            }
        )
    return result


def _compute_virtual_points_yesterday(points: List[Dict[str, Any]], decimal_places: int) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    day_first_value: Dict[Tuple[int, int, int], float] = {}
    for point in points:
        stats = _point_numeric_stats(point)
        if stats is None:
            continue
        dt = datetime.datetime.fromtimestamp(int(stats["timestamp"]) / 1000.0).astimezone()
        key = (dt.year, dt.month, dt.day)
        if key not in day_first_value:
            day_first_value[key] = float(stats["min"])
    for point in points:
        stats = _point_numeric_stats(point)
        if stats is None:
            continue
        dt = datetime.datetime.fromtimestamp(int(stats["timestamp"]) / 1000.0).astimezone()
        key = (dt.year, dt.month, dt.day)
        today_first = day_first_value.get(key)
        prev_day = dt.date() - datetime.timedelta(days=1)
        prev_key = (prev_day.year, prev_day.month, prev_day.day)
        prev_first = day_first_value.get(prev_key)
        if today_first is None or prev_first is None:
            continue
        delta = today_first - prev_first
        if delta != delta or abs(delta) == float("inf"):
            continue
        result.append(
            {
                "timestamp": int(stats["timestamp"]),
                "start": int(stats["start"]),
                "end": int(stats["end"]),
                "count": int(stats["count"]),
                "min": round(delta, decimal_places),
                "avg": round(delta, decimal_places),
                "max": round(delta, decimal_places),
            }
        )
    return result


def _real_series_points_for_virtual(
    data_dir: str,
    series_name: str,
    start_ms: int,
    end_ms: int,
    bucket_ms: int,
) -> Tuple[List[Dict[str, Any]], int, List[str]]:
    files = find_candidate_files(data_dir, start_ms, end_ms)
    max_decimal_places: Optional[int] = None
    if bucket_ms <= 0:
        events: List[Event] = []
        for path in files:
            events.extend(read_tsdb_events_for_series(path, series_name, start_ms, end_ms))
            fmt = get_series_format_id_in_file(path, series_name)
            if is_numeric_format_id(fmt):
                d = decimal_places_from_format_id(fmt)
                max_decimal_places = d if max_decimal_places is None else max(max_decimal_places, d)
        events.sort(key=lambda e: e.timestamp_ms)
        return [{"timestamp": e.timestamp_ms, "value": e.value} for e in events], max_decimal_places if max_decimal_places is not None else 3, [os.path.basename(p) for p in files]

    has_daily_files = bool(files) and all(os.path.basename(path).startswith("data_") for path in files)
    if not has_daily_files:
        events = []
        for path in files:
            events.extend(read_tsdb_events_for_series(path, series_name, start_ms, end_ms))
            fmt = get_series_format_id_in_file(path, series_name)
            if is_numeric_format_id(fmt):
                d = decimal_places_from_format_id(fmt)
                max_decimal_places = d if max_decimal_places is None else max(max_decimal_places, d)
        events.sort(key=lambda e: e.timestamp_ms)
        return (
            _downsample_fixed_numeric_events(events, bucket_ms, start_ms, end_ms, decimal_places=max_decimal_places if max_decimal_places is not None else 3),
            max_decimal_places if max_decimal_places is not None else 3,
            [os.path.basename(p) for p in files],
        )

    points: List[Dict[str, Any]] = []
    files_used: List[str] = []
    for day in day_range_utc(start_ms, end_ms):
        day_files, day_points = _get_or_build_downsampled_day_points(data_dir, day, bucket_ms, series_name, start_ms, end_ms)
        for name in day_files:
            if name not in files_used:
                files_used.append(name)
        points.extend(day_points)
    for path in files:
        fmt = get_series_format_id_in_file(path, series_name)
        if is_numeric_format_id(fmt):
            d = decimal_places_from_format_id(fmt)
            max_decimal_places = d if max_decimal_places is None else max(max_decimal_places, d)
    points.sort(key=lambda p: int(p.get("timestamp", 0)))
    return points, max_decimal_places if max_decimal_places is not None else 3, files_used


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
    if not name or not left or op not in {"+", "-", "*", "/", "today", "yesterday"}:
        return None
    if op not in {"today", "yesterday"} and not right:
        return None
    return VirtualSeriesDef(name=name, left=left, op=op, right=right)


def _normalize_unit_override_def(obj: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(obj, dict):
        return None
    suffix = str(obj.get("suffix", "")).strip().strip("/")
    unit = str(obj.get("unit", "")).strip()
    scale_op = str(obj.get("scaleOp", obj.get("op", "*"))).strip()
    max_mode = str(obj.get("maxMode", "")).strip().lower()
    try:
        scale = float(obj.get("scale", 1))
        decimals = int(obj.get("decimals"))
    except Exception:
        return None
    if not suffix or decimals < 0 or decimals > 6 or scale <= 0 or scale_op not in {"*", "/"}:
        return None
    if max_mode not in {"", "auto", "max", "nomax"}:
        return None
    return {
        "suffix": suffix,
        "unit": unit,
        "scale": scale,
        "scaleOp": scale_op,
        "decimals": decimals,
        "maxMode": "max" if max_mode in {"", "auto"} else max_mode,
    }


def load_virtual_series_config(data_dir: str) -> Tuple[List[VirtualSeriesDef], List[Dict[str, Any]], int]:
    path = _virtual_series_file_path(data_dir)
    if not os.path.isfile(path):
        return [], [], 10000
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    align_window_ms = 10000
    if isinstance(raw, list):
        items = raw
        overrides_raw = []
    elif isinstance(raw, dict):
        items = raw.get("virtualSeries", [])
        overrides_raw = raw.get("unitOverrides", raw.get("decimalOverrides", []))
        try:
            align_window_ms = int(raw.get("alignWindowMs", 10000))
        except Exception:
            align_window_ms = 10000
    else:
        return [], [], 10000
    if align_window_ms < 0:
        align_window_ms = 0
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
    return defs, overrides, align_window_ms


def load_virtual_series_defs(data_dir: str) -> List[VirtualSeriesDef]:
    defs, _overrides, _align = load_virtual_series_config(data_dir)
    return defs


def _virtual_series_name_set(data_dir: str) -> set[str]:
    return {d.name for d in load_virtual_series_defs(data_dir)}


def save_virtual_series_config(data_dir: str, defs: List[VirtualSeriesDef], unit_overrides: List[Dict[str, Any]], align_window_ms: int = 10000) -> None:
    path = _virtual_series_file_path(data_dir)
    tmp = f"{path}.tmp"
    if align_window_ms < 0:
        align_window_ms = 0
    payload = {
        "alignWindowMs": int(align_window_ms),
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
                "maxMode": str(d.get("maxMode", "max")),
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
    _defs, overrides, align_window_ms = load_virtual_series_config(data_dir)
    save_virtual_series_config(data_dir, defs, overrides, align_window_ms)


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
    t0 = time.perf_counter()
    abs_data_dir = os.path.abspath(data_dir)
    cache_key = (abs_data_dir, series_name)
    file_signatures: List[Tuple[Any, ...]] = []
    file_parts: List[Tuple[str, List[Event]]] = []
    max_decimal_places = 3
    files_used: List[str] = []

    for path in _all_tsdb_files(data_dir):
        cache = get_cached_tsdb_file(path)
        part = cache.series_events.get(series_name, [])
        if part:
            base = os.path.basename(path)
            files_used.append(base)
            file_parts.append((base, part))
            file_signatures.append((base, cache.parsed_offset, len(part), part[-1].timestamp_ms))
        fmt = cache.series_format_ids.get(series_name)
        if is_numeric_format_id(fmt):
            max_decimal_places = max(max_decimal_places, decimal_places_from_format_id(fmt))

    new_sig = tuple(file_signatures)
    with _SERIES_ALL_FILES_CACHE_LOCK:
        cached = _SERIES_ALL_FILES_CACHE.get(cache_key)
        if cached is not None and cached.file_signatures == new_sig:
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            trace_reads = getattr(_REQUEST_TRACE, "series_reads", None)
            if isinstance(trace_reads, list):
                trace_reads.append(
                    {
                        "series": series_name,
                        "cacheHit": True,
                        "elapsedMs": elapsed_ms,
                        "files": len(files_used),
                        "points": len(cached.events),
                        "prefixFiles": len(new_sig),
                    }
                )
            return list(cached.events), cached.decimal_places, list(cached.files), new_sig

    events: List[Event]
    prefix_files = 0
    if cached is not None:
        max_prefix = min(len(cached.file_signatures), len(file_parts))
        for i in range(max_prefix):
            if cached.file_signatures[i] != new_sig[i]:
                break
            prefix_files += 1
        if prefix_files > 0:
            prefix_event_count = sum(int(sig[2]) for sig in cached.file_signatures[:prefix_files])
            events = list(cached.events[:prefix_event_count])
            for _base, part in file_parts[prefix_files:]:
                events.extend(part)
        else:
            events = []
            for _base, part in file_parts:
                events.extend(part)
    else:
        events = []
        for _base, part in file_parts:
            events.extend(part)

    with _SERIES_ALL_FILES_CACHE_LOCK:
        _SERIES_ALL_FILES_CACHE[cache_key] = SeriesAllFilesCacheEntry(
            file_signatures=new_sig,
            events=list(events),
            decimal_places=max_decimal_places,
            files=list(files_used),
        )
    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    trace_reads = getattr(_REQUEST_TRACE, "series_reads", None)
    if isinstance(trace_reads, list):
        trace_reads.append(
            {
                "series": series_name,
                "cacheHit": False,
                "elapsedMs": elapsed_ms,
                "files": len(files_used),
                "points": len(events),
                "prefixFiles": prefix_files,
            }
        )
    return events, max_decimal_places, files_used, new_sig


def _compute_virtual_events(left_events: List[Event], right_events: List[Event], op: str, align_window_ms: int = 0) -> List[Event]:
    result: List[Event] = []
    window = max(0, int(align_window_ms))
    right_numeric: List[Tuple[int, float]] = []
    for ev in right_events:
        rv = ev.value
        if isinstance(rv, (int, float)) and not isinstance(rv, bool):
            right_numeric.append((ev.timestamp_ms, float(rv)))
    if not right_numeric:
        return result
    right_ts = [t for t, _v in right_numeric]
    for lev in left_events:
        lv = lev.value
        if not isinstance(lv, (int, float)) or isinstance(lv, bool):
            continue
        lt = lev.timestamp_ms
        pos = bisect.bisect_left(right_ts, lt)
        best_idx: Optional[int] = None
        best_dist: Optional[int] = None
        for cand in (pos - 1, pos):
            if cand < 0 or cand >= len(right_numeric):
                continue
            rt = right_numeric[cand][0]
            dist = abs(rt - lt)
            if dist > window:
                continue
            if best_dist is None or dist < best_dist or (dist == best_dist and rt < right_numeric[best_idx][0]):  # prefer earlier on tie
                best_idx = cand
                best_dist = dist
        if best_idx is None:
            continue
        a = float(lv)
        b = right_numeric[best_idx][1]
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


def _compute_virtual_events_yesterday(events: List[Event]) -> List[Event]:
    result: List[Event] = []
    day_first_value: Dict[Tuple[int, int, int], float] = {}
    # First pass: capture first numeric value per local day.
    for ev in events:
        v = ev.value
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            continue
        dt = datetime.datetime.fromtimestamp(ev.timestamp_ms / 1000.0).astimezone()
        key = (dt.year, dt.month, dt.day)
        if key not in day_first_value:
            day_first_value[key] = float(v)
    # Second pass: emit previous-day delta at timestamps of current day.
    for ev in events:
        v = ev.value
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            continue
        dt = datetime.datetime.fromtimestamp(ev.timestamp_ms / 1000.0).astimezone()
        key = (dt.year, dt.month, dt.day)
        today_first = day_first_value.get(key)
        prev_day = (dt.date() - datetime.timedelta(days=1))
        prev_key = (prev_day.year, prev_day.month, prev_day.day)
        prev_first = day_first_value.get(prev_key)
        if today_first is None or prev_first is None:
            continue
        out = today_first - prev_first
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
    if op in {"today", "yesterday"}:
        return left_dp
    if op in {"+", "-"}:
        return max(left_dp, right_dp)
    if op == "*":
        return min(6, max(3, left_dp + right_dp))
    if op == "/":
        return max(3, left_dp)
    return 3


def _virtual_result_signature(entry: VirtualSeriesCacheEntry) -> Tuple[Any, ...]:
    return ("virtual", entry.definition, entry.left_sig, entry.right_sig)


def _resolve_virtual_operand(
    data_dir: str,
    operand: str,
    prior_virtuals: Dict[str, Tuple[List[Event], int, List[str], Tuple[Any, ...]]],
) -> Tuple[bool, Optional[float], List[Event], int, List[str], Tuple[Any, ...]]:
    const = _parse_virtual_constant(operand)
    if const is not None:
        return True, float(const), [], 3, [], ("const", const)
    prior = prior_virtuals.get(str(operand))
    if prior is not None:
        events, dp, files, sig = prior
        return False, None, list(events), int(dp), list(files), sig
    events, dp, files, sig = _read_series_all_files(data_dir, operand)
    return False, None, events, dp, files, sig


def _resolve_virtual_operand_points(
    data_dir: str,
    operand: str,
    prior_virtuals: Dict[str, Tuple[List[Dict[str, Any]], int, List[str]]],
    start_ms: int,
    end_ms: int,
    bucket_ms: int,
) -> Tuple[bool, Optional[float], List[Dict[str, Any]], int, List[str]]:
    const = _parse_virtual_constant(operand)
    if const is not None:
        return True, float(const), [], 3, []
    prior = prior_virtuals.get(str(operand))
    if prior is not None:
        points, dp, files = prior
        return False, None, list(points), int(dp), list(files)
    points, dp, files = _real_series_points_for_virtual(data_dir, operand, start_ms, end_ms, bucket_ms)
    return False, None, points, dp, files


def _compute_one_virtual_series_points(
    data_dir: str,
    d: VirtualSeriesDef,
    prior_virtuals: Dict[str, Tuple[List[Dict[str, Any]], int, List[str]]],
    start_ms: int,
    end_ms: int,
    bucket_ms: int,
    align_window_ms: int,
) -> Tuple[List[Dict[str, Any]], int, List[str]]:
    operand_start_ms = start_ms
    operand_end_ms = end_ms
    if d.op in {"today", "yesterday"}:
        start_day = datetime.datetime.fromtimestamp(start_ms / 1000.0, tz=datetime.timezone.utc).date()
        end_day = datetime.datetime.fromtimestamp(end_ms / 1000.0, tz=datetime.timezone.utc).date()
        if d.op == "yesterday":
            start_day = start_day - datetime.timedelta(days=1)
        operand_start_ms = _day_start_ms(start_day)
        operand_end_ms = _day_start_ms(end_day) + 86_400_000 - 1
    left_is_const, left_const, left_points, left_dp, left_files = _resolve_virtual_operand_points(
        data_dir, d.left, prior_virtuals, operand_start_ms, operand_end_ms, bucket_ms
    )
    if d.op in {"today", "yesterday"}:
        if left_is_const:
            return [], 3, []
        decimal_places = _virtual_decimal_places(d.op, left_dp, 0)
        points = _compute_virtual_points_today(left_points, decimal_places) if d.op == "today" else _compute_virtual_points_yesterday(left_points, decimal_places)
        points = [p for p in points if start_ms <= int(p.get("timestamp", 0)) <= end_ms]
        return points, decimal_places, sorted(set(left_files))

    right_is_const, right_const, right_points, right_dp, right_files = _resolve_virtual_operand_points(
        data_dir, d.right, prior_virtuals, start_ms, end_ms, bucket_ms
    )
    decimal_places = _virtual_decimal_places(d.op, left_dp, right_dp)
    if left_is_const and right_is_const:
        return [], 3, []
    if left_is_const:
        return _combine_numeric_points_with_constant(right_points, float(left_const), d.op, True, decimal_places), decimal_places, sorted(set(right_files))
    if right_is_const:
        return _combine_numeric_points_with_constant(left_points, float(right_const), d.op, False, decimal_places), decimal_places, sorted(set(left_files))
    return (
        _combine_numeric_points(left_points, right_points, d.op, align_window_ms, decimal_places),
        decimal_places,
        sorted(set(left_files + right_files)),
    )


def _compute_one_virtual_series_cached(
    data_dir: str,
    d: VirtualSeriesDef,
    prior_virtuals: Dict[str, Tuple[List[Event], int, List[str], Tuple[Any, ...]]],
    align_window_ms: int,
) -> Tuple[List[Event], int, List[str], Tuple[Any, ...]]:
    cache_key = (os.path.abspath(data_dir), d.name)
    definition = (d.name, d.left, d.op, d.right, int(align_window_ms))

    left_is_const, left_const, left_events, left_dp, left_files, left_sig = _resolve_virtual_operand(data_dir, d.left, prior_virtuals)
    if d.op in {"today", "yesterday"}:
        right_sig = (d.op,)
        with _VIRTUAL_SERIES_CACHE_LOCK:
            entry = _VIRTUAL_SERIES_RESULT_CACHE.get(cache_key)
            if (
                entry is not None
                and entry.definition == definition
                and entry.left_sig == left_sig
                and entry.right_sig == right_sig
            ):
                return list(entry.events), entry.decimal_places, list(entry.files), _virtual_result_signature(entry)
        if left_is_const:
            events = []
            decimal_places = 3
            files: List[str] = []
        else:
            events = _compute_virtual_events_today(left_events) if d.op == "today" else _compute_virtual_events_yesterday(left_events)
            decimal_places = _virtual_decimal_places(d.op, left_dp, 0)
            files = sorted(set(left_files))
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
        return list(events), decimal_places, list(files), _virtual_result_signature(cache_entry)

    right_is_const, right_const, right_events, right_dp, right_files, right_sig = _resolve_virtual_operand(data_dir, d.right, prior_virtuals)
    if left_is_const and right_is_const:
        with _VIRTUAL_SERIES_CACHE_LOCK:
            entry = _VIRTUAL_SERIES_RESULT_CACHE.get(cache_key)
            if (
                entry is not None
                and entry.definition == definition
                and entry.left_sig == left_sig
                and entry.right_sig == right_sig
            ):
                return list(entry.events), entry.decimal_places, list(entry.files), _virtual_result_signature(entry)
        cache_entry = VirtualSeriesCacheEntry(
            definition=definition,
            left_sig=left_sig,
            right_sig=right_sig,
            events=[],
            decimal_places=3,
            files=[],
        )
        with _VIRTUAL_SERIES_CACHE_LOCK:
            _VIRTUAL_SERIES_RESULT_CACHE[cache_key] = cache_entry
        return [], 3, [], _virtual_result_signature(cache_entry)

    with _VIRTUAL_SERIES_CACHE_LOCK:
        entry = _VIRTUAL_SERIES_RESULT_CACHE.get(cache_key)
        if (
            entry is not None
            and entry.definition == definition
            and entry.left_sig == left_sig
            and entry.right_sig == right_sig
        ):
            return list(entry.events), entry.decimal_places, list(entry.files), _virtual_result_signature(entry)

    if left_is_const:
        events = _compute_virtual_events_with_constant(right_events, float(left_const), d.op, True)
    elif right_is_const:
        events = _compute_virtual_events_with_constant(left_events, float(right_const), d.op, False)
    else:
        events = _compute_virtual_events(left_events, right_events, d.op, align_window_ms)
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
    return list(events), decimal_places, list(files), _virtual_result_signature(cache_entry)


def get_virtual_series_events_cached(data_dir: str, series_name: str) -> Optional[Tuple[List[Event], int, List[str]]]:
    defs, _overrides, align_window_ms = load_virtual_series_config(data_dir)
    target_idx: Optional[int] = None
    for i, d in enumerate(defs):
        if d.name == series_name:
            target_idx = i
            break
    if target_idx is None:
        return None

    prior_virtuals: Dict[str, Tuple[List[Event], int, List[str], Tuple[Any, ...]]] = {}
    for d in defs[:target_idx + 1]:
        events, decimal_places, files, sig = _compute_one_virtual_series_cached(data_dir, d, prior_virtuals, align_window_ms)
        prior_virtuals[d.name] = (events, decimal_places, files, sig)
    events, decimal_places, files, _sig = prior_virtuals[series_name]
    return list(events), int(decimal_places), list(files)


def get_virtual_series_points(
    data_dir: str,
    series_name: str,
    start_ms: int,
    end_ms: int,
    bucket_ms: int,
) -> Optional[Tuple[List[Dict[str, Any]], int, List[str]]]:
    defs, _overrides, align_window_ms = load_virtual_series_config(data_dir)
    target_idx: Optional[int] = None
    for i, d in enumerate(defs):
        if d.name == series_name:
            target_idx = i
            break
    if target_idx is None:
        return None
    prior_virtuals: Dict[str, Tuple[List[Dict[str, Any]], int, List[str]]] = {}
    for d in defs[:target_idx + 1]:
        points, decimal_places, files = _compute_one_virtual_series_points(
            data_dir,
            d,
            prior_virtuals,
            start_ms,
            end_ms,
            bucket_ms,
            align_window_ms,
        )
        prior_virtuals[d.name] = (points, decimal_places, files)
    points, decimal_places, files = prior_virtuals[series_name]
    return list(points), int(decimal_places), list(files)


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
        virtual_bucket_ms = 0 if (end_ms - start_ms + 1) < _DOWNSAMPLE_BUCKETS[0][0] else _choose_fixed_bucket_ms(start_ms, end_ms, max_events)
        virtual = get_virtual_series_points(data_dir, series, start_ms, end_ms, virtual_bucket_ms) if virtual_bucket_ms > 0 else None
        if virtual is None:
            virtual = get_virtual_series_events_cached(data_dir, series)
        if virtual is not None:
            if virtual_bucket_ms > 0 and isinstance(virtual[0], list) and (not virtual[0] or isinstance(virtual[0][0], dict)):
                points = list(virtual[0])
                max_decimal_places = int(virtual[1])
                files_used = list(virtual[2])
                downsampled = True
                bucket_ms = virtual_bucket_ms
                response: Dict[str, Any] = {
                    "series": series,
                    "start": start_ms,
                    "end": end_ms,
                    "requestedMaxEvents": max_events,
                    "returnedPoints": len(points),
                    "downsampled": downsampled,
                    "files": files_used,
                    "points": points,
                }
                if max_decimal_places is not None:
                    response["decimalPlaces"] = max_decimal_places
                response["bucketMs"] = bucket_ms
                return response
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
        files_used = [os.path.basename(p) for p in files]

        if len(events) <= max_events:
            downsampled = False
            points = [{"timestamp": e.timestamp_ms, "value": e.value} for e in events]
        else:
            bucket_ms = _choose_fixed_bucket_ms(start_ms, end_ms, max_events)
            if virtual is not None:
                if all_numeric:
                    points = _downsample_fixed_numeric_events(
                        events,
                        bucket_ms,
                        start_ms,
                        end_ms,
                        decimal_places=max_decimal_places if max_decimal_places is not None else 3,
                    )
                else:
                    points = [{"timestamp": e.timestamp_ms, "value": e.value} for e in events[:max_events]]
                    bucket_ms = 0
                downsampled = bucket_ms > 0
            else:
                has_daily_files = bool(files) and all(os.path.basename(path).startswith("data_") for path in files)
                if not has_daily_files:
                    if all_numeric:
                        points = _downsample_fixed_numeric_events(
                            events,
                            bucket_ms,
                            start_ms,
                            end_ms,
                            decimal_places=max_decimal_places if max_decimal_places is not None else 3,
                        )
                    else:
                        points = [{"timestamp": e.timestamp_ms, "value": e.value} for e in events[:max_events]]
                        bucket_ms = 0
                    downsampled = bucket_ms > 0
                else:
                    downsampled = True
                    points = []
                    files_used = []
                    for day in day_range_utc(start_ms, end_ms):
                        day_files, day_points = _get_or_build_downsampled_day_points(
                            data_dir,
                            day,
                            bucket_ms,
                            series,
                            start_ms,
                            end_ms,
                        )
                        if day_files:
                            for name in day_files:
                                if name not in files_used:
                                    files_used.append(name)
                        points.extend(day_points)

        response: Dict[str, Any] = {
            "series": series,
            "start": start_ms,
            "end": end_ms,
            "requestedMaxEvents": max_events,
            "returnedPoints": len(points),
            "downsampled": downsampled,
            "files": files_used,
            "points": points,
        }
        if max_decimal_places is not None:
            response["decimalPlaces"] = max_decimal_places
        if downsampled:
            response["bucketMs"] = bucket_ms
        if not all_numeric and len(events) > max_events and virtual is not None:
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
        virtual_names = _virtual_series_name_set(data_dir)
        if len(series_values) == 1:
            self._send_json(200, self._stats_for_series(data_dir, series_values[0], start_ms, end_ms, virtual_names))
            return
        stats_list = [self._stats_for_series(data_dir, s, start_ms, end_ms, virtual_names) for s in series_values]
        self._send_json(
            200,
            {
                "start": start_ms,
                "end": end_ms,
                "requestedSeries": series_values,
                "stats": stats_list,
            },
        )

    def _stats_for_series(
        self,
        data_dir: str,
        series: str,
        start_ms: int,
        end_ms: int,
        virtual_names: Optional[set[str]] = None,
    ) -> Dict[str, Any]:
        t0 = time.perf_counter()
        _REQUEST_TRACE.series_reads = []
        is_virtual = series in (virtual_names if virtual_names is not None else _virtual_series_name_set(data_dir))
        event_data = self._events_for_series(data_dir, series, start_ms, end_ms, STATS_MAX_EVENTS)
        points = list(event_data.get("points") or [])
        downsampled = bool(event_data.get("downsampled"))
        current_ts: Optional[int] = None
        current_value: Any = None
        max_value: Optional[float] = None
        numeric_count = 0

        if downsampled:
            for point in points:
                if not isinstance(point, dict):
                    continue
                current_ts = point.get("timestamp")
                if "avg" in point:
                    current_value = point.get("avg")
                else:
                    current_value = point.get("value")
                if "max" in point and isinstance(point.get("max"), (int, float)) and not isinstance(point.get("max"), bool):
                    numeric_count += 1
                    v = float(point["max"])
                    if max_value is None or v > max_value:
                        max_value = v
                elif isinstance(point.get("value"), (int, float)) and not isinstance(point.get("value"), bool):
                    numeric_count += 1
                    v = float(point["value"])
                    if max_value is None or v > max_value:
                        max_value = v
        else:
            for point in points:
                if not isinstance(point, dict):
                    continue
                current_ts = point.get("timestamp")
                current_value = point.get("value")
                if isinstance(current_value, (int, float)) and not isinstance(current_value, bool):
                    numeric_count += 1
                    v = float(current_value)
                    if max_value is None or v > max_value:
                        max_value = v

        count = len(points)
        response = {
            "series": series,
            "start": start_ms,
            "end": end_ms,
            "count": count,
            "numericCount": numeric_count,
            "isNumeric": numeric_count == count and count > 0,
            "currentTimestamp": current_ts,
            "currentValue": current_value,
            "maxValue": max_value,
            "decimalPlaces": event_data.get("decimalPlaces"),
            "files": list(event_data.get("files") or []),
            "downsampled": downsampled,
            "bucketMs": event_data.get("bucketMs"),
        }
        trace_reads = getattr(_REQUEST_TRACE, "series_reads", [])
        cache_hits = 0
        cache_misses = 0
        if isinstance(trace_reads, list):
            for item in trace_reads:
                if item.get("cacheHit"):
                    cache_hits += 1
                else:
                    cache_misses += 1
        source_cache = "n/a"
        if trace_reads:
            source_cache = "hit" if cache_misses == 0 else ("miss" if cache_hits == 0 else f"mixed({cache_hits}/{cache_misses})")
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        response["debug"] = {
            "virtual": is_virtual,
            "sourceCache": source_cache,
            "elapsedMs": elapsed_ms,
            "seriesReads": trace_reads if isinstance(trace_reads, list) else [],
        }
        _REQUEST_TRACE.series_reads = []
        return response

    def _handle_virtual_series_get(self) -> None:
        data_dir = self.server.data_dir  # type: ignore[attr-defined]
        defs, overrides, align_window_ms = load_virtual_series_config(data_dir)
        self._send_json(
            200,
            {
                "alignWindowMs": int(align_window_ms),
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
        align_window_raw = payload.get("alignWindowMs", 10000)
        if not isinstance(items, list):
            raise ValueError("virtualSeries payload must be a list")
        if not isinstance(overrides_raw, list):
            raise ValueError("unitOverrides payload must be a list")
        try:
            align_window_ms = int(align_window_raw)
        except Exception:
            raise ValueError("alignWindowMs must be an integer")
        if align_window_ms < 0:
            raise ValueError("alignWindowMs must be >= 0")
        defs: List[VirtualSeriesDef] = []
        seen: set[str] = set()
        for item in items:
            d = _normalize_virtual_series_def(item)
            if d is None:
                raise ValueError("Each virtual series must include name,left,op and valid operator (+ - * / today yesterday); right is optional for today/yesterday")
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
        save_virtual_series_config(data_dir, defs, overrides, align_window_ms)
        self._send_json(200, {"ok": True, "count": len(defs), "unitOverrideCount": len(overrides), "alignWindowMs": align_window_ms})

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
