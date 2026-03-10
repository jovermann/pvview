#!/usr/bin/env python3
import argparse
import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import math
import mimetypes
import os
import sys
import tempfile
import threading
import time
from typing import Any, Optional
import json
import fnmatch
import re
import queue as queue_mod
from urllib.parse import unquote, urlparse
import urllib.request

from tsdb import (
    NumericWithDecimals,
    TimeSeriesDbAppender,
    compress_timeseries_db_file,
    create_timeseries_db_writer,
    decimal_places_from_format_id,
    dump_timeseries_db_bytes,
    downsample_series_points,
    read_timeseries_db,
    stat_timeseries_db,
    write_series_array_timeseries_db,
)

COLLECTOR_UI_API_VERSION = 1
def _tsdb_filename_for_utc_day(day: datetime.date) -> str:
    return f"data_{day.isoformat()}.tsdb"


def _value_from_mqtt_payload(payload: bytes) -> Any:
    try:
        text = payload.decode("utf-8-sig").strip()
    except Exception:
        return f"hex:{payload.hex()}"
    numeric = _parse_strict_float(text)
    if numeric is not None:
        mantissa = text
        exp = 0
        if "e" in mantissa.lower():
            parts = mantissa.lower().split("e", 1)
            mantissa = parts[0]
            try:
                exp = int(parts[1])
            except Exception:
                exp = 0
        decimals = 0
        if "." in mantissa:
            decimals = len(mantissa.split(".", 1)[1])
        effective_decimals = max(0, decimals - exp)
        return NumericWithDecimals(float(numeric), max(0, effective_decimals))
    return text


def _quantize_timestamp_ms(timestamp_ms: int, quantize_timestamps_ms: int) -> int:
    if quantize_timestamps_ms <= 0:
        return timestamp_ms
    return (timestamp_ms // quantize_timestamps_ms) * quantize_timestamps_ms


def collect_to_tsdb(
    server: Optional[str],
    subscriptions: list[str],
    verbose: int,
    quantize_timestamps_ms: int = 0,
    data_dir: str = ".",
    http_config: Optional[dict[str, Any]] = None,
) -> int:
    http_cfg = http_config if isinstance(http_config, dict) else {}
    http_urls_raw = http_cfg.get("urls", [])
    http_urls = [u for u in http_urls_raw if isinstance(u, dict) and str(u.get("url", "")).strip()]
    http_base_url = str(http_cfg.get("base_url", "")).strip()
    try:
        http_poll_interval_ms = max(100, int(http_cfg.get("poll_interval_ms", 5000)))
    except Exception:
        http_poll_interval_ms = 5000

    if not subscriptions and not http_urls:
        print("--collect requires at least one topic via --topics or config")
        return 2

    mqtt = None
    client = None
    host = ""
    port = 0
    if subscriptions:
        if not server:
            print("MQTT server not specified. Use --mqtt-server or set mqtt_server in config.")
            return 2
        try:
            import paho.mqtt.client as mqtt  # type: ignore
        except Exception:
            print("paho-mqtt is required. Install with: pip install paho-mqtt")
            return 2
        host, port = parse_host_port(server)
        if not hasattr(mqtt, "CallbackAPIVersion"):
            print("paho-mqtt v2 is required. Please upgrade paho-mqtt.")
            return 2

    pending_events: list[tuple[int, str, Any]] = []
    lock = threading.Lock()
    appenders: dict[datetime.date, TimeSeriesDbAppender] = {}
    os.makedirs(data_dir, exist_ok=True)
    downsample_queue: "queue_mod.Queue[Optional[tuple[datetime.date, str]]]" = queue_mod.Queue()
    downsample_pending: set[datetime.date] = set()
    downsample_lock = threading.Lock()

    def schedule_downsample(day: datetime.date) -> None:
        path = os.path.join(data_dir, _tsdb_filename_for_utc_day(day))
        if not os.path.isfile(path):
            return
        with downsample_lock:
            if day in downsample_pending:
                return
            downsample_pending.add(day)
        downsample_queue.put((day, path))

    def downsample_worker() -> None:
        while True:
            job = downsample_queue.get()
            try:
                if job is None:
                    return
                day, path = job
                if verbose:
                    print(f"downsample worker start: {path}")
                try:
                    _downsample_file(path)
                    if verbose:
                        print(f"downsample worker done: {path}")
                except Exception as exc:
                    print(f"downsample worker failed for {path!r}: {exc}")
                finally:
                    with downsample_lock:
                        downsample_pending.discard(day)
            finally:
                downsample_queue.task_done()

    worker = threading.Thread(target=downsample_worker, name="tsdb-downsample", daemon=True)
    worker.start()

    if mqtt is not None:
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
            ts_ms = _quantize_timestamp_ms(int(time.time() * 1000), quantize_timestamps_ms)
            value = _value_from_mqtt_payload(msg.payload)
            value_for_log = value.value if isinstance(value, NumericWithDecimals) else value
            if verbose >= 2:
                print(f"received: {msg.topic}={value_for_log}")
            with lock:
                pending_events.append((ts_ms, msg.topic, value))

        client.on_connect = on_connect
        client.on_message = on_message
        try:
            client.connect(host, port, keepalive=30)
        except Exception as exc:
            print(f"Unable to connect to MQTT server {host}:{port}: {exc}")
            return 2

    def flush_batch(batch: list[tuple[int, str, Any]]) -> None:
        if batch:
            # MQTT callbacks and HTTP poll completion can race when enqueuing; sort to preserve timestamp order.
            batch.sort(key=lambda item: int(item[0]))
        by_day: dict[datetime.date, list[tuple[int, str, Any]]] = {}
        for ts_ms, topic, value in batch:
            day = datetime.datetime.fromtimestamp(ts_ms / 1000.0, tz=datetime.timezone.utc).date()
            by_day.setdefault(day, []).append((ts_ms, topic, value))
        for day, day_events in by_day.items():
            if day not in appenders:
                path = os.path.join(data_dir, _tsdb_filename_for_utc_day(day))
                if verbose:
                    action = "opening existing" if os.path.exists(path) else "creating new"
                    print(f"{action} TSDB file: {path}")
                appenders[day] = TimeSeriesDbAppender(path)
            appenders[day].append_events(day_events)
        if appenders:
            newest_day = max(appenders.keys())
            for day in list(appenders.keys()):
                if day < newest_day:
                    schedule_downsample(day)
        if verbose and batch:
            print(f"flushed {len(batch)} events")

    if client is not None:
        client.loop_start()
    last_flush = time.monotonic()
    next_http_poll = time.monotonic()
    try:
        while True:
            now = time.monotonic()
            if http_urls and now >= next_http_poll:
                http_pending_values: list[tuple[str, Any]] = []
                emitted = 0
                for url_cfg in http_urls:
                    url_raw = str(url_cfg.get("url", "")).strip()
                    url = resolve_http_url(url_raw, http_base_url)
                    base_topic = str(url_cfg.get("base_topic", "")).strip().strip("/")
                    if not url:
                        continue
                    flat, error = fetch_http_json_flattened(url)
                    if error:
                        if verbose:
                            print(f"http fetch failed: {url} ({error})")
                        continue
                    if flat is None:
                        if verbose:
                            print(f"http fetch returned non-object JSON: {url}")
                        continue
                    values_raw = url_cfg.get("values", [])
                    values = values_raw if isinstance(values_raw, list) else []
                    for value_cfg in values:
                        if not isinstance(value_cfg, dict):
                            continue
                        path = str(value_cfg.get("path", "")).strip()
                        if not path:
                            continue
                        if path not in flat:
                            continue
                        topic_leaf = str(value_cfg.get("topic", "")).strip()
                        if not topic_leaf:
                            continue
                        full_topic = f"{base_topic}/{topic_leaf}" if base_topic else topic_leaf
                        http_pending_values.append((full_topic, _value_from_http_text(flat[path])))
                        emitted += 1
                    if verbose >= 2:
                        print(f"http fetched: {url} keys={len(flat)}")
                if http_pending_values:
                    ts_ms = _quantize_timestamp_ms(int(time.time() * 1000), quantize_timestamps_ms)
                    http_events = [(ts_ms, topic, value) for topic, value in http_pending_values]
                    with lock:
                        pending_events.extend(http_events)
                if verbose and http_urls:
                    print(f"http poll done urls={len(http_urls)} emitted={emitted}")
                next_http_poll = now + (http_poll_interval_ms / 1000.0)

            if now - last_flush >= 10.0:
                with lock:
                    batch = list(pending_events)
                    pending_events.clear()
                flush_batch(batch)
                last_flush = now
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        with lock:
            batch = list(pending_events)
            pending_events.clear()
        flush_batch(batch)
        if appenders:
            newest_day = max(appenders.keys())
            for day in list(appenders.keys()):
                if day < newest_day:
                    schedule_downsample(day)
        downsample_queue.join()
        downsample_queue.put(None)
        downsample_queue.join()
        worker.join(timeout=1.0)
        if client is not None:
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


def _print_tsdb_file_stats(path: str) -> None:
    stats = stat_timeseries_db(path)

    format_header = ("format", "name", "values", "value bytes", "total bytes")
    format_rows = [
        (f"0x{row.format_id:02x}", row.format_name, str(row.count), row.value_size_text, str(row.total_value_bytes))
        for row in stats.per_format
    ]
    format_widths = [
        max(len(format_header[i]), *(len(row[i]) for row in format_rows)) if format_rows else len(format_header[i])
        for i in range(5)
    ]
    print("Per-format value bytes:")
    print(
        f"{format_header[0]:<{format_widths[0]}}  "
        f"{format_header[1]:<{format_widths[1]}}  "
        f"{format_header[2]:>{format_widths[2]}}  "
        f"{format_header[3]:>{format_widths[3]}}  "
        f"{format_header[4]:>{format_widths[4]}}"
    )
    for row in format_rows:
        print(
            f"{row[0]:<{format_widths[0]}}  "
            f"{row[1]:<{format_widths[1]}}  "
            f"{row[2]:>{format_widths[2]}}  "
            f"{row[3]:>{format_widths[3]}}  "
            f"{row[4]:>{format_widths[4]}}"
        )

    total = max(1, stats.total_bytes)
    summary_rows = [
        ("values", stats.value_count, stats.value_bytes, 100.0 * stats.value_bytes / total),
        ("timestamps", stats.timestamp_count, stats.timestamp_bytes, 100.0 * stats.timestamp_bytes / total),
        ("channel definitions", stats.channel_definition_count, stats.channel_definition_bytes, 100.0 * stats.channel_definition_bytes / total),
        ("other", stats.other_count, stats.other_bytes, 100.0 * stats.other_bytes / total),
        ("total", stats.value_count + stats.timestamp_count + stats.channel_definition_count + stats.other_count, stats.total_bytes, 100.0),
    ]
    summary_header = ("category", "number of", "bytes", "percent")
    summary_widths = [
        max(len(summary_header[0]), *(len(row[0]) for row in summary_rows)),
        max(len(summary_header[1]), *(len(str(row[1])) for row in summary_rows)),
        max(len(summary_header[2]), *(len(str(row[2])) for row in summary_rows)),
        max(len(summary_header[3]), *(len(f"{row[3]:.2f}%") for row in summary_rows)),
    ]
    print()
    print("Overall byte usage:")
    print(
        f"{summary_header[0]:<{summary_widths[0]}}  "
        f"{summary_header[1]:>{summary_widths[1]}}  "
        f"{summary_header[2]:>{summary_widths[2]}}  "
        f"{summary_header[3]:>{summary_widths[3]}}"
    )
    for label, count, byte_count, pct in summary_rows:
        print(
            f"{label:<{summary_widths[0]}}  "
            f"{count:>{summary_widths[1]}}  "
            f"{byte_count:>{summary_widths[2]}}  "
            f"{pct:>{summary_widths[3]}.2f}%"
        )


_DOWNSAMPLE_LEVELS: list[tuple[int, str, int]] = [
    (1000, "1s", 1),
    (5000, "5s", 3),
    (15000, "15s", 3),
    (60000, "1m", 3),
    (300000, "5m", 3),
    (900000, "15m", 3),
    (3600000, "1h", 3),
]


def _parse_downsample_input_path(path: str) -> tuple[datetime.date, int]:
    base = os.path.basename(path)
    if base.startswith("data_") and base.endswith(".tsdb"):
        return datetime.date.fromisoformat(base[5:15]), 0
    m = re.fullmatch(r"dsda_(\d{4}-\d{2}-\d{2})\.(1s|5s|15s|1m|5m|15m|1h)\.tsdb", base)
    if m:
        day = datetime.date.fromisoformat(m.group(1))
        label = m.group(2)
        for bucket_ms, bucket_label, _elem_size in _DOWNSAMPLE_LEVELS:
            if bucket_label == label:
                return day, bucket_ms
    raise ValueError(f"Unsupported downsample input file name: {path!r}")


def _downsample_output_path(input_path: str, day: datetime.date, label: str) -> str:
    return os.path.join(os.path.dirname(input_path), f"dsda_{day.isoformat()}.{label}.tsdb")


def _numeric_series_points_from_db(db: Any) -> dict[str, list[tuple[int, Any]]]:
    return {
        series_name: [
            (ts_ms, value)
            for ts_ms, value in db.get_series_values(series_name)
            if isinstance(value, dict) or (isinstance(value, (int, float)) and not isinstance(value, bool))
        ]
        for series_name in db.list_series()
    }


def _write_series_array_timeseries_db_atomic(
    output_path: str,
    day: datetime.date,
    bucket_ms: int,
    series_names: list[str],
    series_decimals: dict[str, int],
    points_by_series: dict[str, list[tuple[int, Any]]],
    elem_size: int,
) -> None:
    tmp_path = f"{output_path}.tmp"
    write_series_array_timeseries_db(
        tmp_path,
        day,
        bucket_ms,
        series_names,
        series_decimals,
        points_by_series,
        elem_size,
    )
    os.replace(tmp_path, output_path)


def _downsample_file(path: str) -> None:
    source_path = os.path.abspath(path)
    day, source_bucket_ms = _parse_downsample_input_path(source_path)
    db = read_timeseries_db(source_path)
    source_points_by_series = _numeric_series_points_from_db(db)
    series_names = [series_name for series_name in db.list_series() if source_points_by_series.get(series_name)]
    series_decimals = {
        series_name: min(3, decimal_places_from_format_id(db.get_series_format_id(series_name)))
        for series_name in series_names
    }
    next_levels = [level for level in _DOWNSAMPLE_LEVELS if level[0] > source_bucket_ms]
    if not next_levels:
        print(f"No coarser downsample levels above {source_path}")
        return
    current_points_by_series = source_points_by_series
    for bucket_ms, label, elem_size in next_levels:
        output_path = _downsample_output_path(source_path, day, label)
        if os.path.exists(output_path):
            print(f"downsampling {os.path.basename(source_path)} -> {os.path.basename(output_path)} ({label}) already exists, skipping")
            current_points_by_series = _numeric_series_points_from_db(read_timeseries_db(output_path))
            continue
        print(f"downsampling {os.path.basename(source_path)} -> {os.path.basename(output_path)} ({label})")
        next_points_by_series: dict[str, list[tuple[int, Any]]] = {}
        for idx, series_name in enumerate(series_names, start=1):
            print(f"  series {idx}/{len(series_names)}: {series_name}")
            next_points_by_series[series_name] = downsample_series_points(
                current_points_by_series.get(series_name, []),
                day,
                bucket_ms,
                elem_size,
                series_decimals.get(series_name, 0),
            )
        _write_series_array_timeseries_db_atomic(
            output_path,
            day,
            bucket_ms,
            series_names,
            series_decimals,
            next_points_by_series,
            elem_size,
        )
        print(f"  wrote {output_path}")
        current_points_by_series = next_points_by_series

_EMBEDDED_DEMO_SERIES_TEXT = """
solar/ac/power=1200.0
solar/ac/yieldday=0
solar/ac/yieldtotal=12500
solar/114172608275/0/current=0.02
solar/114172608275/0/efficiency=95.000
solar/114172608275/0/frequency=49.99
solar/114172608275/0/power=5.7
solar/114172608275/0/powerdc=6.0
solar/114172608275/0/powerfactor=1.001
solar/114172608275/0/reactivepower=0.0
solar/114172608275/0/temperature=14.2
solar/114172608275/0/voltage=236.4
solar/114172608275/0/yieldday=175
solar/114172608275/0/yieldtotal=6.283
solar/114172608275/1/current=0.10
solar/114172608275/1/irradiation=0.600
solar/114172608275/1/power=3.0
solar/114172608275/1/voltage=29.3
solar/114172608275/1/yieldday=87
solar/114172608275/1/yieldtotal=3.151
solar/114172608275/2/current=0.10
solar/114172608275/2/irradiation=0.600
solar/114172608275/2/power=3.0
solar/114172608275/2/voltage=29.4
solar/114172608275/2/yieldday=88
solar/114172608275/2/yieldtotal=3.132
solar/114172608275/device/bootloaderversion=104
solar/114172608275/device/fwbuildversion=10008
solar/114172608275/device/hwpartnumber=269553683
solar/114172608275/name=HM600_BalkonUnten
solar/114172608275/radio/rssi=-80
solar/114172608275/status/last_update=1770827328
solar/114172608275/status/limit_absolute=600.00
solar/114172608275/status/limit_relative=100.00
solar/114172608275/status/producing=1
solar/114172608275/status/reachable=0
""".strip()


def _parse_demo_series_lines(lines: list[str]) -> list[tuple[str, Any, bool, int]]:
    series: list[tuple[str, Any, bool, int]] = []
    for line in lines:
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


def _load_demo_series(data_txt_path: str) -> list[tuple[str, Any, bool, int]]:
    with open(data_txt_path, "r", encoding="utf-8") as f:
        return _parse_demo_series_lines(f.readlines())


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
        series = _parse_demo_series_lines(_EMBEDDED_DEMO_SERIES_TEXT.splitlines())
    else:
        series = _load_demo_series(data_txt_path)
    if not series:
        raise ValueError(f"No series found in {data_txt_path!r}" if data_txt_path else "No embedded demo series configured")

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
        path = os.path.join(output_dir, f"demo_{day.isoformat()}.tsdb")
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
    config = load_collector_config(rc_path)
    mqtt = config.get("mqtt", {}) if isinstance(config.get("mqtt"), dict) else {}
    server = str(mqtt.get("mqtt_server", "")).strip()
    return server or None


def read_default_topics(rc_path: str) -> list[str]:
    config = load_collector_config(rc_path)
    mqtt = config.get("mqtt", {}) if isinstance(config.get("mqtt"), dict) else {}
    return _normalize_topics(mqtt.get("topics", []))


def read_default_quantize_timestamps(rc_path: str) -> int:
    config = load_collector_config(rc_path)
    mqtt = config.get("mqtt", {}) if isinstance(config.get("mqtt"), dict) else {}
    try:
        return max(0, int(mqtt.get("quantize_timestamps", 0)))
    except Exception:
        return 0


def read_default_data_dir(rc_path: str) -> str:
    config = load_collector_config(rc_path)
    mqtt = config.get("mqtt", {}) if isinstance(config.get("mqtt"), dict) else {}
    text = str(mqtt.get("data_dir", ".")).strip()
    return text if text else "."


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


def fetch_http_json_flattened(url: str, timeout: float = 10.0) -> tuple[Optional[dict[str, str]], Optional[str]]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read()
    except Exception as exc:
        return None, f"{exc.__class__.__name__}: {exc}"
    try:
        text = raw.decode("utf-8-sig")
    except Exception as exc:
        return None, f"{exc.__class__.__name__}: {exc}"
    return flatten_json(text)


def resolve_http_url(url: str, base_url: str) -> str:
    text = str(url or "").strip()
    if text.startswith("base_url/"):
        base = str(base_url or "").strip()
        if not base:
            return text
        return base.rstrip("/") + "/" + text[len("base_url/"):].lstrip("/")
    return text


def _value_from_http_text(text: str) -> Any:
    stripped = text.strip()
    numeric = _parse_strict_float(stripped)
    if numeric is None:
        return stripped
    return NumericWithDecimals(float(numeric), _decimal_places_from_literal(stripped))


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
    return os.path.expanduser("~/.tsdb_collector.toml")


def _load_toml_dict(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        try:
            import tomllib  # Python 3.11+
        except Exception:  # pragma: no cover - fallback if tomllib missing
            import toml as tomllib  # type: ignore
    except Exception:
        return {}
    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _normalize_topics(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def load_collector_config(rc_path: str) -> dict[str, Any]:
    data = _load_toml_dict(rc_path)
    mqtt_block = data.get("mqtt") if isinstance(data.get("mqtt"), dict) else {}
    http_block = data.get("http") if isinstance(data.get("http"), dict) else {}

    mqtt_server = ""
    for key in ("mqtt_server", "mqtt-server", "server"):
        if key in data and str(data[key]).strip():
            mqtt_server = str(data[key]).strip()
            break
    if not mqtt_server:
        for key in ("mqtt_server", "mqtt-server", "server"):
            if key in mqtt_block and str(mqtt_block[key]).strip():
                mqtt_server = str(mqtt_block[key]).strip()
                break

    topics: list[str] = []
    for key in ("topics", "mqtt_topics", "mqtt-topics"):
        if key in data:
            topics = _normalize_topics(data[key])
            if topics:
                break
    if not topics:
        for key in ("topics", "mqtt_topics", "mqtt-topics"):
            if key in mqtt_block:
                topics = _normalize_topics(mqtt_block[key])
                if topics:
                    break

    quantize_timestamps = 0
    for key in ("quantize_timestamps", "quantize-timestamps"):
        if key in data:
            try:
                quantize_timestamps = max(0, int(data[key]))
            except Exception:
                quantize_timestamps = 0
            break
    else:
        for key in ("quantize_timestamps", "quantize-timestamps"):
            if key in mqtt_block:
                try:
                    quantize_timestamps = max(0, int(mqtt_block[key]))
                except Exception:
                    quantize_timestamps = 0
                break

    data_dir = "."
    for key in ("data_dir", "data-dir"):
        if key in data:
            data_dir = str(data[key]).strip() or "."
            break
    else:
        for key in ("data_dir", "data-dir"):
            if key in mqtt_block:
                data_dir = str(mqtt_block[key]).strip() or "."
                break

    poll_interval_ms = 5000
    for key in ("poll_interval_ms", "poll-interval-ms", "http_poll_interval_ms"):
        if key in http_block:
            try:
                poll_interval_ms = max(100, int(http_block[key]))
            except Exception:
                poll_interval_ms = 5000
            break
        if key in data:
            try:
                poll_interval_ms = max(100, int(data[key]))
            except Exception:
                poll_interval_ms = 5000
            break

    base_url = ""
    for key in ("base_url", "base-url"):
        if key in http_block:
            base_url = str(http_block[key]).strip()
            break
        if key in data:
            base_url = str(data[key]).strip()
            break

    urls_raw = http_block.get("urls")
    # Backward compatibility with old shape: http.sources / http_sources.
    if not isinstance(urls_raw, list):
        urls_raw = http_block.get("sources", data.get("http_sources", []))
    if poll_interval_ms == 5000 and isinstance(urls_raw, list):
        for item in urls_raw:
            if not isinstance(item, dict):
                continue
            if "interval_ms" in item:
                try:
                    poll_interval_ms = max(100, int(item.get("interval_ms", 5000)))
                except Exception:
                    poll_interval_ms = 5000
                break
    urls: list[dict[str, Any]] = []
    if isinstance(urls_raw, list):
        for item in urls_raw:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url", "")).strip()
            base_topic = str(item.get("base_topic", item.get("topic_prefix", ""))).strip()
            values_raw = item.get("values", [])
            values: list[dict[str, Any]] = []
            if isinstance(values_raw, list):
                for value_item in values_raw:
                    if not isinstance(value_item, dict):
                        continue
                    path = str(value_item.get("path", "")).strip()
                    topic = str(value_item.get("topic", "")).strip()
                    if not path:
                        continue
                    values.append(
                        {
                            "path": path,
                            "topic": topic,
                        }
                    )
            urls.append(
                {
                    "url": url,
                    "base_topic": base_topic,
                    "values": values,
                }
            )

    return {
        "mqtt": {
            "mqtt_server": mqtt_server,
            "data_dir": data_dir,
            "quantize_timestamps": quantize_timestamps,
            "topics": topics,
        },
        "http": {
            "poll_interval_ms": poll_interval_ms,
            "base_url": base_url,
            "urls": urls,
        },
    }


def _quote_toml_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _format_toml_list(values: list[Any], indent: str = "    ") -> list[str]:
    if not values:
        return ["[]"]
    lines = ["["]
    for value in values:
        if isinstance(value, str):
            rendered = _quote_toml_string(value)
        elif isinstance(value, bool):
            rendered = "true" if value else "false"
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            rendered = str(value)
        else:
            rendered = _quote_toml_string(str(value))
        lines.append(f"{indent}{rendered},")
    lines.append("]")
    return lines


def _toml_top_level_item_id(stripped_line: str) -> Optional[str]:
    if stripped_line.startswith("[[") and stripped_line.endswith("]]"):
        return stripped_line
    if stripped_line.startswith("[") and stripped_line.endswith("]"):
        return stripped_line
    if "=" in stripped_line:
        key = stripped_line.split("=", 1)[0].strip()
        if key and " " not in key and not key.startswith("#"):
            return key
    return None


def _extract_toml_comment_blocks_by_item(path: str) -> tuple[dict[str, list[list[str]]], list[str], list[str]]:
    """Preserve comment/blank blocks and associate each with the next top-level TOML item."""
    if not os.path.exists(path):
        return {}, [], []
    blocks_by_item: dict[str, list[list[str]]] = {}
    pending_block: list[str] = []
    orphan_lines: list[str] = []
    trailing_lines: list[str] = []
    seen_any_item = False
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.rstrip("\n")
                stripped = line.strip()
                if stripped == "" or stripped.startswith("#"):
                    pending_block.append(line)
                    continue

                item_id = _toml_top_level_item_id(stripped)
                if item_id is not None:
                    seen_any_item = True
                    if pending_block:
                        blocks_by_item.setdefault(item_id, []).append(list(pending_block))
                        pending_block.clear()
                    continue

                # Non-item content breaks association.
                pending_block.clear()
    except Exception:
        return {}, [], []

    if pending_block:
        if seen_any_item:
            trailing_lines = pending_block
        else:
            orphan_lines = pending_block
    return blocks_by_item, orphan_lines, trailing_lines


def _dumps_collector_config_toml(
    config: dict[str, Any],
    comment_blocks_by_item: Optional[dict[str, list[list[str]]]] = None,
    orphan_preamble_lines: Optional[list[str]] = None,
    trailing_lines: Optional[list[str]] = None,
) -> str:
    mqtt = config.get("mqtt", {}) if isinstance(config.get("mqtt"), dict) else {}
    http = config.get("http", {}) if isinstance(config.get("http"), dict) else {}

    mqtt_server = str(mqtt.get("mqtt_server", "")).strip()
    data_dir = str(mqtt.get("data_dir", ".")).strip() or "."
    try:
        quantize = max(0, int(mqtt.get("quantize_timestamps", 0)))
    except Exception:
        quantize = 0
    topics = _normalize_topics(mqtt.get("topics", []))

    lines: list[str] = []
    blocks = dict(comment_blocks_by_item or {})

    def emit_item(item_id: str, line: str, aliases: Optional[list[str]] = None) -> None:
        keys = [item_id]
        if aliases:
            keys.extend(aliases)
        queue = None
        selected_key = None
        for key in keys:
            q = blocks.get(key)
            if q:
                queue = q
                selected_key = key
                break
        if queue and selected_key is not None:
            block = queue.pop(0)
            lines.extend(block)
        lines.append(line)

    def has_block(item_id: str, aliases: Optional[list[str]] = None) -> bool:
        keys = [item_id]
        if aliases:
            keys.extend(aliases)
        for key in keys:
            q = blocks.get(key)
            if q:
                return True
        return False

    if orphan_preamble_lines:
        lines.extend(orphan_preamble_lines)

    emit_item("mqtt_server", f'mqtt_server = {_quote_toml_string(mqtt_server)}')
    emit_item("data_dir", f'data_dir = {_quote_toml_string(data_dir)}')
    emit_item("quantize_timestamps", f"quantize_timestamps = {quantize}")
    topic_lines = _format_toml_list(topics, indent="    ")
    emit_item("topics", f"topics = {topic_lines[0]}")
    for continuation in topic_lines[1:]:
        lines.append(continuation)

    try:
        poll_interval_ms = max(100, int(http.get("poll_interval_ms", 5000)))
    except Exception:
        poll_interval_ms = 5000
    base_url = str(http.get("base_url", "")).strip()
    if lines and lines[-1].strip() != "" and not has_block("[http]"):
        lines.append("")
    emit_item("[http]", "[http]")
    lines.append(f"poll_interval_ms = {poll_interval_ms}")
    emit_item("base_url", f'base_url = {_quote_toml_string(base_url)}')

    urls = http.get("urls", [])
    if isinstance(urls, list):
        for url_item in urls:
            if not isinstance(url_item, dict):
                continue
            url = str(url_item.get("url", "")).strip()
            base_topic = str(url_item.get("base_topic", "")).strip()
            values = url_item.get("values", [])
            valid_values: list[tuple[str, str]] = []
            if isinstance(values, list):
                for value_item in values:
                    if not isinstance(value_item, dict):
                        continue
                    path = str(value_item.get("path", "")).strip()
                    topic = str(value_item.get("topic", "")).strip()
                    if not path:
                        continue
                    valid_values.append((path, topic))
            if not valid_values:
                continue
            if lines and lines[-1].strip() != "" and not has_block("[[http.urls]]", aliases=["[[http.sources]]"]):
                lines.append("")
            emit_item("[[http.urls]]", "[[http.urls]]", aliases=["[[http.sources]]"])
            lines.append(f'url = {_quote_toml_string(url)}')
            emit_item("base_topic", f'base_topic = {_quote_toml_string(base_topic)}', aliases=["topic_prefix"])
            for path, topic in valid_values:
                    if lines and lines[-1].strip() != "" and not has_block("[[http.urls.values]]"):
                        lines.append("")
                    emit_item("[[http.urls.values]]", "[[http.urls.values]]")
                    lines.append(f'path = {_quote_toml_string(path)}')
                    lines.append(f'topic = {_quote_toml_string(topic)}')
    if trailing_lines:
        lines.extend(trailing_lines)
    return "\n".join(lines) + "\n"


def save_collector_config(rc_path: str, config: dict[str, Any]) -> None:
    comment_blocks_by_item, orphan_preamble_lines, trailing_lines = _extract_toml_comment_blocks_by_item(rc_path)
    text = _dumps_collector_config_toml(
        config,
        comment_blocks_by_item=comment_blocks_by_item,
        orphan_preamble_lines=orphan_preamble_lines,
        trailing_lines=trailing_lines,
    )
    parent = os.path.dirname(os.path.abspath(rc_path))
    os.makedirs(parent, exist_ok=True)
    tmp = rc_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, rc_path)


class CollectorUiRequestHandler(BaseHTTPRequestHandler):
    server_version = "TSDBCollectorUI/1.0"

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

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
            self._send_json(400, {"error": {"code": "bad_request", "message": "Invalid static path"}})
            return True
        if not os.path.isfile(full_path):
            self._send_json(404, {"error": {"code": "not_found", "message": f"Static file not found: {rel}"}})
            return True
        with open(full_path, "rb") as f:
            body = f.read()
        mime, _ = mimetypes.guess_type(full_path)
        self._send_bytes(200, body, mime or "application/octet-stream")
        return True

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if self._handle_static(path):
            return
        if path == "/health":
            self._send_json(200, {"ok": True, "apiVersion": COLLECTOR_UI_API_VERSION})
            return
        if path == "/config":
            config_path = self.server.config_path  # type: ignore[attr-defined]
            self._send_json(200, {"configPath": config_path, "config": load_collector_config(config_path)})
            return
        if path == "/config/raw":
            config_path = self.server.config_path  # type: ignore[attr-defined]
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    content = f.read()
                mtime_ns = int(os.stat(config_path).st_mtime_ns)
            except FileNotFoundError:
                content = ""
                mtime_ns = 0
            except Exception as exc:
                self._send_json(500, {"error": {"code": "io_error", "message": str(exc)}})
                return
            self._send_json(200, {"configPath": config_path, "content": content, "mtimeNs": mtime_ns})
            return
        self._send_json(404, {"error": {"code": "not_found", "message": f"Unknown endpoint: {path}"}})

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path not in {"/config", "/config/raw"}:
            self._send_json(404, {"error": {"code": "not_found", "message": f"Unknown endpoint: {path}"}})
            return
        length_raw = self.headers.get("Content-Length", "").strip()
        if not length_raw:
            self._send_json(400, {"error": {"code": "bad_request", "message": "Missing Content-Length"}})
            return
        try:
            length = int(length_raw)
        except ValueError:
            self._send_json(400, {"error": {"code": "bad_request", "message": "Invalid Content-Length"}})
            return
        if length <= 0:
            self._send_json(400, {"error": {"code": "bad_request", "message": "Empty request body"}})
            return
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            self._send_json(400, {"error": {"code": "bad_request", "message": "Invalid JSON body"}})
            return
        config_path = self.server.config_path  # type: ignore[attr-defined]
        if path == "/config/raw":
            if not isinstance(payload, dict):
                self._send_json(400, {"error": {"code": "bad_request", "message": "Payload must be an object"}})
                return
            content = payload.get("content")
            if not isinstance(content, str):
                self._send_json(400, {"error": {"code": "bad_request", "message": "Missing string field: content"}})
                return
            try:
                os.makedirs(os.path.dirname(config_path) or ".", exist_ok=True)
                with open(config_path, "w", encoding="utf-8", newline="") as f:
                    f.write(content)
            except Exception as exc:
                self._send_json(500, {"error": {"code": "io_error", "message": str(exc)}})
                return
            self._send_json(200, {"ok": True, "configPath": config_path})
            return
        config = payload.get("config", payload) if isinstance(payload, dict) else None
        if not isinstance(config, dict):
            self._send_json(400, {"error": {"code": "bad_request", "message": "Config payload must be an object"}})
            return
        try:
            save_collector_config(config_path, config)
        except Exception as exc:
            self._send_json(500, {"error": {"code": "io_error", "message": str(exc)}})
            return
        self._send_json(200, {"ok": True, "configPath": config_path})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path != "/http/fetch":
            self._send_json(404, {"error": {"code": "not_found", "message": f"Unknown endpoint: {path}"}})
            return
        length_raw = self.headers.get("Content-Length", "").strip()
        if not length_raw:
            self._send_json(400, {"error": {"code": "bad_request", "message": "Missing Content-Length"}})
            return
        try:
            length = int(length_raw)
        except ValueError:
            self._send_json(400, {"error": {"code": "bad_request", "message": "Invalid Content-Length"}})
            return
        if length <= 0:
            self._send_json(400, {"error": {"code": "bad_request", "message": "Empty request body"}})
            return
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            self._send_json(400, {"error": {"code": "bad_request", "message": "Invalid JSON body"}})
            return
        if not isinstance(payload, dict):
            self._send_json(400, {"error": {"code": "bad_request", "message": "Request body must be object"}})
            return
        url_raw = str(payload.get("url", "")).strip()
        base_url = str(payload.get("base_url", "")).strip()
        url = resolve_http_url(url_raw, base_url)
        if not url_raw:
            self._send_json(400, {"error": {"code": "bad_request", "message": "Missing url"}})
            return
        flat, error = fetch_http_json_flattened(url)
        if error:
            self._send_json(502, {"error": {"code": "fetch_failed", "message": error}})
            return
        if flat is None:
            self._send_json(200, {"url": url, "values": [], "note": "JSON root is not an object"})
            return
        values = [{"path": k, "value": flat[k]} for k in sorted(flat.keys())]
        self._send_json(200, {"url": url, "values": values})

    def log_message(self, fmt: str, *args: Any) -> None:
        timestamp = time.strftime("%d/%b/%Y %H:%M:%S")
        message = f'{self.address_string()} - - [{timestamp}] {fmt % args}\n'
        sys.stdout.write(message)
        sys.stdout.flush()


class CollectorUiHttpServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], config_path: str, ui_dir: str):
        super().__init__(server_address, CollectorUiRequestHandler)
        self.config_path = config_path
        self.ui_dir = ui_dir


def main() -> int:
    default_config_path = _resolve_config_path_for_read()
    parser = argparse.ArgumentParser(
        description="TSDB collector utilities.",
    )
    parser.add_argument(
        "--config",
        default=default_config_path,
        help=f"Path to config file (default: {default_config_path})",
    )
    parser.add_argument("--ui", action="store_true", help="Start web UI for editing config and do not collect")
    parser.add_argument(
        "--ui-dir",
        default=os.path.join(os.path.dirname(__file__), "tsdb_collector_ui"),
        help="Directory containing UI assets (default: ./tsdb_collector_ui next to tsdb_collector.py)",
    )
    parser.add_argument("--ui-host", default="127.0.0.1", help="UI bind host (default: 127.0.0.1)")
    parser.add_argument("--ui-port", type=int, default=8081, help="UI bind port (default: 8081)")
    parser.add_argument(
        "--mqtt-server",
        help="MQTT server host[:port]. If omitted, read from config file",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Directory for TSDB files. If omitted, read data_dir from config file (default: current directory).",
    )
    parser.add_argument("--list-topics", action="store_true", help="List available topics as a flat list with hierarchical names")
    parser.add_argument("--monitor", action="store_true", help="Monitor topics and print messages as they arrive")
    parser.add_argument("--open-dtu-summary", action="store_true", help="Print OpenDTU inverter summary and totals (expects 'solar' root topic)")
    parser.add_argument("-c", "--collect", action="store_true", help="Collect subscribed MQTT topics into current TSDB files")
    parser.add_argument("--topics", action="append", default=[], help="MQTT subscription topic filter (repeatable)")
    parser.add_argument("--dump", help="Dump a TimeSeriesDB file in human-readable format")
    parser.add_argument("--dump-bytes", metavar="TSDB_FILE", help="Dump a TSDB file byte-by-byte with decoded semantics")
    parser.add_argument("--stat-tsdb", metavar="TSDB_FILE", help="Print byte statistics for a TimeSeriesDB file")
    parser.add_argument("--downsample", metavar="DATA_FILE", help="Read data_* or dsda_* TSDB file and create all coarser dsda_* variants up to 1h")
    parser.add_argument("--generate-demo-db", type=int, metavar="DAYS", help="Generate demo TSDB files for DAYS days")
    parser.add_argument("--compress", metavar="DBFILE", help="Compress a TSDB file in place")
    parser.add_argument("--timeout", type=float, default=1.0, help="Seconds to listen for topics when listing (default: 1.0)")
    parser.add_argument(
        "--quantize-timestamps",
        type=int,
        default=None,
        metavar="MS",
        help="Quantize collect timestamps to MS milliseconds (0 disables). If omitted, read quantize_timestamps from config file.",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity (can be repeated)")
    parser.add_argument("--filter", help="fnmatch pattern to filter topics (e.g. sensors/*)")
    parser.add_argument("--flatten", action="store_true", help="Flatten JSON payloads when listing topics")
    args = parser.parse_args()

    rc_path = os.path.expanduser(args.config)

    if args.ui:
        ui_dir = os.path.abspath(os.path.expanduser(args.ui_dir))
        if not os.path.isdir(ui_dir):
            print(f"UI directory not found: {ui_dir}")
            return 2
        if not (1 <= args.ui_port <= 65535):
            print("--ui-port must be in range 1..65535")
            return 2
        config_path = os.path.abspath(os.path.expanduser(rc_path))
        httpd = CollectorUiHttpServer((args.ui_host, args.ui_port), config_path=config_path, ui_dir=ui_dir)
        print(f"Serving TSDB Collector UI on http://{args.ui_host}:{args.ui_port} (config={config_path}, ui_dir={ui_dir})")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            httpd.server_close()
        return 0

    default_data_dir = read_default_data_dir(rc_path)
    selected_data_dir = args.data_dir if args.data_dir else default_data_dir
    data_dir = os.path.abspath(os.path.expanduser(selected_data_dir))

    def resolve_tsdb_path(path: str) -> str:
        expanded = os.path.expanduser(path)
        if os.path.isabs(expanded):
            return expanded
        return os.path.join(data_dir, expanded)

    if args.dump:
        dump_path = resolve_tsdb_path(args.dump)
        try:
            if args.verbose:
                read_timeseries_db(dump_path, dump_out=sys.stdout, verbose=args.verbose)
                return 0
            db = read_timeseries_db(dump_path)
        except Exception as exc:
            print(f"Failed to read DB file {dump_path!r}: {exc}")
            return 2
        db.dump()
        return 0
    if args.dump_bytes:
        dump_path = resolve_tsdb_path(args.dump_bytes)
        try:
            dump_timeseries_db_bytes(dump_path, out=sys.stdout)
        except Exception as exc:
            print(f"Failed to dump bytes for DB file {dump_path!r}: {exc}")
            return 2
        return 0
    if args.stat_tsdb:
        stat_path = resolve_tsdb_path(args.stat_tsdb)
        try:
            _print_tsdb_file_stats(stat_path)
        except Exception as exc:
            print(f"Failed to stat DB file {stat_path!r}: {exc}")
            return 2
        return 0
    if args.downsample:
        downsample_path = resolve_tsdb_path(args.downsample)
        try:
            _downsample_file(downsample_path)
        except Exception as exc:
            print(f"Failed to downsample DB file {downsample_path!r}: {exc}")
            return 2
        return 0
    if args.generate_demo_db is not None:
        try:
            os.makedirs(data_dir, exist_ok=True)
            paths = generateDemoData(args.generate_demo_db, output_dir=data_dir)
        except Exception as exc:
            print(f"Failed to generate demo DB files: {exc}")
            return 2
        for path in paths:
            print(path)
        return 0
    if args.compress:
        source = resolve_tsdb_path(args.compress)
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

    if args.mqtt_server:
        persist_server(rc_path, args.mqtt_server)
    loaded_config = load_collector_config(rc_path)
    default_server = read_default_server(rc_path)
    default_topics = read_default_topics(rc_path)
    default_quantize_timestamps = read_default_quantize_timestamps(rc_path)
    default_http = loaded_config.get("http", {}) if isinstance(loaded_config.get("http"), dict) else {}
    default_http_urls = default_http.get("urls", []) if isinstance(default_http.get("urls"), list) else []
    server = args.mqtt_server or default_server
    quantize_timestamps_ms = args.quantize_timestamps if args.quantize_timestamps is not None else default_quantize_timestamps
    quantize_timestamps_ms = max(0, quantize_timestamps_ms)

    if not server and not default_http_urls:
        print(f"MQTT server not specified. Use --mqtt-server or set mqtt_server in {rc_path}")
        return 2

    no_cli_options = len(sys.argv) == 1
    default_to_collect = no_cli_options and (bool(default_server) or bool(default_http_urls))

    if args.list_topics:
        if not server:
            print(f"MQTT server not specified. Use --mqtt-server or set mqtt_server in {rc_path}")
            return 2
        return list_topics(server, args.timeout, args.verbose, args.filter, args.flatten, False)
    if args.monitor:
        if not server:
            print(f"MQTT server not specified. Use --mqtt-server or set mqtt_server in {rc_path}")
            return 2
        return list_topics(server, args.timeout, args.verbose, args.filter, args.flatten, True)
    if args.open_dtu_summary:
        if not server:
            print(f"MQTT server not specified. Use --mqtt-server or set mqtt_server in {rc_path}")
            return 2
        return open_dtu_summary(server, args.timeout, args.filter, args.verbose)
    if args.collect or default_to_collect:
        topics = args.topics if args.topics else default_topics
        return collect_to_tsdb(
            server,
            topics,
            args.verbose,
            quantize_timestamps_ms=quantize_timestamps_ms,
            data_dir=data_dir,
            http_config=default_http,
        )

    print("No action specified. Use --ui, --list-topics, --open-dtu-summary, --collect, --dump, --dump-bytes, --stat-tsdb, --downsample, --generate-demo-db, or --compress.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
