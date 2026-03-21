#!/usr/bin/env python3
"""Extract mapped time series from InfluxDB (read-only) into TSDB files."""

import argparse
import datetime
import json
import math
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from tsdb import NumericWithDecimals, downsample_series_points, write_series_array_timeseries_db


_DOWNSAMPLE_LEVELS: list[tuple[int, str, int]] = [
    (1000, "1s", 1),
    (5000, "5s", 3),
    (15000, "15s", 3),
    (60000, "1m", 3),
    (300000, "5m", 3),
    (900000, "15m", 3),
    (3600000, "1h", 3),
]


@dataclass(frozen=True)
class MappingTarget:
    """One configured mapping target with optional scale."""

    series: str
    scale_op: str = "*"
    scale_factor: float = 1.0


def _format_mapping_target(target: MappingTarget) -> str:
    """Render one mapping target including optional scale for diagnostics."""
    if target.scale_op == "*" and abs(float(target.scale_factor) - 1.0) < 1e-12:
        return target.series
    return f"{target.series},{target.scale_op}{target.scale_factor:g}"


def _load_toml_dict(path: str) -> dict[str, Any]:
    try:
        import tomllib  # Python 3.11+
    except Exception:
        import toml as tomllib  # type: ignore
    with open(path, "rb") as f:
        data = tomllib.load(f)
    if not isinstance(data, dict):
        return {}
    return data


def _require_str(table: dict[str, Any], key: str, where: str) -> str:
    value = table.get(key)
    if not isinstance(value, str):
        raise ValueError(f"Missing or invalid {where}.{key} (expected string)")
    return value


def _parse_mapping_value(value: Any) -> list[MappingTarget]:
    tokens: list[str] = []
    if isinstance(value, str):
        tokens.extend(part.strip() for part in value.split(",") if part.strip())
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                tokens.extend(part.strip() for part in item.split(",") if part.strip())
    else:
        return []

    out: list[MappingTarget] = []
    for token in tokens:
        if token.startswith("*") or token.startswith("/"):
            if not out:
                raise ValueError(f"Scale {token!r} has no preceding target")
            factor_text = token[1:].strip()
            if not factor_text:
                raise ValueError(f"Missing scale factor in {token!r}")
            try:
                factor = float(factor_text)
            except Exception:
                raise ValueError(f"Invalid scale factor in {token!r}")
            if not math.isfinite(factor) or factor == 0:
                raise ValueError(f"Scale factor must be finite and non-zero in {token!r}")
            prev = out[-1]
            if prev.scale_factor != 1.0 or prev.scale_op != "*":
                raise ValueError(f"Duplicate scale for target {prev.series!r}")
            out[-1] = MappingTarget(series=prev.series, scale_op=token[0], scale_factor=factor)
            continue
        out.append(MappingTarget(series=token))
    return out


def _parse_config(path: str) -> tuple[dict[str, Any], dict[tuple[str, str], list[MappingTarget]]]:
    config = _load_toml_dict(path)

    influx = config.get("influxdb")
    if not isinstance(influx, dict):
        raise ValueError("Missing [influxdb] table in config")
    _require_str(influx, "url", "influxdb")
    _require_str(influx, "database", "influxdb")

    mapping_raw = config.get("mapping")
    if mapping_raw is None:
        mapping_raw = {}
    if not isinstance(mapping_raw, dict):
        raise ValueError("Invalid [mapping] table in config")

    parsed_mapping: dict[tuple[str, str], list[MappingTarget]] = {}
    for source_key, raw_targets in mapping_raw.items():
        if not isinstance(source_key, str):
            raise ValueError("Invalid mapping key (expected string)")
        if "/" not in source_key:
            raise ValueError(f"Invalid mapping key {source_key!r}; expected measurement/field")
        measurement, field = source_key.split("/", 1)
        measurement = measurement.strip()
        field = field.strip()
        if not measurement or not field:
            raise ValueError(f"Invalid mapping key {source_key!r}; empty measurement or field")
        try:
            targets = _parse_mapping_value(raw_targets)
        except ValueError as exc:
            raise ValueError(f"Invalid mapping value for {source_key!r}: {exc}")
        parsed_mapping[(measurement, field)] = targets

    return config, parsed_mapping


def _validate_no_target_conflicts(mapping: dict[tuple[str, str], list[MappingTarget]]) -> list[tuple[str, list[str]]]:
    target_to_sources: dict[str, set[str]] = {}
    for (measurement, field), targets in mapping.items():
        source_key = f"{measurement}/{field}"
        for target in targets:
            target_to_sources.setdefault(target.series, set()).add(source_key)
    conflicts: list[tuple[str, list[str]]] = []
    for target, sources in sorted(target_to_sources.items()):
        if len(sources) > 1:
            conflicts.append((target, sorted(sources)))
    return conflicts


def _escape_influx_ident(name: str) -> str:
    return '"' + name.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _influx_query(
    base_url: str,
    database: str,
    query: str,
    username: str,
    password: str,
    timeout_s: float,
) -> dict[str, Any]:
    endpoint = base_url.rstrip("/") + "/query"
    params: dict[str, str] = {"db": database, "q": query, "epoch": "ms"}
    if username:
        params["u"] = username
    if password:
        params["p"] = password
    url = endpoint + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        payload = resp.read()
    obj = json.loads(payload.decode("utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("Invalid JSON response")
    if "error" in obj:
        raise ValueError(f"InfluxDB error: {obj['error']}")
    results = obj.get("results")
    if isinstance(results, list):
        for entry in results:
            if isinstance(entry, dict) and "error" in entry:
                raise ValueError(f"InfluxDB error: {entry['error']}")
    return obj


def _round_to_max_3_decimals(value: float) -> NumericWithDecimals:
    rounded = round(float(value), 3)
    if not math.isfinite(rounded):
        raise ValueError("Non-finite numeric value")
    decimals = 3
    for d in range(0, 4):
        if abs(rounded - round(rounded, d)) < 1e-12:
            decimals = d
            break
    return NumericWithDecimals(rounded, decimals)


def _utc_day_from_ms(timestamp_ms: int) -> datetime.date:
    return datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=datetime.timezone.utc).date()


def _tsdb_filename_for_utc_day(day: datetime.date) -> str:
    return f"data_{day.isoformat()}.tsdb"


def _dsda_filename_for_utc_day(day: datetime.date, label: str) -> str:
    return f"dsda_{day.isoformat()}.{label}.tsdb"


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
        string_values=None,
    )
    os.replace(tmp_path, output_path)


def _extract_series_points(
    day_events: list[tuple[int, str, NumericWithDecimals]],
) -> tuple[list[str], dict[str, list[tuple[int, Any]]], dict[str, int]]:
    by_series: dict[str, list[tuple[int, Any]]] = {}
    series_decimals: dict[str, int] = {}
    for ts_ms, series_name, numeric in day_events:
        by_series.setdefault(series_name, []).append((int(ts_ms), float(numeric.value)))
        prev = series_decimals.get(series_name, 0)
        if numeric.decimals > prev:
            series_decimals[series_name] = min(3, int(numeric.decimals))
    for series_name in by_series.keys():
        by_series[series_name].sort(key=lambda item: item[0])
        series_decimals[series_name] = min(3, int(series_decimals.get(series_name, 0)))
    series_names = sorted(by_series.keys())
    return series_names, by_series, series_decimals


def _build_downsampled_levels_for_day(
    day: datetime.date,
    day_events: list[tuple[int, str, NumericWithDecimals]],
) -> dict[str, dict[str, list[tuple[int, Any]]]]:
    series_names, raw_points_by_series, series_decimals = _extract_series_points(day_events)
    levels_points: dict[str, dict[str, list[tuple[int, Any]]]] = {}
    if not series_names:
        return levels_points
    current_points_by_series = raw_points_by_series
    for bucket_ms, label, elem_size in _DOWNSAMPLE_LEVELS:
        next_points_by_series: dict[str, list[tuple[int, Any]]] = {}
        for series_name in series_names:
            next_points_by_series[series_name] = downsample_series_points(
                current_points_by_series.get(series_name, []),
                day,
                bucket_ms,
                elem_size,
                series_decimals.get(series_name, 0),
            )
        levels_points[label] = next_points_by_series
        current_points_by_series = next_points_by_series
    return levels_points


def _extract_day_events_for_measurement(
    influx_cfg: dict[str, Any],
    measurement: str,
    fields: list[str],
    target_by_source: dict[tuple[str, str], list[MappingTarget]],
    day_start_ms: int,
    day_end_ms: int,
    timeout_s: float,
) -> tuple[list[tuple[int, str, NumericWithDecimals]], dict[str, dict[str, int]]]:
    base_url = str(influx_cfg.get("url", "")).strip()
    database = str(influx_cfg.get("database", "")).strip()
    username = str(influx_cfg.get("username", "")).strip()
    password = str(influx_cfg.get("password", "")).strip()
    field_expr = ", ".join(_escape_influx_ident(field) for field in fields)
    query = (
        f"SELECT {field_expr} FROM {_escape_influx_ident(measurement)} "
        f"WHERE time >= {int(day_start_ms)}ms AND time < {int(day_end_ms)}ms"
    )
    response = _influx_query(base_url, database, query, username, password, timeout_s)
    out: list[tuple[int, str, NumericWithDecimals]] = []
    field_stats: dict[str, dict[str, int]] = {
        field: {"column_present": 0, "rows": 0, "numeric": 0, "null": 0, "non_numeric": 0}
        for field in fields
    }
    results = response.get("results", [])
    if not isinstance(results, list) or not results:
        return out, field_stats
    statement = results[0]
    if not isinstance(statement, dict):
        return out, field_stats
    series_list = statement.get("series", [])
    if not isinstance(series_list, list):
        return out, field_stats

    for series in series_list:
        if not isinstance(series, dict):
            continue
        columns = series.get("columns", [])
        values = series.get("values", [])
        if not isinstance(columns, list) or not isinstance(values, list):
            continue
        try:
            time_idx = columns.index("time")
        except ValueError:
            continue
        field_indices: dict[str, int] = {}
        for field in fields:
            if field in columns:
                field_indices[field] = columns.index(field)
                field_stats[field]["column_present"] = 1

        for row in values:
            if not isinstance(row, list):
                continue
            if time_idx >= len(row):
                continue
            ts_raw = row[time_idx]
            if not isinstance(ts_raw, (int, float)) or isinstance(ts_raw, bool):
                continue
            ts_ms = int(ts_raw)
            for field, idx in field_indices.items():
                if idx >= len(row):
                    continue
                field_stats[field]["rows"] += 1
                raw_value = row[idx]
                if raw_value is None:
                    field_stats[field]["null"] += 1
                    continue
                if not isinstance(raw_value, (int, float)) or isinstance(raw_value, bool):
                    field_stats[field]["non_numeric"] += 1
                    continue
                field_stats[field]["numeric"] += 1
                for target in target_by_source.get((measurement, field), []):
                    scaled_value = float(raw_value)
                    if target.scale_op == "/":
                        scaled_value = scaled_value / target.scale_factor
                    else:
                        scaled_value = scaled_value * target.scale_factor
                    numeric = _round_to_max_3_decimals(scaled_value)
                    out.append((ts_ms, target.series, numeric))
    return out, field_stats


def _measurement_time_bounds_ms(
    influx_cfg: dict[str, Any],
    measurement: str,
    timeout_s: float,
) -> tuple[int, int] | None:
    base_url = str(influx_cfg.get("url", "")).strip()
    database = str(influx_cfg.get("database", "")).strip()
    username = str(influx_cfg.get("username", "")).strip()
    password = str(influx_cfg.get("password", "")).strip()
    q_asc = f"SELECT * FROM {_escape_influx_ident(measurement)} ORDER BY time ASC LIMIT 1"
    q_desc = f"SELECT * FROM {_escape_influx_ident(measurement)} ORDER BY time DESC LIMIT 1"
    asc = _influx_query(base_url, database, q_asc, username, password, timeout_s)
    desc = _influx_query(base_url, database, q_desc, username, password, timeout_s)

    def first_ts(payload: dict[str, Any]) -> int | None:
        results = payload.get("results", [])
        if not isinstance(results, list) or not results:
            return None
        st = results[0]
        if not isinstance(st, dict):
            return None
        series = st.get("series", [])
        if not isinstance(series, list) or not series:
            return None
        first_series = series[0]
        if not isinstance(first_series, dict):
            return None
        cols = first_series.get("columns", [])
        rows = first_series.get("values", [])
        if not isinstance(cols, list) or not isinstance(rows, list) or not rows:
            return None
        try:
            idx = cols.index("time")
        except ValueError:
            return None
        first_row = rows[0]
        if not isinstance(first_row, list) or idx >= len(first_row):
            return None
        ts = first_row[idx]
        if not isinstance(ts, (int, float)) or isinstance(ts, bool):
            return None
        return int(ts)

    min_ms = first_ts(asc)
    max_ms = first_ts(desc)
    if min_ms is None or max_ms is None:
        return None
    if max_ms < min_ms:
        min_ms, max_ms = max_ms, min_ms
    return min_ms, max_ms


def _day_bounds_ms(day: datetime.date) -> tuple[int, int]:
    start = datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc)
    end = start + datetime.timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _extract_events(
    influx_cfg: dict[str, Any],
    mapping: dict[tuple[str, str], list[MappingTarget]],
    day_filter: datetime.date | None,
    num_days: int,
    verbose: int = 0,
    timeout_s: float = 30.0,
) -> tuple[
    dict[datetime.date, list[tuple[int, str, NumericWithDecimals]]],
    dict[datetime.date, dict[str, dict[str, int]]],
]:
    source_by_measurement: dict[str, list[str]] = {}
    target_by_source: dict[tuple[str, str], list[MappingTarget]] = {}
    for (measurement, field), targets in mapping.items():
        if not targets:
            continue
        source_by_measurement.setdefault(measurement, []).append(field)
        target_by_source[(measurement, field)] = targets

    if not source_by_measurement:
        return {}, {}

    requested_days: list[datetime.date] = []
    if day_filter is not None:
        requested_days = [day_filter + datetime.timedelta(days=offset) for offset in range(max(1, int(num_days)))]
    else:
        min_day: datetime.date | None = None
        max_day: datetime.date | None = None
        for measurement in sorted(source_by_measurement.keys()):
            bounds = _measurement_time_bounds_ms(influx_cfg, measurement, timeout_s)
            if bounds is None:
                continue
            day_lo = _utc_day_from_ms(bounds[0])
            day_hi = _utc_day_from_ms(bounds[1])
            if min_day is None or day_lo < min_day:
                min_day = day_lo
            if max_day is None or day_hi > max_day:
                max_day = day_hi
        if min_day is None or max_day is None:
            return {}, {}
        day = min_day
        while day <= max_day:
            requested_days.append(day)
            day = day + datetime.timedelta(days=1)

    events_by_day: dict[datetime.date, list[tuple[int, str, NumericWithDecimals]]] = {}
    source_stats_by_day: dict[datetime.date, dict[str, dict[str, int]]] = {}
    all_source_keys = sorted(target_by_source.keys(), key=lambda k: (k[0], k[1]))
    for day in requested_days:
        day_start_ms, day_end_ms = _day_bounds_ms(day)
        day_events: list[tuple[int, str, NumericWithDecimals]] = []
        day_source_stats: dict[str, dict[str, int]] = {
            f"{measurement}/{field}": {"column_present": 0, "rows": 0, "numeric": 0, "null": 0, "non_numeric": 0}
            for measurement, field in all_source_keys
        }
        for measurement in sorted(source_by_measurement.keys()):
            fields = sorted(set(source_by_measurement[measurement]))
            if verbose:
                print(f"query: day={day.isoformat()} measurement={measurement} fields={len(fields)}")
            part, field_stats = _extract_day_events_for_measurement(
                influx_cfg=influx_cfg,
                measurement=measurement,
                fields=fields,
                target_by_source=target_by_source,
                day_start_ms=day_start_ms,
                day_end_ms=day_end_ms,
                timeout_s=timeout_s,
            )
            for field in fields:
                source_key = f"{measurement}/{field}"
                src_stats = day_source_stats[source_key]
                fs = field_stats.get(field, {})
                src_stats["column_present"] = max(src_stats["column_present"], int(fs.get("column_present", 0)))
                src_stats["rows"] += int(fs.get("rows", 0))
                src_stats["numeric"] += int(fs.get("numeric", 0))
                src_stats["null"] += int(fs.get("null", 0))
                src_stats["non_numeric"] += int(fs.get("non_numeric", 0))
            if part:
                day_events.extend(part)
        source_stats_by_day[day] = day_source_stats
        if day_events:
            day_events.sort(key=lambda item: (item[0], item[1]))
            events_by_day[day] = day_events
            if verbose:
                print(f"events: day={day.isoformat()} count={len(day_events)}")
    return events_by_day, source_stats_by_day


def _resolve_requested_days(
    influx_cfg: dict[str, Any],
    mapping: dict[tuple[str, str], list[MappingTarget]],
    day_filter: datetime.date | None,
    num_days: int,
    timeout_s: float,
) -> list[datetime.date]:
    """Resolve the ordered list of days that should be processed."""
    if day_filter is not None:
        return [day_filter + datetime.timedelta(days=offset) for offset in range(max(1, int(num_days)))]

    source_measurements = sorted({measurement for (measurement, _field), targets in mapping.items() if targets})
    min_day: datetime.date | None = None
    max_day: datetime.date | None = None
    for measurement in source_measurements:
        bounds = _measurement_time_bounds_ms(influx_cfg, measurement, timeout_s)
        if bounds is None:
            continue
        day_lo = _utc_day_from_ms(bounds[0])
        day_hi = _utc_day_from_ms(bounds[1])
        if min_day is None or day_lo < min_day:
            min_day = day_lo
        if max_day is None or day_hi > max_day:
            max_day = day_hi
    if min_day is None or max_day is None:
        return []
    out: list[datetime.date] = []
    day = min_day
    while day <= max_day:
        out.append(day)
        day = day + datetime.timedelta(days=1)
    return out


def _source_missing_reason(stats: dict[str, int]) -> str:
    if int(stats.get("numeric", 0)) > 0:
        return ""
    if int(stats.get("column_present", 0)) <= 0:
        return "field not present in query result"
    if int(stats.get("rows", 0)) <= 0:
        return "no rows in selected day"
    null_count = int(stats.get("null", 0))
    non_numeric_count = int(stats.get("non_numeric", 0))
    if null_count > 0 and non_numeric_count <= 0:
        return "only null values"
    if non_numeric_count > 0 and null_count <= 0:
        return "only non-numeric values"
    if null_count > 0 and non_numeric_count > 0:
        return "only null/non-numeric values"
    return "no numeric values"


def _print_unrepresented_mappings(
    day: datetime.date,
    active_mapping: dict[tuple[str, str], list[MappingTarget]],
    day_source_stats: dict[str, dict[str, int]],
    represented_series: set[str],
) -> None:
    print(f"unrepresented mapping for day {day.isoformat()}:")

    missing_sources: list[tuple[str, list[str], dict[str, int], str]] = []
    target_sources: dict[str, list[str]] = {}
    source_reason_by_key: dict[str, str] = {}
    for (measurement, field), targets in sorted(active_mapping.items(), key=lambda item: (item[0][0], item[0][1])):
        source_key = f"{measurement}/{field}"
        stats = day_source_stats.get(source_key, {"column_present": 0, "rows": 0, "numeric": 0, "null": 0, "non_numeric": 0})
        reason = _source_missing_reason(stats)
        source_reason_by_key[source_key] = reason
        target_series = [target.series for target in targets]
        if reason:
            missing_sources.append((source_key, target_series, stats, reason))
        for target in targets:
            target_sources.setdefault(target.series, []).append(source_key)

    if not missing_sources:
        print("  sources: all represented")
    else:
        print(f"  sources ({len(missing_sources)}):")
        for source_key, targets, stats, reason in missing_sources:
            target_text = ", ".join(targets)
            print(
                f"    {source_key} -> {target_text}: {reason} "
                f"(rows={int(stats.get('rows', 0))}, null={int(stats.get('null', 0))}, non_numeric={int(stats.get('non_numeric', 0))})"
            )

    mapped_targets = sorted(target_sources.keys())
    missing_targets = [target for target in mapped_targets if target not in represented_series]
    if not missing_targets:
        print("  targets: all represented")
    else:
        print(f"  targets ({len(missing_targets)}):")
        for target in missing_targets:
            sources = target_sources.get(target, [])
            reasons = sorted({source_reason_by_key.get(source, "unknown") for source in sources})
            source_text = ", ".join(sources)
            reason_text = "; ".join(reasons)
            print(f"    {target} <= {source_text}: {reason_text}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract InfluxDB data into TSDB files.")
    parser.add_argument("--config", required=True, help="Path to TOML config file")
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory for generated TSDB files (default: data)",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing output TSDB files")
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds for InfluxDB requests (default: 30.0)",
    )
    parser.add_argument(
        "--day",
        default="",
        help="Restrict extraction to one UTC day (YYYY-MM-DD)",
    )
    parser.add_argument(
        "-n",
        "--num-days",
        type=int,
        default=1,
        help="Number of sequential UTC days to extract, starting at --day (default: 1)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write files; print what would be written",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = os.path.abspath(os.path.expanduser(args.config))
    data_dir = os.path.abspath(os.path.expanduser(args.data_dir))

    try:
        config, mapping = _parse_config(config_path)
    except Exception as exc:
        print(f"Failed to read config {config_path!r}: {exc}")
        return 2

    influx_cfg = config.get("influxdb", {})
    day_filter: datetime.date | None = None
    if args.day:
        try:
            day_filter = datetime.date.fromisoformat(args.day)
        except Exception:
            print(f"Invalid --day value {args.day!r}; expected YYYY-MM-DD")
            return 2
    if int(args.num_days) <= 0:
        print(f"Invalid --num-days value {args.num_days!r}; expected > 0")
        return 2

    active_mapping = {(measurement, field): list(targets) for (measurement, field), targets in mapping.items() if targets}
    conflicts = _validate_no_target_conflicts(active_mapping)
    if conflicts:
        print("Invalid config: conflicting mappings (multiple sources map to same target):")
        for target, sources in conflicts:
            print(f"  {target} <= {', '.join(sources)}")
        print("Please resolve conflicts before extraction.")
        return 2

    mapped_sources = len(active_mapping)
    mapped_targets = sum(len(targets) for targets in active_mapping.values())
    print(
        f"Loaded config: url={influx_cfg.get('url')} database={influx_cfg.get('database')} "
        f"mapped_sources={mapped_sources} mapped_targets={mapped_targets} "
        f"day={day_filter.isoformat() if day_filter else 'all'} "
        f"num_days={int(args.num_days)}"
    )

    try:
        requested_days = _resolve_requested_days(
            influx_cfg=influx_cfg if isinstance(influx_cfg, dict) else {},
            mapping=mapping,
            day_filter=day_filter,
            num_days=int(args.num_days),
            timeout_s=float(args.timeout),
        )
    except urllib.error.URLError as exc:
        print(f"InfluxDB request failed: {exc}")
        return 2
    except Exception as exc:
        print(f"Extraction failed: {exc}")
        return 2

    if not requested_days:
        print("No days to process.")
        return 0

    day_outputs: dict[datetime.date, list[str]] = {}
    for day in requested_days:
        outputs: list[str] = []
        for _bucket_ms, label, _elem_size in _DOWNSAMPLE_LEVELS:
            outputs.append(os.path.join(data_dir, _dsda_filename_for_utc_day(day, label)))
        day_outputs[day] = outputs

    if args.dry_run:
        print("Dry run: no files will be written.")
        print("Effective mapping:")
        for (measurement, field), targets in sorted(active_mapping.items(), key=lambda item: (item[0][0], item[0][1])):
            target_text = ", ".join(_format_mapping_target(target) for target in targets)
            print(f"  {measurement}/{field} -> {target_text}")
        total_events = 0
        per_series_total: dict[str, int] = {}
        any_events = False
        for day in requested_days:
            try:
                day_events_map, day_stats_map = _extract_events(
                    influx_cfg=influx_cfg if isinstance(influx_cfg, dict) else {},
                    mapping=mapping,
                    day_filter=day,
                    num_days=1,
                    verbose=int(args.verbose or 0),
                    timeout_s=float(args.timeout),
                )
            except urllib.error.URLError as exc:
                print(f"InfluxDB request failed: {exc}")
                return 2
            except Exception as exc:
                print(f"Extraction failed: {exc}")
                return 2
            day_events = day_events_map.get(day, [])
            day_stats = day_stats_map.get(day, {})
            total_events += len(day_events)
            per_series_day: dict[str, int] = {}
            for _ts, series, _value in day_events:
                per_series_day[series] = per_series_day.get(series, 0) + 1
                per_series_total[series] = per_series_total.get(series, 0) + 1
            if day_events:
                any_events = True
            print(f"day {day.isoformat()}: events={len(day_events)} series={len(per_series_day)}")
            for series in sorted(per_series_day.keys()):
                print(f"  {series}: {per_series_day[series]}")
            for out_path in day_outputs.get(day, []):
                print(f"  output: {out_path}")
            _print_unrepresented_mappings(
                day=day,
                active_mapping=active_mapping,
                day_source_stats=day_stats,
                represented_series=set(per_series_day.keys()),
            )
        if not any_events:
            print("No mapped numeric events found.")
        print(f"total events: {total_events}")
        print(f"total series: {len(per_series_total)}")
        return 0

    os.makedirs(data_dir, exist_ok=True)
    if not args.force:
        requested_days_filtered: list[datetime.date] = []
        for day in requested_days:
            existing_for_day = [path for path in day_outputs.get(day, []) if os.path.exists(path)]
            if existing_for_day:
                print(f"Warning: skipping day {day.isoformat()} because output files already exist:")
                for path in existing_for_day:
                    print(f"  {path}")
                continue
            requested_days_filtered.append(day)
        requested_days = requested_days_filtered
        if not requested_days:
            print("No days left to process (all requested days already have output files).")
            return 0

    any_events = False
    for day in requested_days:
        try:
            day_events_map, day_stats_map = _extract_events(
                influx_cfg=influx_cfg if isinstance(influx_cfg, dict) else {},
                mapping=mapping,
                day_filter=day,
                num_days=1,
                verbose=int(args.verbose or 0),
                timeout_s=float(args.timeout),
            )
        except urllib.error.URLError as exc:
            print(f"InfluxDB request failed: {exc}")
            return 2
        except Exception as exc:
            print(f"Extraction failed: {exc}")
            return 2
        day_events = day_events_map.get(day, [])
        day_stats = day_stats_map.get(day, {})
        if not day_events:
            _print_unrepresented_mappings(
                day=day,
                active_mapping=active_mapping,
                day_source_stats=day_stats,
                represented_series=set(),
            )
            continue
        any_events = True
        series_names, _raw_points_by_series, series_decimals = _extract_series_points(day_events)
        levels_points = _build_downsampled_levels_for_day(day, day_events)
        for bucket_ms, label, elem_size in _DOWNSAMPLE_LEVELS:
            out_path = os.path.join(data_dir, _dsda_filename_for_utc_day(day, label))
            if os.path.exists(out_path) and args.force:
                os.remove(out_path)
            _write_series_array_timeseries_db_atomic(
                out_path,
                day,
                bucket_ms,
                series_names,
                series_decimals,
                levels_points.get(label, {}),
                elem_size,
            )
            points_total = sum(len(levels_points.get(label, {}).get(name, [])) for name in series_names)
            print(f"wrote {out_path} points={points_total} series={len(series_names)}")
        _print_unrepresented_mappings(
            day=day,
            active_mapping=active_mapping,
            day_source_stats=day_stats,
            represented_series=set(series_names),
        )

    if not any_events:
        print("No mapped numeric events found.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
