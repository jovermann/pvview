#!/usr/bin/env python3
"""Extract data from InfluxDB into TSDB files.

This tool currently provides config loading and validation scaffolding.
"""

import argparse
import os
import sys
from typing import Any


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


def _parse_config(path: str) -> dict[str, Any]:
    config = _load_toml_dict(path)
    influx = config.get("influxdb")
    if not isinstance(influx, dict):
        raise ValueError("Missing [influxdb] table in config")
    _require_str(influx, "url", "influxdb")
    _require_str(influx, "database", "influxdb")
    mapping = config.get("mapping")
    if mapping is None:
        mapping = {}
    if not isinstance(mapping, dict):
        raise ValueError("Invalid [mapping] table in config")
    for key, value in mapping.items():
        if not isinstance(key, str):
            raise ValueError("Invalid [mapping] key (expected string)")
        if not isinstance(value, str):
            raise ValueError(f"Invalid [mapping] value for {key!r} (expected string)")
    return config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract InfluxDB data to TSDB files.")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to TOML config file",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = os.path.abspath(os.path.expanduser(args.config))
    try:
        config = _parse_config(config_path)
    except Exception as exc:
        print(f"Failed to read config {config_path!r}: {exc}")
        return 2

    influx = config["influxdb"]
    mapping = config.get("mapping", {})
    non_empty_targets = sum(1 for v in mapping.values() if isinstance(v, str) and v.strip())
    print(
        "Loaded config: "
        f"url={influx.get('url')} "
        f"database={influx.get('database')} "
        f"mapped={non_empty_targets}/{len(mapping)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
