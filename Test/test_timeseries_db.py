import datetime
import pytest
import sys
import subprocess
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tsdb_collector import (
    _downsample_file,
    TimeSeriesDbAppender,
    compress_timeseries_db_file,
    create_timeseries_db_writer,
    generateDemoData,
    load_collector_config,
    read_timeseries_db,
    save_collector_config,
)
from tsdb import get_cached_tsdb_file, write_downsampled_timeseries_db, stat_timeseries_db, write_series_array_timeseries_db
from tsdb_server import _get_or_build_downsampled_day_points, get_virtual_series_points, save_virtual_series_config, VirtualSeriesDef


def test_roundtrip_double_and_string_values(tmp_path):
    path = tmp_path / "sample.tsdb"

    writer = create_timeseries_db_writer(str(path))
    writer.addValue("pv.power", 123.5, timestamp_ms=1000)
    writer.addStringValue("status.mode", "running", timestamp_ms=1000)
    writer.addValue("pv.power", 124.25, timestamp_ms=1500)
    writer.addStringValue("status.mode", "idle", timestamp_ms=2300)
    writer.close(mark_complete=True)

    db = read_timeseries_db(str(path))

    assert db.list_series() == ["pv.power", "status.mode"]
    assert db.get_series_values("pv.power") == [(1000, 123.5), (1500, 124.25)]
    assert db.get_series_values("status.mode") == [(1000, "running"), (2300, "idle")]


def test_reader_returns_absolute_timestamps(tmp_path):
    path = tmp_path / "timestamps.tsdb"

    writer = create_timeseries_db_writer(str(path))
    writer.addValue("a", 1.0, timestamp_ms=2000)
    writer.addValue("a", 2.0, timestamp_ms=2000)
    writer.addValue("a", 3.0, timestamp_ms=2100)
    writer.addValue("a", 4.0, timestamp_ms=1000)  # forces a new absolute timestamp
    writer.close()

    db = read_timeseries_db(str(path))
    assert db.get_series_values("a") == [(2000, 1.0), (2000, 2.0), (2100, 3.0), (1000, 4.0)]


def test_series_format_is_locked_by_first_value_type(tmp_path):
    path = tmp_path / "format_lock.tsdb"

    writer = create_timeseries_db_writer(str(path))
    writer.addValue("series1", 1.23, timestamp_ms=1)
    with pytest.raises(ValueError):
        writer.addStringValue("series1", "nope", timestamp_ms=2)
    writer.close()


def test_reader_rejects_invalid_tag(tmp_path):
    path = tmp_path / "invalid.tsdb"
    path.write_bytes(b"NOT_TSDB_FILE")

    with pytest.raises(ValueError, match="Invalid TSDB tag"):
        read_timeseries_db(str(path))


def test_querying_missing_series_returns_empty_list(tmp_path):
    path = tmp_path / "empty_series.tsdb"

    writer = create_timeseries_db_writer(str(path))
    writer.addValue("present", 9.0, timestamp_ms=100)
    writer.close()

    db = read_timeseries_db(str(path))
    assert db.get_series_values("missing") == []


def test_downsampled_file_roundtrip_and_cache_metadata(tmp_path):
    path = tmp_path / "data5s_2026-02-20.tsdb"
    write_downsampled_timeseries_db(
        str(path),
        5000,
        ["pv.power", "state"],
        {"pv.power": 0x02, "state": 0x08},
        {"pv.power": [(2500, 100.0, 101.25, 102.0)]},
        {"state": [(2500, "on")]},
    )

    db = read_timeseries_db(str(path))
    assert db.get_meta_info("dsBucketMs") == 5000
    assert db.get_series_values("pv.power") == [(2500, {"min": 100.0, "avg": 101.25, "max": 102.0})]
    assert db.get_series_values("state") == [(2500, "on")]

    cache = get_cached_tsdb_file(str(path))
    assert cache.ds_bucket_ms == 5000
    assert cache.meta_info["dsBucketMs"] == 5000


def test_series_array_roundtrip(tmp_path):
    day = datetime.date(2026, 2, 20)
    path = tmp_path / "dsda_2026-02-20.5s.tsdb"
    day_start_ms = int(datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    write_series_array_timeseries_db(
        str(path),
        day,
        5000,
        ["pv.power", "pv.temp"],
        {"pv.power": 1, "pv.temp": 1},
        {
            "pv.power": [
                (day_start_ms + 2500, {"min": 10.0, "avg": 11.0, "max": 12.0}),
                (day_start_ms + 7500, {"min": 20.0, "avg": 21.0, "max": 22.0}),
            ],
            "pv.temp": [
                (day_start_ms + 2500, {"min": 30.0, "avg": 31.0, "max": 32.0}),
            ],
        },
        3,
    )
    db = read_timeseries_db(str(path))
    assert db.get_series_values("pv.power") == [
        (day_start_ms + 2500, {"min": 10.0, "avg": 11.0, "max": 12.0}),
        (day_start_ms + 7500, {"min": 20.0, "avg": 21.0, "max": 22.0}),
    ]
    assert db.get_series_values("pv.temp") == [
        (day_start_ms + 2500, {"min": 30.0, "avg": 31.0, "max": 32.0}),
    ]


def test_current_day_downsampling_updates_incrementally_for_completed_buckets(tmp_path):
    day = datetime.datetime.now(datetime.timezone.utc).date()
    day_start_ms = int(datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    path = tmp_path / f"data_{day.isoformat()}.tsdb"

    writer = create_timeseries_db_writer(str(path))
    writer.addValue("pv.power", 10.0, timestamp_ms=day_start_ms + 1000)
    writer.addValue("pv.power", 12.0, timestamp_ms=day_start_ms + 2000)
    writer.addValue("pv.power", 14.0, timestamp_ms=day_start_ms + 4000)
    writer.close()

    files, points = _get_or_build_downsampled_day_points(str(tmp_path), day, 5000, "pv.power", day_start_ms, day_start_ms + 10000)
    assert files == [path.name]
    assert points == []

    appender = TimeSeriesDbAppender(str(path))
    appender.append_events([(day_start_ms + 6000, "pv.power", 20.0)])

    files, points = _get_or_build_downsampled_day_points(str(tmp_path), day, 5000, "pv.power", day_start_ms, day_start_ms + 10000)
    assert files == [path.name]
    assert len(points) == 1
    assert points[0]["timestamp"] == day_start_ms + 2500
    assert points[0]["start"] == day_start_ms
    assert points[0]["end"] == day_start_ms + 4999
    assert points[0]["count"] == 3
    assert points[0]["min"] == pytest.approx(10.0)
    assert points[0]["avg"] == pytest.approx(12.0)
    assert points[0]["max"] == pytest.approx(14.0)


def test_server_past_day_downsampling_builds_all_dsda_variants(tmp_path):
    day = datetime.date(2026, 2, 20)
    day_start_ms = int(datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    path = tmp_path / f"data_{day.isoformat()}.tsdb"

    writer = create_timeseries_db_writer(str(path))
    writer.addValue("pv.power", 10.0, timestamp_ms=day_start_ms + 1000)
    writer.addValue("pv.power", 12.0, timestamp_ms=day_start_ms + 2000)
    writer.addValue("pv.power", 20.0, timestamp_ms=day_start_ms + 6000)
    writer.close(mark_complete=True)
    _downsample_file(str(path))

    files, points = _get_or_build_downsampled_day_points(str(tmp_path), day, 5000, "pv.power", day_start_ms, day_start_ms + 10000)
    assert files == [f"dsda_{day.isoformat()}.5s.tsdb"]
    assert points
    for label in ("1s", "5s", "15s", "1m", "5m", "15m", "1h"):
        assert (tmp_path / f"dsda_{day.isoformat()}.{label}.tsdb").exists()


def test_virtual_series_uses_bucketed_source_data(tmp_path):
    day = datetime.date(2026, 2, 20)
    day_start_ms = int(datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    path = tmp_path / f"data_{day.isoformat()}.tsdb"

    writer = create_timeseries_db_writer(str(path))
    writer.addValue("a", 10.0, timestamp_ms=day_start_ms + 1000)
    writer.addValue("a", 20.0, timestamp_ms=day_start_ms + 3000)
    writer.addValue("a", 30.0, timestamp_ms=day_start_ms + 6000)
    writer.addValue("a", 40.0, timestamp_ms=day_start_ms + 8000)
    writer.addValue("b", 1.0, timestamp_ms=day_start_ms + 1200)
    writer.addValue("b", 2.0, timestamp_ms=day_start_ms + 3200)
    writer.addValue("b", 3.0, timestamp_ms=day_start_ms + 6200)
    writer.addValue("b", 4.0, timestamp_ms=day_start_ms + 8200)
    writer.close(mark_complete=True)
    _downsample_file(str(path))

    save_virtual_series_config(str(tmp_path), [VirtualSeriesDef(name="sum", left="a", op="+", right="b")], [], 10000)
    result = get_virtual_series_points(str(tmp_path), "sum", day_start_ms, day_start_ms + 9999, 5000)
    assert result is not None
    points, decimal_places, files = result
    assert decimal_places == 0
    assert files == [f"dsda_{day.isoformat()}.5s.tsdb"]
    assert len(points) == 2
    assert points[0]["timestamp"] == day_start_ms + 2500
    assert points[0]["min"] == pytest.approx(11.0)
    assert points[0]["avg"] == pytest.approx(17.0)
    assert points[0]["max"] == pytest.approx(22.0)
    assert points[1]["timestamp"] == day_start_ms + 7500
    assert points[1]["min"] == pytest.approx(33.0)
    assert points[1]["avg"] == pytest.approx(39.0)
    assert points[1]["max"] == pytest.approx(44.0)


def test_virtual_yesterday_uses_bucketed_source_data(tmp_path):
    day1 = datetime.date(2026, 2, 20)
    day2 = datetime.date(2026, 2, 21)
    start1 = int(datetime.datetime(day1.year, day1.month, day1.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    start2 = int(datetime.datetime(day2.year, day2.month, day2.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    path1 = tmp_path / f"data_{day1.isoformat()}.tsdb"
    path2 = tmp_path / f"data_{day2.isoformat()}.tsdb"

    writer1 = create_timeseries_db_writer(str(path1))
    writer1.addValue("yieldtotal", 100.0, timestamp_ms=start1 + 1000)
    writer1.addValue("yieldtotal", 110.0, timestamp_ms=start1 + 3000)
    writer1.close(mark_complete=True)
    _downsample_file(str(path1))

    writer2 = create_timeseries_db_writer(str(path2))
    writer2.addValue("yieldtotal", 125.0, timestamp_ms=start2 + 1000)
    writer2.addValue("yieldtotal", 135.0, timestamp_ms=start2 + 3000)
    writer2.close(mark_complete=True)
    _downsample_file(str(path2))

    save_virtual_series_config(str(tmp_path), [VirtualSeriesDef(name="yday", left="yieldtotal", op="yesterday", right="")], [], 10000)
    result = get_virtual_series_points(str(tmp_path), "yday", start2, start2 + 4999, 5000)
    assert result is not None
    points, _decimal_places, files = result
    assert files == [f"dsda_{day1.isoformat()}.5s.tsdb", f"dsda_{day2.isoformat()}.5s.tsdb"]
    assert len(points) == 1
    assert points[0]["timestamp"] == start2 + 2500
    assert points[0]["avg"] == pytest.approx(25.0)
    assert points[0]["min"] == pytest.approx(25.0)
    assert points[0]["max"] == pytest.approx(25.0)


def _extract_channel_format_ids(path):
    raw = path.read_bytes()
    offset = 12
    result = {}
    while offset < len(raw):
        t = raw[offset]
        offset += 1
        if t == 0xF5:
            channel_id = raw[offset]
            format_id = raw[offset + 1]
            name_len = raw[offset + 2]
            offset += 3
            name = raw[offset:offset + name_len].decode("utf-8")
            offset += name_len
            result[name] = (channel_id, format_id)
            continue
        if t == 0xF6:
            channel_id = int.from_bytes(raw[offset:offset + 2], "little")
            format_id = raw[offset + 2]
            name_len = raw[offset + 3]
            offset += 4
            name = raw[offset:offset + name_len].decode("utf-8")
            offset += name_len
            result[name] = (channel_id, format_id)
            continue
        break
    return result


def test_compress_chooses_uint16_scaled_by_10(tmp_path):
    in_path = tmp_path / "input.tsdb"
    out_path = tmp_path / "output.tsdb"

    writer = create_timeseries_db_writer(str(in_path))
    writer.addValue("pv.power", 101.9, timestamp_ms=1000)
    writer.addValue("pv.power", 0.0, timestamp_ms=1100)
    writer.addValue("pv.power", 210.0, timestamp_ms=1200)
    writer.close(mark_complete=True)

    selected = compress_timeseries_db_file(str(in_path), str(out_path))
    assert selected["pv.power"] == 0xA1

    db = read_timeseries_db(str(out_path))
    assert db.get_series_values("pv.power") == [(1000, 101.9), (1100, 0.0), (1200, 210.0)]

    format_ids = _extract_channel_format_ids(out_path)
    assert format_ids["pv.power"][1] == 0xA1
    assert out_path.stat().st_size < in_path.stat().st_size


def test_compress_chooses_small_string_format(tmp_path):
    in_path = tmp_path / "input_strings.tsdb"
    out_path = tmp_path / "output_strings.tsdb"

    writer = create_timeseries_db_writer(str(in_path))
    writer.addStringValue("state", "on", timestamp_ms=1000)
    writer.addStringValue("state", "off", timestamp_ms=2000)
    writer.close(mark_complete=True)

    selected = compress_timeseries_db_file(str(in_path), str(out_path))
    assert selected["state"] == 0x08

    db = read_timeseries_db(str(out_path))
    assert db.get_series_values("state") == [(1000, "on"), (2000, "off")]


def test_dump_includes_format_and_abs_rel_timestamps(tmp_path, capsys):
    path = tmp_path / "dump.tsdb"
    writer = create_timeseries_db_writer(str(path))
    writer.addValue("pv.power", 10.5, timestamp_ms=1000)
    writer.addValue("pv.power", 11.0, timestamp_ms=1125)
    writer.addStringValue("state", "ok", timestamp_ms=900)  # forces absolute reset
    writer.close(mark_complete=True)

    db = read_timeseries_db(str(path))
    db.dump()
    out = capsys.readouterr().out

    assert "Series:" in out
    assert "pv.power: format=0x01" in out
    assert "state: format=0x0b (UTF-8 string with uint64_t length prefix)" in out
    assert "ts_abs=1000 (1970-01-01 00:00:01.000) ts_rel=ABS" in out
    assert "ts_abs=1125 (1970-01-01 00:00:01.125) ts_rel=+125" in out
    assert "ts_abs=900 (1970-01-01 00:00:00.900) ts_rel=ABS" in out


def test_generate_demo_data_creates_daily_files_and_yields(tmp_path):
    files = generateDemoData(2, output_dir=str(tmp_path))
    assert len(files) == 2
    assert Path(files[0]).exists()
    assert Path(files[1]).exists()

    first = read_timeseries_db(files[0])
    second = read_timeseries_db(files[1])
    assert "solar/ac/power" in first.list_series()
    assert "solar/ac/yieldday" in first.list_series()
    assert "solar/ac/yieldtotal" in first.list_series()

    yd_first = first.get_series_values("solar/ac/yieldday")
    yd_second = second.get_series_values("solar/ac/yieldday")
    yt_first = first.get_series_values("solar/ac/yieldtotal")
    yt_second = second.get_series_values("solar/ac/yieldtotal")

    assert yd_first[0][1] == pytest.approx(0.0, abs=1e-9)
    assert yd_second[0][1] == pytest.approx(0.0, abs=1e-9)
    assert yd_first[-1][1] >= yd_first[0][1]
    assert yd_second[-1][1] >= yd_second[0][1]
    assert yt_second[-1][1] >= yt_first[-1][1]


def test_cli_dump_db_file_works_with_generated_demo(tmp_path):
    files = generateDemoData(1, output_dir=str(tmp_path))
    db_path = files[0]
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, str(repo_root / "tsdb_collector.py"), "--dump-tsdb", db_path],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stderr == ""
    assert "TimeSeriesDB dump" in result.stdout
    assert "solar/ac/power" in result.stdout


def test_cli_generate_demo_db_creates_files(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, str(repo_root / "tsdb_collector.py"), "--generate-demo-db", "1"],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(tmp_path),
    )
    assert result.returncode == 0
    assert result.stderr == ""
    created = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    assert len(created) == 1
    assert (tmp_path / created[0]).exists()


def test_cli_compress_in_place_with_verbose_stats(tmp_path):
    db_path = tmp_path / "compress_me.tsdb"
    writer = create_timeseries_db_writer(str(db_path))
    for i in range(200):
        writer.addValue("pv.power", 100.0 + ((i % 10) / 10.0), timestamp_ms=1_700_000_000_000 + i * 1000)
    writer.close(mark_complete=True)

    old_size = db_path.stat().st_size
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, str(repo_root / "tsdb_collector.py"), "--compress", str(db_path), "-v"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stderr == ""
    new_size = db_path.stat().st_size
    assert new_size < old_size
    assert "old_size=" in result.stdout
    assert "new_size=" in result.stdout
    assert "gained=" in result.stdout


def test_tsdb_appender_appends_multiple_batches(tmp_path):
    path = tmp_path / "append.tsdb"
    appender = TimeSeriesDbAppender(str(path))
    appender.append_events(
        [
            (1000, "a", 1.5),
            (1000, "b", "x"),
        ]
    )
    appender.append_events(
        [
            (1010, "a", 2.5),
            (1020, "b", "y"),
        ]
    )
    db = read_timeseries_db(str(path))
    assert db.get_series_values("a") == [(1000, 1.5), (1010, 2.5)]
    assert db.get_series_values("b") == [(1000, "x"), (1020, "y")]


def test_cli_collect_requires_subscription(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    cfg_path = tmp_path / "empty.toml"
    cfg_path.write_text("", encoding="utf-8")
    result = subprocess.run(
        [
            sys.executable,
            str(repo_root / "tsdb_collector.py"),
            "--collect",
            "--mqtt-server",
            "127.0.0.1:1883",
            "--config",
            str(cfg_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(tmp_path),
    )
    assert result.returncode == 2
    assert result.stderr == ""
    assert "--collect requires at least one topic via --topics or config" in result.stdout


def test_stat_timeseries_db_counts_bytes(tmp_path):
    path = tmp_path / "stats.tsdb"
    writer = create_timeseries_db_writer(str(path))
    writer.addValue("pv.power", 10.5, timestamp_ms=1000)
    writer.addValue("pv.power", 11.5, timestamp_ms=1100)
    writer.addStringValue("state", "ok", timestamp_ms=1100)
    writer.close(mark_complete=True)

    stats = stat_timeseries_db(str(path))
    assert stats.total_bytes == path.stat().st_size
    assert stats.value_bytes > 0
    assert stats.timestamp_bytes > 0
    assert stats.channel_definition_bytes > 0
    assert stats.other_bytes >= 0
    assert stats.value_bytes + stats.timestamp_bytes + stats.channel_definition_bytes + stats.other_bytes == stats.total_bytes
    by_format = {row.format_id: row for row in stats.per_format}
    assert by_format[0x01].count == 2
    assert by_format[0x01].value_size_text == "8"
    assert by_format[0x0b].count == 1
    assert by_format[0x0b].value_size_text == "10"


def test_cli_stat_tsdb_prints_tables(tmp_path):
    path = tmp_path / "stats_cli.tsdb"
    writer = create_timeseries_db_writer(str(path))
    writer.addValue("pv.power", 10.5, timestamp_ms=1000)
    writer.addStringValue("state", "ok", timestamp_ms=1100)
    writer.close(mark_complete=True)

    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, str(repo_root / "tsdb_collector.py"), "--stat-tsdb", str(path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stderr == ""
    assert "Per-format value bytes:" in result.stdout
    assert "Overall byte usage:" in result.stdout
    assert "0x01" in result.stdout
    assert "0x0b" in result.stdout
    assert "values" in result.stdout
    assert "timestamps" in result.stdout
    assert "channel definitions" in result.stdout


def test_cli_downsample_creates_series_array_variants(tmp_path):
    day = datetime.date(2026, 2, 20)
    path = tmp_path / f"data_{day.isoformat()}.tsdb"
    day_start_ms = int(datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    writer = create_timeseries_db_writer(str(path))
    writer.addValue("pv.power", 10.0, timestamp_ms=day_start_ms + 200)
    writer.addValue("pv.power", 20.0, timestamp_ms=day_start_ms + 1200)
    writer.addValue("pv.power", 30.0, timestamp_ms=day_start_ms + 5200)
    writer.close(mark_complete=True)

    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, str(repo_root / "tsdb_collector.py"), "--downsample", str(path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stderr == ""
    for label in ("1s", "5s", "15s", "1m", "5m", "15m", "1h"):
        out_path = tmp_path / f"dsda_{day.isoformat()}.{label}.tsdb"
        assert out_path.exists()
        db = read_timeseries_db(str(out_path))
        assert "pv.power" in db.list_series()


def test_collector_config_preserves_comment_blocks_roundtrip(tmp_path):
    config_path = tmp_path / "collector.toml"
    config_path.write_text(
        "\n".join(
            [
                "# c_mqtt_server",
                'mqtt_server = "broker:1883"',
                "",
                "# c_data_dir",
                'data_dir = "./data"',
                "",
                "# c_quantize",
                "quantize_timestamps = 100",
                "",
                "# c_topics",
                'topics = ["solar/#", "dtu/#"]',
                "",
                "# c_http_block_1",
                "[[http.sources]]",
                'name = "A"',
                'url = "http://a.local/json"',
                'topic_prefix = "http/a"',
                "interval_ms = 2000",
                "enabled = true",
                "",
                "# c_http_block_2",
                "[[http.sources]]",
                'name = "B"',
                'url = "http://b.local/json"',
                'topic_prefix = "http/b"',
                "interval_ms = 3000",
                "enabled = false",
                "",
                "# c_trailing_end_1",
                "# c_trailing_end_2",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = load_collector_config(str(config_path))
    save_collector_config(str(config_path), cfg)
    text = config_path.read_text(encoding="utf-8")

    assert text.index("# c_mqtt_server") < text.index("mqtt_server = ")
    assert text.index("# c_data_dir") < text.index("data_dir = ")
    assert text.index("# c_quantize") < text.index("quantize_timestamps = ")
    assert text.index("# c_topics") < text.index("topics = ")
    assert "[[http.urls]]" not in text
    assert text.strip().endswith("# c_trailing_end_2")


def test_collector_config_preserves_comment_blocks_after_modify_write(tmp_path):
    config_path = tmp_path / "collector_modify.toml"
    config_path.write_text(
        "\n".join(
            [
                "# c_mqtt_server",
                'mqtt_server = "broker:1883"',
                "",
                "# c_data_dir",
                'data_dir = "./data"',
                "",
                "# c_quantize",
                "quantize_timestamps = 100",
                "",
                "# c_topics",
                'topics = ["solar/#"]',
                "",
                "# c_http_block_1",
                "[[http.sources]]",
                'name = "A"',
                'url = "http://a.local/json"',
                'topic_prefix = "http/a"',
                "interval_ms = 2000",
                "enabled = true",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = load_collector_config(str(config_path))
    cfg["mqtt"]["mqtt_server"] = "new-broker:2883"
    cfg["mqtt"]["quantize_timestamps"] = 250
    cfg["mqtt"]["topics"] = ["solar/ac/#", "solar/dtu/#"]
    cfg["http"]["poll_interval_ms"] = 5000
    save_collector_config(str(config_path), cfg)
    text = config_path.read_text(encoding="utf-8")

    assert text.index("# c_mqtt_server") < text.index('mqtt_server = "new-broker:2883"')
    assert text.index("# c_quantize") < text.index("quantize_timestamps = 250")
    assert text.index("# c_topics") < text.index("topics = [")
    assert '    "solar/ac/#",' in text
    assert '    "solar/dtu/#",' in text
    assert "[[http.urls]]" not in text
    assert "poll_interval_ms = 5000" in text


def test_collector_config_repeated_save_does_not_add_blank_lines_before_trailing_comments(tmp_path):
    config_path = tmp_path / "collector_repeat.toml"
    config_path.write_text(
        "\n".join(
            [
                'mqtt_server = "broker:1883"',
                'data_dir = "."',
                "quantize_timestamps = 0",
                "topics = [",
                '    "solar/#",',
                "]",
                "",
                "# trailing comment",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = load_collector_config(str(config_path))
    save_collector_config(str(config_path), cfg)
    first = config_path.read_text(encoding="utf-8")
    save_collector_config(str(config_path), cfg)
    second = config_path.read_text(encoding="utf-8")

    assert second == first


def test_collector_config_saves_http_values_without_enabled_field(tmp_path):
    config_path = tmp_path / "enabled_only.toml"
    cfg = {
        "mqtt": {"mqtt_server": "broker:1883", "data_dir": ".", "quantize_timestamps": 0, "topics": []},
        "http": {
            "poll_interval_ms": 5000,
            "base_url": "",
            "urls": [
                {
                    "url": "http://host/json",
                    "base_topic": "meter",
                    "values": [
                        {"path": "a.p", "topic": "a/p"},
                        {"path": "a.v", "topic": "a/v"},
                        {"path": "a.t", "topic": "a/t"},
                    ],
                }
            ],
        },
    }
    save_collector_config(str(config_path), cfg)
    text = config_path.read_text(encoding="utf-8")
    assert "enabled =" not in text
    assert 'path = "a.p"' in text
    assert 'path = "a.t"' in text
    assert 'path = "a.v"' in text


def test_collector_config_load_ignores_enabled_field_if_present(tmp_path):
    config_path = tmp_path / "load_values.toml"
    config_path.write_text(
        "\n".join(
            [
                'mqtt_server = "broker:1883"',
                "",
                "[http]",
                "poll_interval_ms = 5000",
                "",
                "[[http.urls]]",
                'url = "http://host/json"',
                'base_topic = "meter"',
                "",
                "[[http.urls.values]]",
                'path = "a.p"',
                'topic = "a/p"',
                "",
                "[[http.urls.values]]",
                'path = "a.v"',
                'topic = "a/v"',
                "enabled = false",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    cfg = load_collector_config(str(config_path))
    values = cfg["http"]["urls"][0]["values"]
    assert values == [{"path": "a.p", "topic": "a/p"}, {"path": "a.v", "topic": "a/v"}]
