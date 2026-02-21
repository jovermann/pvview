import pytest
import sys
import subprocess
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tsdb_collector import (
    TimeSeriesDbAppender,
    compress_timeseries_db_file,
    create_timeseries_db_writer,
    generateDemoData,
    load_collector_config,
    read_timeseries_db,
    save_collector_config,
)


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
    files = generateDemoData(2, output_dir=str(tmp_path), data_txt_path=str(Path(__file__).resolve().parents[1] / "data.txt"))
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
    files = generateDemoData(1, output_dir=str(tmp_path), data_txt_path=str(Path(__file__).resolve().parents[1] / "data.txt"))
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
    first_http = text.index("[[http.sources]]")
    assert text.index("# c_http_block_1") < first_http
    second_http = text.index("[[http.sources]]", first_http + 1)
    assert text.index("# c_http_block_2") < second_http
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
    cfg["http"]["sources"][0]["interval_ms"] = 5000
    save_collector_config(str(config_path), cfg)
    text = config_path.read_text(encoding="utf-8")

    assert text.index("# c_mqtt_server") < text.index('mqtt_server = "new-broker:2883"')
    assert text.index("# c_quantize") < text.index("quantize_timestamps = 250")
    assert text.index("# c_topics") < text.index("topics = [")
    assert '    "solar/ac/#",' in text
    assert '    "solar/dtu/#",' in text
    assert text.index("# c_http_block_1") < text.index("[[http.sources]]")
    assert "interval_ms = 5000" in text


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
