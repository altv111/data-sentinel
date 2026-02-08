import pytest

from datasentinel.data_loader import get_driver_class, get_file_loader, load_config


def test_get_driver_class_known():
    assert get_driver_class("oracle") == "oracle.jdbc.driver.OracleDriver"
    assert get_driver_class("postgres") == "org.postgresql.Driver"
    assert get_driver_class("hive") == "org.apache.hive.jdbc.HiveDriver"


def test_get_driver_class_unknown_returns_none():
    assert get_driver_class("sqlite") is None


def test_get_file_loader_unknown():
    with pytest.raises(ValueError, match="Unsupported file format"):
        get_file_loader("xls")


def test_load_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("steps:\n  - name: demo\n    type: load\n")
    config = load_config(str(config_file))
    assert config["steps"][0]["name"] == "demo"
