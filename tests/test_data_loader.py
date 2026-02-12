import pytest
from unittest.mock import Mock

from datasentinel.data_loader import (
    get_driver_class,
    get_file_loader,
    load_config,
    load_table_data,
)


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


def test_load_table_data_uses_query():
    spark = Mock()
    reader = Mock()
    spark.read = reader
    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = "df"

    out = load_table_data(
        db_type="postgres",
        connection_string="jdbc:postgresql://localhost:5432/db",
        spark=spark,
        query="SELECT * FROM trade_pricing WHERE tradebook = 'X'",
    )

    assert out == "df"
    calls = [c.args for c in reader.option.call_args_list]
    assert ("url", "jdbc:postgresql://localhost:5432/db") in calls
    assert ("driver", "org.postgresql.Driver") in calls
    assert ("query", "SELECT * FROM trade_pricing WHERE tradebook = 'X'") in calls


def test_load_table_data_uses_dbtable_and_extra_options():
    spark = Mock()
    reader = Mock()
    spark.read = reader
    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = "df"

    out = load_table_data(
        db_type="oracle",
        connection_string="jdbc:oracle:thin:@//host:1521/service",
        spark=spark,
        table_name="TRADE_PRICING",
        jdbc_options={"fetchsize": "1000"},
    )

    assert out == "df"
    calls = [c.args for c in reader.option.call_args_list]
    assert ("dbtable", "TRADE_PRICING") in calls
    assert ("fetchsize", "1000") in calls


def test_load_table_data_requires_exactly_one_source():
    spark = Mock()
    with pytest.raises(ValueError, match="exactly one of table_name or query"):
        load_table_data(
            db_type="postgres",
            connection_string="jdbc:postgresql://localhost:5432/db",
            spark=spark,
            table_name="t",
            query="SELECT * FROM t",
        )
    with pytest.raises(ValueError, match="exactly one of table_name or query"):
        load_table_data(
            db_type="postgres",
            connection_string="jdbc:postgresql://localhost:5432/db",
            spark=spark,
        )
