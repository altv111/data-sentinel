import pytest

from datasentinel import cli


def test_cli_requires_config_path(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["datasentinel"])  # no config path
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "Usage: datasentinel <yaml_config_path>" in captured.out
