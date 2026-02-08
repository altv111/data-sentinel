import os
import pytest

from datasentinel.paths import PathResolver


def test_resolve_input_absolute_path(tmp_path):
    abs_path = tmp_path / "data.csv"
    resolver = PathResolver(results_home=None, input_home=None)
    assert resolver.resolve_input(str(abs_path)) == str(abs_path)


def test_resolve_input_relative_with_input_home(tmp_path):
    input_home = tmp_path / "inputs"
    resolver = PathResolver(results_home=None, input_home=str(input_home))
    assert resolver.resolve_input("file.csv") == os.path.join(str(input_home), "file.csv")


def test_resolve_input_relative_without_input_home_fallback_true(tmp_path):
    resolver = PathResolver(results_home=None, input_home=None)
    rel_path = "file.csv"
    with pytest.MonkeyPatch().context() as mp:
        mp.chdir(tmp_path)
        resolved = resolver.resolve_input(rel_path, allow_cwd_fallback=True)
    assert resolved == os.path.join(str(tmp_path), rel_path)


def test_resolve_input_relative_without_input_home_fallback_false():
    resolver = PathResolver(results_home=None, input_home=None)
    with pytest.raises(ValueError, match="SENTINEL_INPUT_HOME"):
        resolver.resolve_input("file.csv", allow_cwd_fallback=False)


def test_output_base_dir_defaults_to_cwd(tmp_path):
    resolver = PathResolver(results_home=None, input_home=None)
    with pytest.MonkeyPatch().context() as mp:
        mp.chdir(tmp_path)
        assert resolver.output_base_dir() == str(tmp_path)


def test_output_base_dir_with_results_home(tmp_path):
    resolver = PathResolver(results_home=str(tmp_path), input_home=None)
    assert resolver.output_base_dir() == str(tmp_path)
