# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from dataclasses import dataclass, field

import pytest

from qq_lib.core.config import QQConfig, _dict_to_dataclass


def test_dict_to_dataclass_simple_conversion():
    @dataclass
    class SimpleConfig:
        name: str = "default"
        count: int = 0

    data = {"name": "test", "count": 42}
    result = _dict_to_dataclass(SimpleConfig, data)

    assert isinstance(result, SimpleConfig)
    assert result.name == "test"
    assert result.count == 42


def test_dict_to_dataclass_nested_conversion():
    @dataclass
    class Inner:
        value: int = 0

    @dataclass
    class Outer:
        inner: Inner = field(default_factory=Inner)
        name: str = "default"

    data = {"inner": {"value": 99}, "name": "outer"}
    result = _dict_to_dataclass(Outer, data)

    assert isinstance(result, Outer)
    assert isinstance(result.inner, Inner)
    assert result.inner.value == 99
    assert result.name == "outer"


def test_dict_to_dataclass_partial_data_uses_defaults():
    @dataclass
    class Config:
        required: str = "default"
        optional: int = 42

    data = {"required": "provided"}
    result = _dict_to_dataclass(Config, data)

    assert result.required == "provided"
    assert result.optional == 42


def test_dict_to_dataclass_extra_fields_ignored():
    @dataclass
    class Config:
        valid: str = "default"

    data = {"valid": "value", "invalid": "ignored"}
    result = _dict_to_dataclass(Config, data)

    assert result.valid == "value"
    assert not hasattr(result, "invalid")


def test_dict_to_dataclass_non_dataclass_returns_unchanged():
    data = {"key": "value"}
    result = _dict_to_dataclass(str, data)
    assert result == data


def test_get_config_path_env_variable_highest_priority(tmp_path, monkeypatch):
    config_file = tmp_path / "custom_config.toml"
    config_file.write_text("")

    # create other config files
    cwd_config = tmp_path / "qq_config.toml"
    cwd_config.write_text("")

    monkeypatch.setenv("QQ_CONFIG", str(config_file))
    monkeypatch.chdir(tmp_path)

    result = QQConfig._get_config_path()
    assert result == config_file


def test_get_config_path_current_directory_second_priority(tmp_path, monkeypatch):
    config_file = tmp_path / "qq_config.toml"
    config_file.write_text("")

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("QQ_CONFIG", raising=False)

    # set unused xdg_config
    xdg_config = tmp_path / "config"
    qq_dir = xdg_config / "qq"
    qq_dir.mkdir(parents=True)
    unused_config_file = qq_dir / "config.toml"
    unused_config_file.write_text("")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg_config))

    result = QQConfig._get_config_path()
    assert result == config_file


def test_get_config_path_xdg_config_home_third_priority(tmp_path, monkeypatch):
    xdg_config = tmp_path / "config"
    qq_dir = xdg_config / "qq"
    qq_dir.mkdir(parents=True)
    config_file = qq_dir / "config.toml"
    config_file.write_text("")

    other_dir = tmp_path / "other"
    other_dir.mkdir()

    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg_config))
    monkeypatch.chdir(other_dir)
    monkeypatch.delenv("QQ_CONFIG", raising=False)

    result = QQConfig._get_config_path()
    assert result == config_file


def test_get_config_path_returns_none_when_no_config_exists(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("QQ_CONFIG", raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "nonexistent"))

    result = QQConfig._get_config_path()
    assert result is None


def test_load_with_explicit_path(tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text("""
binary_name = "qqd"

[timeouts]
ssh = 120
rsync = 1200

[runner]
retry_tries = 5

[exit_codes]
default = 100
""")

    config = QQConfig.load(config_file)

    assert config.exit_codes.default == 100
    assert config.binary_name == "qqd"
    assert config.timeouts.ssh == 120
    assert config.timeouts.rsync == 1200
    assert config.runner.retry_tries == 5

    # non-overriden values
    assert config.runner.sigterm_to_sigkill == 5
    assert config.runner.retry_wait == 300


def test_load_nested_dataclass(tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text("""
[jobs_presenter]
max_job_name_length = 50

[jobs_presenter.colors]
main = "cyan"
strong_warning = "red"
""")

    config = QQConfig.load(config_file)

    assert config.jobs_presenter.max_job_name_length == 50
    assert config.jobs_presenter.colors.main == "cyan"
    assert config.jobs_presenter.colors.strong_warning == "red"

    assert config.jobs_presenter.colors.secondary == "grey70"


def test_load_returns_defaults_when_file_missing(tmp_path):
    non_existent = tmp_path / "does_not_exist.toml"

    config = QQConfig.load(non_existent)
    default_config = QQConfig()

    assert config == default_config


def test_load_without_path_searches_standard_locations(tmp_path, monkeypatch):
    config_file = tmp_path / "qq_config.toml"
    config_file.write_text("""
binary_name = "qqd"
""")

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("QQ_CONFIG", raising=False)

    config = QQConfig.load()

    assert config.binary_name == "qqd"


def test_load_partial_config_preserves_defaults(tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text("""
[runner]
retry_tries = 10
""")

    config = QQConfig.load(config_file)

    # overriden value
    assert config.runner.retry_tries == 10
    # all other defaults intact
    assert config.runner.retry_wait == 300
    assert config.archiver.retry_tries == 3
    assert config.timeouts.ssh == 60
    assert config.suffixes.qq_info == ".qqinfo"


def test_load_empty_config_file_uses_all_defaults(tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text("")

    config = QQConfig.load(config_file)
    default_config = QQConfig()

    assert config == default_config


def test_load_large_config_override(tmp_path):
    """Test loading a complete configuration that overrides all values."""
    config_file = tmp_path / "config.toml"
    config_file.write_text("""
binary_name = "qqd"

[suffixes]
qq_info = ".info"
qq_out = ".output"
stdout = ".stdout"
stderr = ".stderr"

[timeouts]
ssh = 90
rsync = 900

[runner]
retry_tries = 7
retry_wait = 500
sigterm_to_sigkill = 10

[archiver]
retry_tries = 4
retry_wait = 400

[goer]
wait_time = 10

[loop_jobs]
pattern = "+%05d"

[date_formats]
standard = "%d/%m/%Y %H:%M"
pbs = "%Y-%m-%d %H:%M:%S"

[jobs_presenter]
max_job_name_length = 40

[jobs_presenter.colors]
main = "blue"
secondary = "gray"
strong_warning = "magenta"
mild_warning = "yellow"

[exit_codes]
unexpected_error = 150
""")

    config = QQConfig.load(config_file)

    assert config.binary_name == "qqd"
    assert config.suffixes.qq_info == ".info"
    assert config.timeouts.ssh == 90
    assert config.runner.retry_tries == 7
    assert config.archiver.retry_wait == 400
    assert config.goer.wait_time == 10
    assert config.loop_jobs.pattern == "+%05d"
    assert config.date_formats.standard == "%d/%m/%Y %H:%M"
    assert config.jobs_presenter.max_job_name_length == 40
    assert config.jobs_presenter.colors.main == "blue"
    assert config.exit_codes.unexpected_error == 150


def test_load_with_invalid_toml_raises_error(tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text("""
[runner
retry_tries = 5
""")  # missing closing bracket

    with pytest.raises(ValueError, match="Could not read qq config"):
        QQConfig.load(config_file)
