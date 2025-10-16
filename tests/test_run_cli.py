# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.core.error import QQError, QQRunCommunicationError, QQRunFatalError
from qq_lib.run.cli import CFG, ensureQQEnv, run
from qq_lib.run.runner import QQRunner


def test_ensureQQEnv_raises_if_guard_missing(monkeypatch):
    # remove CFG.env_vars.guard from environment
    monkeypatch.delenv(CFG.env_vars.guard, raising=False)

    with pytest.raises(QQError, match="This script must be run as a qq job"):
        ensureQQEnv()


def test_ensureQQEnv_passes_if_guard_present(monkeypatch):
    monkeypatch.setenv(CFG.env_vars.guard, "1")

    # should not raise
    ensureQQEnv()


def test_run_exits_90_if_not_in_qq_env(monkeypatch):
    runner = CliRunner()

    # remove CFG.env_vars.guard from environment
    monkeypatch.delenv(CFG.env_vars.guard, raising=False)

    result = runner.invoke(run, ["script.sh"])
    assert result.exit_code == 90
    assert "This script must be run as a qq job" in result.output


def test_run_exits_92_if_info_file_env_missing(monkeypatch):
    runner = CliRunner()

    monkeypatch.setenv(CFG.env_vars.guard, "1")
    monkeypatch.delenv(CFG.env_vars.info_file, raising=False)
    monkeypatch.setenv(CFG.env_vars.input_machine, "random.host.org")

    result = runner.invoke(run, ["script.sh"])
    assert result.exit_code == 92
    assert f"'{CFG.env_vars.info_file}'" in result.output
    assert "not set" in result.output


def test_run_exits_92_if_input_machine_env_missing(monkeypatch):
    runner = CliRunner()

    monkeypatch.setenv(CFG.env_vars.guard, "1")
    monkeypatch.setenv(CFG.env_vars.info_file, "/path/to/file")
    monkeypatch.delenv(CFG.env_vars.input_machine, raising=False)

    result = runner.invoke(run, ["script.sh"])
    assert result.exit_code == 92
    assert f"'{CFG.env_vars.input_machine}'" in result.output
    assert "not set" in result.output


def test_run_executes_and_exits_with_script_code(monkeypatch):
    runner = CliRunner()

    monkeypatch.setenv(CFG.env_vars.guard, "1")
    monkeypatch.setenv(CFG.env_vars.info_file, "dummy.qqinfo")
    monkeypatch.setenv(CFG.env_vars.input_machine, "random.host.org")

    dummy_runner = MagicMock()
    dummy_runner.execute.return_value = 2
    dummy_runner.prepare = MagicMock()
    dummy_runner.finalize = MagicMock()

    with patch("qq_lib.run.cli.QQRunner", return_value=dummy_runner):
        result = runner.invoke(run, ["script.sh"])

    dummy_runner.prepare.assert_called_once()
    dummy_runner.execute.assert_called_once()
    dummy_runner.finalize.assert_called_once()
    assert result.exit_code == 2


def test_run_exits_91_on_standard_qqerror(monkeypatch):
    runner = CliRunner()

    monkeypatch.setenv(CFG.env_vars.guard, "1")
    monkeypatch.setenv(CFG.env_vars.info_file, "dummy.qqinfo")
    monkeypatch.setenv(CFG.env_vars.input_machine, "random.host.org")

    # simulate a QQError raised inside runner.execute()
    dummy_runner = MagicMock()
    dummy_runner.execute.side_effect = QQError("standard qq error")
    dummy_runner.prepare = MagicMock()
    dummy_runner.finalize = MagicMock()
    dummy_runner.logFailureAndExit = QQRunner.logFailureAndExit.__get__(dummy_runner)

    with patch("qq_lib.run.cli.QQRunner", return_value=dummy_runner):
        result = runner.invoke(run, ["script.sh"])

    assert result.exit_code == 91
    assert "standard qq error" in result.output


def test_run_exits_92_on_qqrunfatalerror(monkeypatch):
    runner = CliRunner()

    monkeypatch.setenv(CFG.env_vars.guard, "1")
    monkeypatch.setenv(CFG.env_vars.info_file, "dummy.qqinfo")
    monkeypatch.setenv(CFG.env_vars.input_machine, "random.host.org")

    # simulate QQRunner raising QQRunFatalError during initialization
    with patch("qq_lib.run.cli.QQRunner", side_effect=QQRunFatalError("fatal error")):
        result = runner.invoke(run, ["script.sh"])

    assert result.exit_code == 92
    assert "fatal error" in result.output


def test_run_exits_93_on_qqruncommunicationerror(monkeypatch):
    runner = CliRunner()

    monkeypatch.setenv(CFG.env_vars.guard, "1")
    monkeypatch.setenv(CFG.env_vars.info_file, "dummy.qqinfo")
    monkeypatch.setenv(CFG.env_vars.input_machine, "random.host.org")

    # simulate QQRunner.execute raising QQRunCommunicationError
    dummy_runner = MagicMock()
    dummy_runner.execute.side_effect = QQRunCommunicationError("comm error")
    dummy_runner.prepare = MagicMock()
    dummy_runner.finalize = MagicMock()
    dummy_runner.logFailureAndExit = QQRunner.logFailureAndExit.__get__(dummy_runner)

    with patch("qq_lib.run.cli.QQRunner", return_value=dummy_runner):
        result = runner.invoke(run, ["script.sh"])

    # QQRunCommunicationError triggers log_fatal_error_and_exit exit_code = 93
    assert result.exit_code == 93
    assert "comm error" in result.output
