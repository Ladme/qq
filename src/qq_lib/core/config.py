# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import os
import tomllib
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, Self


@dataclass
class FileSuffixes:
    """File suffixes used by qq."""

    qq_info: str = ".qqinfo"
    qq_out: str = ".qqout"
    stdout: str = ".out"
    stderr: str = ".err"

    @property
    def all_suffixes(self) -> list[str]:
        """List of all file suffixes."""
        return [self.qq_info, self.qq_out, self.stdout, self.stderr]


@dataclass
class EnvironmentVariables:
    """Environment variable names used by qq."""

    guard: str = "QQ_ENV_SET"
    debug_mode: str = "QQ_DEBUG"
    info_file: str = "QQ_INFO"
    input_machine: str = "QQ_INPUT_MACHINE"
    input_dir: str = "QQ_INPUT_DIR"
    shared_submit: str = "QQ_SHARED_SUBMIT"
    batch_system: str = "QQ_BATCH_SYSTEM"
    loop_current: str = "QQ_LOOP_CURRENT"
    loop_start: str = "QQ_LOOP_START"
    loop_end: str = "QQ_LOOP_END"
    archive_format: str = "QQ_ARCHIVE_FORMAT"
    pbs_scratch_dir: str = "SCRATCHDIR"


@dataclass
class TimeoutSettings:
    """Timeout settings in seconds."""

    ssh: int = 60
    rsync: int = 600


@dataclass
class RunnerSettings:
    """Settings for QQRunner operations."""

    retry_tries: int = 3
    retry_wait: int = 300
    scratch_dir_inner: str = "main"
    sigterm_to_sigkill: int = 5
    subprocess_checks_wait_time: int = 2


@dataclass
class ArchiverSettings:
    """Settings for QQArchiver operations."""

    retry_tries: int = 3
    retry_wait: int = 300


@dataclass
class GoerSettings:
    """Settings for QQGoer operations."""

    wait_time: int = 5


@dataclass
class LoopJobSettings:
    """Settings for qq loop jobs."""

    pattern: str = "+%04d"


@dataclass
class JobsPresenterColors:
    """Color scheme for QQJobsPresenter."""

    main: str = "white"
    secondary: str = "grey70"
    strong_warning: str = "bright_red"
    mild_warning: str = "bright_yellow"


@dataclass
class JobsPresenterSettings:
    """Settings for QQJobsPresenter."""

    max_job_name_length: int = 20
    colors: JobsPresenterColors = field(default_factory=JobsPresenterColors)


@dataclass
class DateFormats:
    """Date and time format strings."""

    standard: str = "%Y-%m-%d %H:%M:%S"
    pbs: str = "%a %b %d %H:%M:%S %Y"


@dataclass
class ExitCodes:
    """Exit codes used for various errors."""

    not_qq_env: int = 90
    default: int = 91
    qq_run_fatal: int = 92
    qq_run_communication: int = 93
    unexpected_error: int = 99


@dataclass
class StateColors:
    """Color scheme for RealState display."""

    queued: str = "bright_magenta"
    held: str = "bright_magenta"
    suspended: str = "bright_black"
    waiting: str = "bright_magenta"
    running: str = "bright_blue"
    booting: str = "bright_cyan"
    killed: str = "bright_red"
    failed: str = "bright_red"
    finished: str = "bright_green"
    exiting: str = "bright_yellow"
    in_an_inconsistent_state: str = "grey70"
    unknown: str = "grey70"


@dataclass
class QQConfig:
    """Main configuration for qq."""

    suffixes: FileSuffixes = field(default_factory=FileSuffixes)
    env_vars: EnvironmentVariables = field(default_factory=EnvironmentVariables)
    timeouts: TimeoutSettings = field(default_factory=TimeoutSettings)
    runner: RunnerSettings = field(default_factory=RunnerSettings)
    archiver: ArchiverSettings = field(default_factory=ArchiverSettings)
    goer: GoerSettings = field(default_factory=GoerSettings)
    loop_jobs: LoopJobSettings = field(default_factory=LoopJobSettings)
    jobs_presenter: JobsPresenterSettings = field(default_factory=JobsPresenterSettings)
    date_formats: DateFormats = field(default_factory=DateFormats)
    exit_codes: ExitCodes = field(default_factory=ExitCodes)
    state_colors: StateColors = field(default_factory=StateColors)
    binary_name: str = "qq"

    @classmethod
    def load(cls, config_path: Path | None = None) -> Self:
        """
        Load configuration from TOML file or use defaults.

        Args:
            config_path: Explicit path to config file. If None, searches standard locations.

        Returns:
            QQConfig instance with loaded or default values.
        """
        if config_path is None:
            config_path = QQConfig._get_config_path()

        try:
            if config_path and config_path.exists():
                with config_path.open("rb") as f:
                    config_data = tomllib.load(f)
                return _dict_to_dataclass(cls, config_data)
        except Exception as e:
            raise ValueError(f"Could not read qq config '{config_path}': {e}.")

        # no config found - use defaults
        return cls()

    @staticmethod
    def _get_config_path() -> Path | None:
        """
        Search for config file in standard locations (XDG compliant).
        Returns the first existing config file, or None.
        """
        config_locations: list[Path] = [
            # 1. Explicit environment variable (highest priority)
            Path(env_path) if (env_path := os.getenv("QQ_CONFIG")) else None,
            # 2. Current working directory (for development/override)
            Path.cwd() / "qq_config.toml",
            # 3. XDG config home (standard user config location)
            Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config"))
            / "qq"
            / "config.toml",
        ]

        for path in config_locations:
            if path and path.is_file():
                return path

        return None


def _dict_to_dataclass(cls, data: dict[str, Any]):
    """
    Recursively convert a dictionary to a dataclass instance.
    Handles nested dataclasses properly.
    """
    if not is_dataclass(cls):
        return data

    field_values = {}
    for field_info in fields(cls):
        field_name = field_info.name
        field_type = field_info.type

        if field_name in data:
            value = data[field_name]
            if is_dataclass(field_type) and isinstance(value, dict):
                field_values[field_name] = _dict_to_dataclass(field_type, value)
            else:
                field_values[field_name] = value

    return cls(**field_values)


# Global configuration for qq.
CFG = QQConfig.load()
