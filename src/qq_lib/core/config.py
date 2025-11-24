# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Configuration system for qq.

This module defines dataclasses representing all configurable aspects of qq,
including file suffixes, environment variables, timeouts, presentation settings,
batch-system options, and global defaults.

The `Config` class loads user configuration from a TOML file (if available)
and provides a globally accessible `CFG` instance.
"""

import os
import tomllib
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, Self


@dataclass
class FileSuffixes:
    """File suffixes used by qq."""

    # Suffix for qq info files.
    qq_info: str = ".qqinfo"
    # Suffix for qq output files.
    qq_out: str = ".qqout"
    # Suffix for captured stdout.
    stdout: str = ".out"
    # Suffix for captured stderr.
    stderr: str = ".err"

    @property
    def all_suffixes(self) -> list[str]:
        """List of all file suffixes."""
        return [self.qq_info, self.qq_out, self.stdout, self.stderr]


@dataclass
class EnvironmentVariables:
    """Environment variable names used by qq."""

    # Indicates job is running inside the qq environment.
    guard: str = "QQ_ENV_SET"
    # Enables qq debug mode.
    debug_mode: str = "QQ_DEBUG"
    # Path to the qq info file for the job.
    info_file: str = "QQ_INFO"
    # Machine from which the job was submitted.
    input_machine: str = "QQ_INPUT_MACHINE"
    # Submission directory path.
    input_dir: str = "QQ_INPUT_DIR"
    # Whether submission was from shared storage.
    shared_submit: str = "QQ_SHARED_SUBMIT"
    # Name of the batch system used.
    batch_system: str = "QQ_BATCH_SYSTEM"
    # Current loop-cycle index.
    loop_current: str = "QQ_LOOP_CURRENT"
    # Starting loop-cycle index.
    loop_start: str = "QQ_LOOP_START"
    # Final loop-cycle index.
    loop_end: str = "QQ_LOOP_END"
    # Non-resubmit flag returned by a job script.
    no_resubmit: str = "QQ_NO_RESUBMIT"
    # Archive filename pattern.
    archive_format: str = "QQ_ARCHIVE_FORMAT"
    # Scratch directory on Metacentrum clusters.
    pbs_scratch_dir: str = "SCRATCHDIR"
    # Slurm account used for the job.
    slurm_job_account: str = "SLURM_JOB_ACCOUNT"
    # Storage type for LUMI scratch.
    lumi_scratch_type: str = "LUMI_SCRATCH_TYPE"
    # Total CPUs used.
    ncpus: str = "QQ_NCPUS"
    # Total GPUs used.
    ngpus: str = "QQ_NGPUS"
    # Total nodes used.
    nnodes: str = "QQ_NNODES"
    # Walltime in hours.
    walltime: str = "QQ_WALLTIME"


@dataclass
class TimeoutSettings:
    """Timeout settings in seconds."""

    # Timeout for SSH in seconds.
    ssh: int = 60
    # Timeout for rsync in seconds.
    rsync: int = 600


@dataclass
class RunnerSettings:
    """Settings for Runner operations."""

    # Maximum number of attempts when retrying an operation.
    retry_tries: int = 3
    # Wait time (in seconds) between retry attempts.
    retry_wait: int = 300
    # Delay (in seconds) between sending SIGTERM and SIGKILL to a job script.
    sigterm_to_sigkill: int = 5
    # Interval (in seconds) between successive checks of the running script's state.
    subprocess_checks_wait_time: int = 2


@dataclass
class ArchiverSettings:
    """Settings for Archiver operations."""

    # Maximum number of attempts when retrying an operation.
    retry_tries: int = 3
    # Wait time (in seconds) between retry attempts.
    retry_wait: int = 300


@dataclass
class GoerSettings:
    """Settings for Goer operations."""

    # Interval (in seconds) between successive checks of the job's state
    # (when waiting for the job to start).
    wait_time: int = 5


@dataclass
class LoopJobSettings:
    """Settings for qq loop jobs."""

    # Pattern used for naming loop jobs.
    pattern: str = "+%04d"


@dataclass
class JobStatusPanelSettings:
    """Settings for creating a job status panel."""

    # Maximal width of the job status panel.
    max_width: int | None = None
    # Minimal width of the job status panel.
    min_width: int | None = 60
    # Style of the border lines.
    border_style: str = "white"
    # Style of the title.
    title_style: str = "white bold"


@dataclass
class FullInfoPanelSettings:
    """Settings for creating a full info panel."""

    # Maximal width of the job info panel.
    max_width: int | None = None
    # Minimal width of the job info panel.
    min_width: int | None = 80
    # Style of the border lines.
    border_style: str = "white"
    # Style of the title.
    title_style: str = "white bold"
    # Style of the separators between individual sections of the panel.
    rule_style: str = "white"


@dataclass
class PresenterSettings:
    """Settings for Presenter."""

    # Settings for the job status panel
    job_status_panel: JobStatusPanelSettings = field(
        default_factory=JobStatusPanelSettings
    )

    # Settings for the job info panel
    full_info_panel: FullInfoPanelSettings = field(
        default_factory=FullInfoPanelSettings
    )

    # Style used for the keys in job status/info panel.
    key_style: str = "default bold"
    # Style used for values in job status/info panel.
    value_style: str = "white"
    # Style used for notes in job status/info panel.
    notes_style: str = "grey50"


@dataclass
class JobsPresenterSettings:
    """Settings for JobsPresenter."""

    # Maximal width of the jobs panel.
    max_width: int | None = None
    # Minimal width of the jobs panel.
    min_width: int | None = 80
    # Maximum displayed length of a job name before truncation.
    max_job_name_length: int = 20
    # Maximum displayed length of working nodes before truncation.
    max_nodes_length: int = 40
    # Style used for border lines.
    border_style: str = "white"
    # Style used for the title.
    title_style: str = "white bold"
    # Style used for table headers.
    headers_style: str = "default"
    # Style used for table values.
    main_style: str = "white"
    # Style used for job statistics.
    secondary_style: str = "grey70"
    # Style used for extra notes.
    extra_info_style: str = "grey50"
    # Style used for strong warning messages.
    strong_warning_style: str = "bright_red"
    # Style used for mild warning messages.
    mild_warning_style: str = "bright_yellow"
    # List of columns to show in the output.
    # If not set, the settings for the current batch system will be used.
    columns_to_show: list[str] | None = None

    # Code used to signify "total jobs".
    sum_jobs_code: str = "Σ"


@dataclass
class QueuesPresenterSettings:
    """Settings for QueuesPresenter."""

    # Maximal width of the queues panel.
    max_width: int | None = None
    # Minimal width of the queues panel.
    min_width: int | None = 80
    # Style used for border lines.
    border_style: str = "white"
    # Style used for the title.
    title_style: str = "white bold"
    # Style used for table headers.
    headers_style: str = "default"

    # Mark used to denote main queues.
    main_mark = "●"
    # Mark used to denote reroutings.
    rerouted_mark = " ··>"

    # Style used for the mark if the queue is available.
    available_mark_style: str = "bright_green"
    # Style used for the mark if the queue is not available.
    unavailable_mark_style: str = "bright_red"
    # Style used for the mark if the queue is dangling.
    dangling_mark_style: str = "bright_yellow"

    # Style used for information about main queues.
    main_text_style: str = "white"
    # Style used for information about reroutings.
    rerouted_text_style: str = "grey50"

    # Code used to signify "other jobs".
    other_jobs_code: str = "O"
    # Code used to signify "total jobs".
    sum_jobs_code: str = "Σ"


@dataclass
class NodesPresenterSettings:
    """Settings for NodesPresenter."""

    # Maximal width of the nodes panel.
    max_width: int | None = None
    # Minimal width of the nodes panel.
    min_width: int | None = 80
    # Maximal width of the shared properties section.
    max_props_panel_width: int = 40
    # Style used for border lines.
    border_style: str = "white"
    # Style used for the title.
    title_style: str = "white bold"
    # Style used for table headers.
    headers_style: str = "default"
    # Style of the separators between individual sections of the panel.
    rule_style: str = "white"
    # Name to use for the leftover nodes that were not assigned to any group.
    others_group_name: str = "other"
    # Name to use for the group if it contains all nodes.
    all_nodes_group_name: str = "all nodes"

    # Mark used to denote nodes.
    state_mark = "●"

    # Style used for main information about the nodes.
    main_text_style: str = "white"
    # Style used for statistics and shared properties.
    secondary_text_style: str = "grey70"
    # Style used for the mark and resources if the node is free.
    free_node_style: str = "bright_green bold"
    # Style used for the mark and resources if the node is partially free.
    part_free_node_style: str = "green"
    # Style used for the mark and resources if the node is busy.
    busy_node_style: str = "blue"
    # Style used for all information about unavailable nodes.
    unavailable_node_style = "bright_red"


@dataclass
class DateFormats:
    """Date and time format strings."""

    # Standard date format used by qq.
    standard: str = "%Y-%m-%d %H:%M:%S"
    # Date format used by PBS Pro.
    pbs: str = "%a %b %d %H:%M:%S %Y"
    # Date format used by Slurm.
    slurm: str = "%Y-%m-%dT%H:%M:%S"


@dataclass
class ExitCodes:
    """Exit codes used for various errors."""

    # Returned when a qq script is run outside the qq environment.
    not_qq_env: int = 90
    # Default error code for failures of qq commands or most errors in the qq environment.
    default: int = 91
    # Returned when a qq job fails and its error state cannot be written to the qq info file.
    qq_run_fatal: int = 92
    # Returned when a qq job fails due to a communication error between qq services.
    qq_run_communication: int = 93
    # Used by job scripts to signal that a loop job should not be resubmitted.
    qq_run_no_resubmit: int = 95
    # Returned on an unexpected or unhandled error.
    unexpected_error: int = 99


@dataclass
class StateColors:
    """Color scheme for RealState display."""

    # Style used for queued jobs.
    queued: str = "bright_magenta"
    # Style used for held jobs.
    held: str = "bright_magenta"
    # Style used for suspended jobs.
    suspended: str = "bright_black"
    # Style used for waiting jobs.
    waiting: str = "bright_magenta"
    # Style used for running jobs.
    running: str = "bright_blue"
    # Style used for booting jobs.
    booting: str = "bright_cyan"
    # Style used for killed jobs.
    killed: str = "bright_red"
    # Style used for failed jobs.
    failed: str = "bright_red"
    # Style used for finished jobs.
    finished: str = "bright_green"
    # Style used for exiting jobs.
    exiting: str = "bright_yellow"
    # Style used for jobs in an inconsistent state.
    in_an_inconsistent_state: str = "grey70"
    # Style used for jobs in an unknown state.
    unknown: str = "grey70"
    # Style used whenever a summary of jobs is provided.
    sum: str = "white"
    # Style used for "other" job states.
    other: str = "grey70"


@dataclass
class SizeOptions:
    """Options associated with the Size dataclass."""

    # Maximal error acceptable when rounding Size values for display.
    max_rounding_error: float = 0.1


@dataclass
class PBSOptions:
    """Options associated with PBS."""

    # Name of the subdirectory inside SCRATCHDIR used as the job's working directory.
    scratch_dir_inner: str = "main"


@dataclass
class SlurmOptions:
    """Options associated with Slurm."""

    # Maximal number of threads used to collect information about jobs using scontrol.
    jobs_scontrol_nthreads: int = 8


@dataclass
class SlurmIT4IOptions:
    """Options associated with Slurm on IT4I clusters."""

    # Number of attempts when preparing a working directory on scratch.
    scratch_dir_attempts: int = 3


@dataclass
class SlurmLumiOptions:
    """Options associated with Slurm on LUMI."""

    # Number of attempts when preparing a working directory on scratch.
    scratch_dir_attempts: int = 3


@dataclass
class Config:
    """Main configuration for qq."""

    suffixes: FileSuffixes = field(default_factory=FileSuffixes)
    env_vars: EnvironmentVariables = field(default_factory=EnvironmentVariables)
    timeouts: TimeoutSettings = field(default_factory=TimeoutSettings)
    runner: RunnerSettings = field(default_factory=RunnerSettings)
    archiver: ArchiverSettings = field(default_factory=ArchiverSettings)
    goer: GoerSettings = field(default_factory=GoerSettings)
    presenter: PresenterSettings = field(default_factory=PresenterSettings)
    loop_jobs: LoopJobSettings = field(default_factory=LoopJobSettings)
    jobs_presenter: JobsPresenterSettings = field(default_factory=JobsPresenterSettings)
    queues_presenter: QueuesPresenterSettings = field(
        default_factory=QueuesPresenterSettings
    )
    nodes_presenter: NodesPresenterSettings = field(
        default_factory=NodesPresenterSettings
    )
    date_formats: DateFormats = field(default_factory=DateFormats)
    exit_codes: ExitCodes = field(default_factory=ExitCodes)
    state_colors: StateColors = field(default_factory=StateColors)
    size: SizeOptions = field(default_factory=SizeOptions)
    pbs_options: PBSOptions = field(default_factory=PBSOptions)
    slurm_options: SlurmOptions = field(default_factory=SlurmOptions)
    slurm_it4i_options: SlurmIT4IOptions = field(default_factory=SlurmIT4IOptions)
    slurm_lumi_options: SlurmLumiOptions = field(default_factory=SlurmLumiOptions)

    # Name of the qq binary.
    binary_name: str = "qq"

    @classmethod
    def load(cls, config_path: Path | None = None) -> Self:
        """
        Load configuration from TOML file or use defaults.

        Args:
            config_path: Explicit path to config file. If None, searches standard locations.

        Returns:
            Config instance with loaded or default values.
        """
        if config_path is None:
            config_path = Config._get_config_path()

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
        config_locations: list[Path | None] = [
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
CFG = Config.load()
