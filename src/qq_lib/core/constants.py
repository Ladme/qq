# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ----------------------------
# SUFFIXES
# ----------------------------

# Suffix for qq info files
QQ_INFO_SUFFIX = ".qqinfo"

# Suffix for qq output files
QQ_OUT_SUFFIX = ".qqout"

# Suffix for standard output from script execution
STDOUT_SUFFIX = ".out"

# Suffix for standard error output from script execution
STDERR_SUFFIX = ".err"

# List of all file suffixes used by qq
QQ_SUFFIXES = [QQ_INFO_SUFFIX, QQ_OUT_SUFFIX, STDOUT_SUFFIX, STDERR_SUFFIX]


# ----------------------------
# ENVIRONMENT VARIABLES
# ----------------------------

# Indicates whether we are inside a qq environment
GUARD = "QQ_ENV_SET"

# Enables printing of debug logs
DEBUG_MODE = "QQ_DEBUG"

# Absolute path to the file storing qq job information
INFO_FILE = "QQ_INFO"

# Name of the machine used to submit the qq job
INPUT_MACHINE = "QQ_INPUT_MACHINE"

# Absolute path to the directory from which the job was submitted.
INPUT_DIR = "QQ_INPUT_DIR"

# Indicates whether the job was submitted from shared storage
SHARED_SUBMIT = "QQ_SHARED_SUBMIT"

# Name of the batch system to use.
BATCH_SYSTEM = "QQ_BATCH_SYSTEM"

# The number of the current cycle of a loop job.
LOOP_CURRENT = "QQ_LOOP_CURRENT"

# The number of the starting cycle of a loop job.
LOOP_START = "QQ_LOOP_START"

# The number of the last cycle of a loop job.
LOOP_END = "QQ_LOOP_END"

# The format used for archived data.
ARCHIVE_FORMAT = "QQ_ARCHIVE_FORMAT"

# ----------------------------
# OTHER CONSTANTS
# ----------------------------

# Format string for dates and times
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Format string for dates and times used by PBS
PBS_DATE_FORMAT = "%a %b %d %H:%M:%S %Y"

# Name of the work directory inside the scratch directory
SCRATCH_DIR_INNER = "main"

# SSH connection timeout in seconds
SSH_TIMEOUT = 60

# RSYNC operation timeout in seconds
# (i.e., copying files to/from work_dir cannot take longer than this time)
RSYNC_TIMEOUT = 600

# Maximum number of retry attempts for operations in QQRunner.
RUNNER_RETRY_TRIES = 3

# Wait time in seconds between retry attempts in QQRunner.
RUNNER_RETRY_WAIT = 300

# Time in seconds between sending a SIGTERM signal to the running process and sending a SIGKILL signal
RUNNER_SIGTERM_TO_SIGKILL = 5

# Time in seconds to wait before rechecking job state when queued.
GOER_WAIT_TIME = 5

# Maximum number of retry attempts for operations in QQArchiver.
ARCHIVER_RETRY_TRIES = 3

# Wait time in seconds between retry attempts in QQArchiver.
ARCHIVER_RETRY_WAIT = 300

# Pattern used for numbering qq loop jobs.
LOOP_JOB_PATTERN = "+%04d"

# Main color used in QQJobsPresenter
JOBS_PRESENTER_MAIN_COLOR = "white"

# Secondary color used in QQJobsPresenter
JOBS_PRESENTER_SECONDARY_COLOR = "grey70"

# Color used to indicate a strong warning in QQJobsPresenter.
JOBS_PRESENTER_STRONG_WARNING_COLOR = "bright_red"

# Color used to indicate a mild warning in QQJobsPresenter.
JOBS_PRESENTER_MILD_WARNING_COLOR = "bright_yellow"
