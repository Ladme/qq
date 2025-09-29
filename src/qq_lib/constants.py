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

# Indicates whether the job was submitted from shared storage
SHARED_SUBMIT = "QQ_SHARED_SUBMIT"

# Name of the batch system to use.
BATCH_SYSTEM = "QQ_BATCH_SYSTEM"

# ----------------------------
# OTHER CONSTANTS
# ----------------------------

# Format string for dates and times
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

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
