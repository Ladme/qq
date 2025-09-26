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


# ----------------------------
# OTHER CONSTANTS
# ----------------------------

# Format string for dates and times
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Name of the work directory inside the scratch directory
SCRATCH_DIR_INNER = "main"

# SSH connection timeout in seconds
SSH_TIMEOUT = 60
