# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# SUFFIXES

# suffix used for qq info files
QQ_INFO_SUFFIX = ".qqinfo"

# suffix used for qq output files
QQ_OUT_SUFFIX = ".qqout"

# suffix used for standard output
# from the script execution
STDOUT_SUFFIX = ".out"

# suffix used for standard error output
# from the script execution
STDERR_SUFFIX = ".err"

# all file suffixes used by qq
QQ_SUFFIXES = [QQ_INFO_SUFFIX, QQ_OUT_SUFFIX, STDOUT_SUFFIX, STDERR_SUFFIX]

# ENVIRONMENT VARIABLES

# Specifies whether we are working inside a qq environment.
GUARD = "QQ_ENV_SET"

# Specifies whether debug logs should be printed.
DEBUG_MODE = "QQ_DEBUG"

# Specifies absolute path to file collecting information about the qq job (inside job directory).
INFO_FILE = "QQ_INFO"

# OTHER CONSTANTS

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
