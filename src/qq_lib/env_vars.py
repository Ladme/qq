# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# Specifies whether we are working inside a qq environment.
GUARD = "QQ_ENV_SET"

# Specifies absolute path to the directory from which the job was submitted.
JOBDIR = "QQ_JOBDIR"

# Specifies the absolute path to the directory where the job is running.
WORKDIR = "QQ_WORKDIR"

# Specifies whether debug logs should be printed.
DEBUG_MODE = "QQ_DEBUG"

# Specifies the path to file collecting standard output during the job execution.
STDOUT_FILE = "QQ_STDOUT"

# Specifies the path to file collecting standard error output during the job execution.
STDERR_FILE = "QQ_STDERR"

# Specifies absolute path to file collecting information about the qq job (inside job directory).
INFO_FILE = "QQ_INFO"
