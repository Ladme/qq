# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: F401

"""
This module defines the `Runner` class and related helpers that manage the
execution of qq jobs within a batch system. It is invoked internally through
the `qq run` command, which is hidden from the user-facing CLI.

Lifecycle of a qq job:
    1. Working directory preparation
       - Shared storage jobs: The working directory is set to the job
         submission directory itself.
       - Scratch-using jobs: A dedicated scratch directory (created by the
         batch system) is used as a working directory. Job files are copied
         to a specific directory inside the working directory.

    2. Execution
       The qq info file is updated to record the "running" state.
       The job script is executed.

    3. Finalization
       - On success:
         - The qq info file is updated to "finished".
         - If running on scratch, job files are copied back to the submission
           (job) directory and then removed from scratch.
       - On failure:
         - The qq info file is updated to "failed".
         - If on scratch, files are left in place for debugging.

    X. Cleanup (on interruption)
       If the process receives a SIGTERM, the runner updates the qq info file
       to "killed", attempts to gracefully terminate the subprocess, and forces
       termination with SIGKILL if necessary.

Summary:
    - Shared-storage jobs execute directly in the job directory, with no
      file copying.
    - Scratch-using jobs copy job files to scratch, execute there, and then
      either copy results back (on success) or leave scratch data intact (on
      failure).
"""

from .cli import run
from .runner import Runner
