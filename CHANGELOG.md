## Version 0.5.0

### Support for LUMI
- qq can now be used on the LUMI supercomputer.

### qq wipe
- Introduced a new command, `qq wipe`, for safely deleting working directory of failed and killed jobs.

### Slurm steps
- `qq info` now prints information about that states of the individual Slurm job steps, if there is more than one of them and if this information is available from the batch system.

### Bug fixes and other small changes
- Added support for the `-h` flag as a shorthand for `--help` to display the help output.
- Added autocomplete for qq.
- Names for loop jobs are now created correctly even if the job script has a file extension.
- Made it possible to submit qq jobs from directories other than the current working directory.
- `get_info_files_from_job_id_or_dir` now properly catches PermissionError when the user does not have permissions to read an info file.
- Obtaining the list of jobs on Slurm is now much faster (but can be still slow due to Slurm limitations).
- Fixed a bug which caused the job to not be able to use multiple MPI ranks on some PBS clusters.

### Internal changes
- Most of the methods of `BatchJobInterface`, `BatchQueueInterface`, and `BatchNodeInterface` now have an optional return.

***

## Version 0.4.0

### Support for Slurm
- qq can now be used on IT4Innovations clusters with the Slurm batch scheduler.
- A new `qq submit` option, `--account`, has been added to allow submitting jobs on IT4I.

### qq shebang
- Introduced a new command, `qq shebang`, which makes it easier to add the required `qq run` shebang line to your scripts.

### qq jobs/stat flag --extra
- Added a flag `-e`/`--extra` for `qq jobs` and `qq stat`, which makes qq print additional information about each job. Currently, the input machine and input directory are printed (if available), but the list may be expanded in the future.

### More qq collective variables
- The collective variables `QQ_NCPUS` (number of allocated CPU cores), `QQ_NGPUS` (number of allocated GPU cores), `QQ_NNODES` (number of allocated nodes), and `QQ_WALLTIME` (walltime in hours) are now exported to the job environment.

### Bug fixes and other small changes
- When `scratch_shm` or `input_dir` is requested, both `work-size` and `work-size-per-cpu` properties are now properly removed from the list of resources and are no longer displayed in the output of `qq info`.
- Fixed occasional SSH authentication failures by explicitly enabling GSSAPI authentication.
- Fixed current cycle identification in loop jobs. Only a partial match in archived files is now required to consider them.
- Jobs obtained using `qq jobs` and `qq stat` are now always sorted by job ID.
- The number of queued jobs shown in the output of `qq queues` now always includes both queued and held jobs. The column title was changed to 'QH' to reflect this.

### Internal changes
- Refactored the loading of the YAML Dumper and SafeLoader.
- Removed the 'QQ' prefix from all custom class names (excluding errors).

***

## Version 0.3.0

- Added support for manually disabling automatic resubmission in loop jobs by returning the value of the `QQ_NO_RESUBMIT` environment variable from within the job script.

***

## Version 0.2.1

### Bug fixes
- Fixed a bug that prevented files from being rsynced when the user’s group differed between the computing node and the filesystem containing the input directory.

### Internal changes
- Renamed PBSJobInfo to PBSJob.
- Set up GitHub Actions to take care of releases.