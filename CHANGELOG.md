## Version 0.4.0

### Support for Slurm
- qq can be now used on IT4Innovations clusters with the Slurm batch scheduler.
- A new `qq submit` option `--account` has been added to allow submitting jobs on IT4I.

### qq shebang
- Introduced a new command, `qq shebang`, which makes it easier to add the required `qq run` shebang line to your scripts.

### Bug fixes and other small changes
- When `scratch_shm` or `input_dir` is requested, both `work-size` and `work-size-per-cpu` properties are now properly removed from the list of resources and are no longer displayed in the output of qq info.
- Fixed current cycle identification in loop jobs. Only a partial match in archived files is now required to consider them.

### Internal changes
- Refactored loading of yaml Dumper and SafeLoader.

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