## Version 0.6.0

### Support for per-node resources
- Number of CPU cores, number of GPUs, the amount of memory and the amount of storage can be now requested per-node using the submission options `ncpus-per-node`, `ngpus-per-node`, `mem-per-node`, and `work-size-per-node`. Per-node properties override per-cpu properties (`mem-per-cpu`, `work-size-per-cpu`) but are overriden by "total" properties (`ncpus`, `ngpus`, `mem`, `work-size`).

### Changes in Gromacs run scripts
- The scripts now by default try to allocate the maximum possible number of MPI ranks.
- Numbers of MPI ranks are now specified per node (in `*_md` scripts) or per client (in `*_re` scripts).

### Bug fixes and minor improvements
- The available types of working directories for the current environment are now shown in the output of `qq submit -h`.
- Fixed a regression from v0.5: missing size property in `qq nodes` is now correctly intepreted as zero size.
- When a job is killed, runtime files are copied to the input directory only after the executed process finishes.
- Changed the way working directories on Karolina and LUMI are created allowing their complete removal.
- Collection of Slurm jobs (which is complicated by Slurm's architecture) is now performed in parallel and is consequently much faster.

### Internal changes
- `Wiper.delete` method has been renamed to `Wiper.wipe`.
- `Killer.terminate` method has been renamed to `Killer.kill`.
- `SubmitterFactory` no longer requires a list of supported parameters and instead loads it itself.
- Added getter methods to `Submitter`.
- `Submitter` no longer requires to provide the "command line". Command line is no longer written into qq info files.

***

## Version 0.5.1

- If no info file is detected when running `qq go`, `qq info`, `qq kill`, `qq sync`, and `qq wipe`, an error message is printed. (This fixes a regression in v0.5.0.)

## Version 0.5.0

### Support for LUMI
- qq is now fully compatible with the **LUMI** supercomputer.

### Handling of failed and killed jobs
- The `.err` and `.out` runtime files are now copied from the working directory to the input directory even when a job fails or is killed.
  This makes it easier to inspect what went wrong while keeping the input directory in a consistent state — all other files remain in the working directory.

### New command: `qq wipe`
- Added the `qq wipe` command for safely deleting the working directories of failed or killed jobs.

### Slurm step information
- `qq info` now displays the status of individual Slurm job steps when multiple steps exist and the information is available from the batch system.

### Updates to `qq nodes`
- The *Comment* column is now hidden when no queues include a comment.
- Added a new *Max Nodes* column showing the maximum number of nodes that can be requested in each queue. This column is hidden if no queue has a set maximal number of nodes.

### New option: `--include` in `qq submit`
- You can now use the `--include` option to specify additional files or directories outside the job's input directory. These will be copied into the working directory upon submission.

### Bug fixes and minor improvements
- Added support for the `-h` flag as a shorthand for `--help`.
- Added shell autocomplete for qq commands.
- Fixed incorrect naming of loop jobs when the job script had a file extension.
- Made it possible to submit qq jobs from directories other than the current working directory.
- `get_info_files_from_job_id_or_dir` now properly catches `PermissionError` when reading restricted info files.
- Retrieving job lists from Slurm is now significantly faster (still limited by Slurm performance).
- Fixed an issue preventing jobs from using multiple MPI ranks on some PBS clusters.
- Improved the dynamic output of `qq jobs`: unused columns are now hidden.
- Operations on job IDs are now faster.
- Job comments are now shown in the output of `qq jobs -e` and `qq stat -e` (if available).
- `qq sync` now correctly synchronizes contents of selected directories when using the `-f` option.

### Internal changes
- Most methods in `BatchJobInterface`, `BatchQueueInterface`, and `BatchNodeInterface` now have optional return values.

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