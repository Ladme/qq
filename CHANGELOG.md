## Version 0.4.0

### Internal changes
- Refactored loading of yaml Dumper and SafeLoader.

## Version 0.3.0

- Added support for manually disabling automatic resubmission in loop jobs by returning the value of the `QQ_NO_RESUBMIT` environment variable from within the job script.

***

## Version 0.2.1

### Bug fixes
- Fixed a bug that prevented files from being rsynced when the user’s group differed between the computing node and the filesystem containing the input directory.

### Internal changes
- Renamed PBSJobInfo to PBSJob.
- Set up GitHub Actions to take care of releases.