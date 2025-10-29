## Version 0.2.1

### Bug fixes
- Fixed a bug that prevented files from being rsynced when the user’s group differed between the computing node and the filesystem containing the input directory.

### Internal changes
- Renamed PBSJobInfo to PBSJob.
- Set up GitHub Actions to take care of releases.