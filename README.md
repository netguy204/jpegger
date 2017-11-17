# jpegger
## Automatic photo organization and de-duplication

jpegger is a tool I wrote for myself to clean up large collections of images with duplicates and inconsistent organization.

jpegger iterates through a directory and copies (actually hard-links) what it finds into a new directory structure.

Files are placed in a directory according to the the date they were taken. Files retain their previous name unless that name would conflict with a file that is already in the directory.

Files that have already been copied (as determined by the SHA256 hash of their contents) are not copied again.

Usage:

```
./jpegger input_dir output_dir
```

More information can be found at:
```
./jpegger --help
```
