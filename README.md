# kingfisher-archive

This script moves data files written by [Kingfisher Collect](https://kingfisher-collect.readthedocs.io/en/latest/) to an archive server, after they have been loaded into [Kingfisher Process](https://kingfisher-process.readthedocs.io/en/latest/). The data is archived in case we need to re-load it in future.

## Setup

As presently written, the script needs to be run by a user that has:

* Access to the ``collection`` table managed by Kingfisher Process (e.g. with the ``.pgpass`` file)
* Permission to read and delete the data files written by Kingfisher Collect (e.g. with sudo access)
* SSH access to the archive server (e.g. with SSH keys)

The script should be scheduled to run daily. Only one instance of the script should run at once. The output can be piped to a log file. For example:

```
0 1 * * * cd /home/archive/ocdskingfisherarchive; ./rsync-downloaded-files.sh  >> /home/archive/logs/rsync-downloaded-files.log 2>&1
```

The archive server simply needs a ``/home/archive/data`` directory to which the SSH user can write.

## Development

LZ4 is used, for fast compression and decompression. (7z is better for size – and faster than xz – but storage is cheap whereas our time is not.)
