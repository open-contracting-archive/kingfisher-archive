# kingfisher-archive

This moves data and log files written by [Kingfisher Collect](https://kingfisher-collect.readthedocs.io/en/latest/) to an archive server, after they have been loaded into [Kingfisher Process](https://kingfisher-process.readthedocs.io/en/latest/). 

The data is archived in case we need to re-load it in future.

## Setup

As presently written, the script needs to be run by a user that has:

* Access to the ``collection`` table managed by Kingfisher Process (e.g. with the ``.pgpass`` file)
* Permission to read and delete the data files written by Kingfisher Collect (e.g. with sudo access)

## Run

    python manage.py archive

To see options

    python manage.py archive --help

