Setup
=====

As presently written, the script needs to be run by a user that has:

-  Access to the ``collection`` table managed by Kingfisher Process
-  Permission to read and delete the data and log files written by Kingfisher Collect (with sudo access under a user called ``ocdskfs``)
-  Read & Write access to a S3 bucket to hold the archives

One install of Kingfisher should back up to one S3 bucket only. There are algorithms to decide whether to back something up based on what has already been backed up, and these may get confused if 2 Kingfisher installs try to back up to the same S3 bucket.

Configuration
-------------

The application can be configured using environment variables, or a ``.env`` file.

The relevant environment variables are:

KINGFISHER_ARCHIVE_BUCKET_NAME
  The Amazon S3 bucket name 
KINGFISHER_ARCHIVE_DATA_DIRECTORY
  Kingfisher Collect's FILES_STORE directory, e.g. ``scrapyd/data``
KINGFISHER_ARCHIVE_LOGS_DIRECTORY
  Kingfisher Collect's project directory within Scrapyd's logs_dir directory, e.g. ``scrapyd/logs/kingfisher``
KINGFISHER_ARCHIVE_DATABASE_FILE
  The SQLite database for caching the local state (defaults to db.sqlite3)
KINGFISHER_ARCHIVE_DATABASE_URL
  Kingfisher Process' database URL
SENTRY_DSN
  Sentry.io's Data Source Name (DSN) (optional)

As linked from :doc:`s3`, you can also set:

AWS_ACCESS_KEY_ID
  The Amazon user's access key ID
AWS_SECRET_ACCESS_KEY
  The Amazon user's secret access key

Logging configuration (optional)
--------------------------------

This should be placed at ``~/.config/ocdskingfisher-archive/logging.json``.

Its contents should be standard Python logging configuration in JSON - for more see https://docs.python.org/3/library/logging.config.html#logging-config-dictschema

To download the default configuration:

.. code-block:: shell-session

   curl https://raw.githubusercontent.com/open-contracting/kingfisher-archive/master/samples/logging.json -o ~/.config/ocdskingfisher-archive/logging.json

To download a different configuration that includes debug messages:

.. code-block:: shell-session

   curl https://raw.githubusercontent.com/open-contracting/kingfisher-archive/master/samples/logging-debug.json -o ~/.config/ocdskingfisher-archive/logging.json
