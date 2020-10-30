Setup
=====

The application needs to be run by a user that has:

-  Read and write access to the data and log files written by `Kingfisher Collect <https://kingfisher-collect.readthedocs.io/en/latest/>`__
-  Read and write access to an Amazon S3 bucket, to which to move the files

Each deployment of this application should be related to a distinct instance of Kingfisher Collect and should move files to a distinct bucket.

Amazon S3
---------

Create a bucket
~~~~~~~~~~~~~~~

#. Open the `S3 dashboard <https://s3.console.aws.amazon.com/s3/home>`__
#. Click the *Create bucket* button

   #. Set *Bucket name*
   #. Click the *Create* button

Create a policy
~~~~~~~~~~~~~~~

#. Open the `IAM dashboard <https://console.aws.amazon.com/iam/home>`__
#. Click *Policies*
#. Click the *Create policy* button

   #. Click *Choose a service*
   #. Filter for and click *S3*
   #. Filter for and check *List Bucket*, *GetObject*, *PutObject* and *DeleteObject*
   #. Expand *Resources*
   #. Click *Add ARN* next to *bucket*
   #. Set *Bucket name* to the bucket created earlier
   #. Click the *Add* button
   #. Click *Add ARN* next to *object*
   #. Set *Bucket name* to the bucket created earlier
   #. Check *Any* next to *Object name*
   #. Click the *Review policy* button
   #. Set *Name*
   #. Click the *Create policy* button

Create a user
~~~~~~~~~~~~~

#. Open the `IAM dashboard <https://console.aws.amazon.com/iam/home>`__
#. Click *Users*
#. Click the *Add user* button

   #. Set *User name*
   #. Check *Programmatic access* next to *Access type*
   #. Click the *Next: Permissions* button
   #. Click the *Attach existing policies directly* panel
   #. Filter for and check the policy created earlier
   #. Click the *Next: Tags* button
   #. Click the *Next: Review* button
   #. Click the *Create user* button

#. Copy the access key ID and secret access key

Kingfisher Archive
------------------

The application can be configured using environment variables, or a ``.env`` file.

The relevant environment variables are:

KINGFISHER_ARCHIVE_BUCKET_NAME
  The Amazon S3 bucket name 
KINGFISHER_ARCHIVE_DATA_DIRECTORY
  Kingfisher Collect's ``FILES_STORE`` directory, e.g. ``scrapyd/data``
KINGFISHER_ARCHIVE_LOGS_DIRECTORY
  Kingfisher Collect's project directory within Scrapyd's ``logs_dir`` directory, e.g. ``scrapyd/logs/kingfisher``
KINGFISHER_ARCHIVE_CACHE_FILE
  The SQLite database for caching the local state (defaults to cache.sqlite3)
KINGFISHER_ARCHIVE_LOGGING_CONFIG_FILE
  A JSON file following `Python's logging configuration dictionary schema <https://docs.python.org/3/library/logging.config.html#logging-config-dictschema>`__
SENTRY_DSN
  Sentry.io's Data Source Name (DSN) (optional)
AWS_ACCESS_KEY_ID
  The Amazon user's access key ID
AWS_SECRET_ACCESS_KEY
  The Amazon user's secret access key

The ``.env`` file would look like:

.. code-block:: none

   KINGFISHER_ARCHIVE_BUCKET_NAME=my-bucket
   KINGFISHER_ARCHIVE_DATA_DIRECTORY=scrapyd/data
   KINGFISHER_ARCHIVE_LOGS_DIRECTORY=scrapyd/logs/kingfisher
   KINGFISHER_ARCHIVE_CACHE_FILE=/home/my-user/cache.sqlite3
   SENTRY_DSN=https://xxx@xxx.ingest.sentry.io/xxx
   AWS_ACCESS_KEY_ID=xxx
   AWS_SECRET_ACCESS_KEY=xxx

Alternatively, you can set the AWS credentials in a `~/.aws/config file <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html>`__.
