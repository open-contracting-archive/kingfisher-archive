Setup
=====

As presently written, the script needs to be run by a user that has:

-  Access to the ``collection`` table managed by Kingfisher Process (e.g. with the ``.pgpass`` file)
-  Permission to read and delete the data and log files written by Kingfisher Collect (with sudo access under a user called ``ocdskfs``)
-  Read & Write access to a S3 bucket to hold the archives

One install of Kingfisher should back up to one S3 bucket only. There are algorithms to decide whether to back something up based on what has already been backed up, and these may get confused if 2 Kingfisher installs try to back up to the same S3 bucket.

Config File
-----------

The config file should be placed at ``~/.config/ocdskingfisher-archive/config.ini``.

Download the sample main configuration file:

.. code-block:: shell-session

   curl https://raw.githubusercontent.com/open-contracting/kingfisher-archive/master/samples/config.ini -o ~/.config/ocdskingfisher-archive/config.ini

PostgreSQL
~~~~~~~~~~

Configure the database connection settings:

.. code-block:: ini

   [DBHOST]
   HOSTNAME = localhost
   PORT = 5432
   USERNAME = ocdskingfisher
   PASSWORD = 
   DBNAME = ocdskingfisher

If you prefer not to store the password in config.ini, you can use the PostgreSQL Password File, ``~/.pgpass``, which overrides any password in config.ini.

AWS S3
~~~~~~

The bucket name should be set here.

.. code-block:: ini

   [S3]
   BUCKETNAME = backups-go-here

For setting access credentials, see a later section.

Directories on disk
~~~~~~~~~~~~~~~~~~~

.. code-block:: ini

   [DIRECTORIES]
   DATA = /scrapyd/data
   LOGS = /scrapyd/logs

Note these should be directories that directly contain directories with names of sources/spiders. On your system there may be a directory for the name of the project in Scrapyd. If so, that should be included in these paths.

For instance, if you have log files like ``/scrapyd/logs/kingfisher/scotland/3487b29602b311ebaad50c9d92c523cb.log``, the path you set here should include ``kingfisher``.

Archives database
~~~~~~~~~~~~~~~~~

Archive has it’s own database; one sqlite file. This should be the full path including file name to that file.

.. code-block:: ini

   [DBARCHIVE]
   FILEPATH = ~/kingfisher-archive-database.sqlite

You do not need to create it yourself; it will be created if it does not exist.

Sentry (optional)
~~~~~~~~~~~~~~~~~

.. code-block:: ini

   [SENTRY]
   DSN = https://<key>@sentry.io/<project>

Python Logging Config File (optional)
-------------------------------------

This should be placed at ``~/.config/ocdskingfisher-archive/logging.json``.

It’s contents should be standard Python logging configuration in JSON - for more see https://docs.python.org/3/library/logging.config.html#logging-config-dictschema

To download the default configuration:

.. code-block:: shell-session

   curl https://raw.githubusercontent.com/open-contracting/kingfisher-archive/master/samples/logging.json -o ~/.config/ocdskingfisher-archive/logging.json

To download a different configuration that includes debug messages:

.. code-block:: shell-session

   curl https://raw.githubusercontent.com/open-contracting/kingfisher-archive/master/samples/logging-debug.json -o ~/.config/ocdskingfisher-archive/logging.json

AWS S3
------

Create a bucket
~~~~~~~~~~~~~~~

Create a policy
~~~~~~~~~~~~~~~

Create a policy with these permissions for the desired bucket:

-  s3:ListBucket
-  s3:PutObject
-  s3:GetObject
-  s3:DeleteObject

Create a user
~~~~~~~~~~~~~

#. Open the `IAM dashboard <https://console.aws.amazon.com/iam/home>`__
#. Click *Users*
#. Click *Add user*

   #. Set *User name*
   #. Check *Programmatic access* under *Access type*
   #. Click *Next: Permissions*
   #. Click *Attach existing policies directly*
   #. Check the policy created earlier
   #. Click *Next: Tags*
   #. Click *Next: Review*
   #. Click *Create user*

#. Copy the access key ID and the secret access key for the next step

Create configuration file
~~~~~~~~~~~~~~~~~~~~~~~~~

This is one of `many methods <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html>`__. Create ``~/.aws/config``:

.. code-block:: ini

   [default]
   region = us-east-1
   aws_access_key_id = xxxxxxxxxxxxx
   aws_secret_access_key = yyyyyyyyyyyyyyyyyyyy
