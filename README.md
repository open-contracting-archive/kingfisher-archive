# Kingfisher Archive

This moves data and log files written by [Kingfisher Collect](https://kingfisher-collect.readthedocs.io/en/latest/) to an AWS S3 Bucket, after they have been loaded into [Kingfisher Process](https://kingfisher-process.readthedocs.io/en/latest/). 

The data is archived in case we need to re-load it in future.

## Setup

As presently written, the script needs to be run by a user that has:

* Access to the ``collection`` table managed by Kingfisher Process (e.g. with the ``.pgpass`` file)
* Permission to read and delete the data and log files written by Kingfisher Collect (with sudo access under a user called `ocdskfs`)
* Read & Write access to a S3 bucket to hold the archives

One install of Kingfisher should back up to one S3 bucket only. There are algorithms to decide whether to back something up based on what has already been backed up, and these may get confused if 2 Kingfisher installs try to back up to the same S3 bucket.

### Config File (required)

The config file should be placed at `~/.config/ocdskingfisher-archive/config.ini`.

Download the sample main configuration file:

    curl https://raw.githubusercontent.com/open-contracting/kingfisher-archive/master/samples/config.ini -o ~/.config/ocdskingfisher-archive/config.ini

#### Postgres (required)

Configure the database connection settings:

    [DBHOST]
    HOSTNAME = localhost
    PORT = 5432
    USERNAME = ocdskingfisher
    PASSWORD = 
    DBNAME = ocdskingfisher

If you prefer not to store the password in config.ini, you can use the PostgreSQL Password File, `~/.pgpass`, which overrides any password in config.ini.

#### S3 (required)

The bucket name should be set here.

    [S3]
    BUCKETNAME = backups-go-here

For setting access credentials, see a later section.

#### Directories on disk (required)

    [DIRECTORIES]
    DATA = /scrapyd/data
    LOGS = /scrapyd/logs

Note these should be directories that directly contain directories with names of sources/spiders. On your system there may be a directory for the name of the project in Scrapyd. If so, that should be included in these paths.

For instance, if you have log files like `/scrapyd/logs/kingfisher/scotland/3487b29602b311ebaad50c9d92c523cb.log`, the path you set here should include `kingfisher`.

#### Archives database (required)

Archive has it's own database; one sqlite file. This should be the full path including file name to that file.

    [DBARCHIVE]
    FILEPATH = ~/kingfisher-archive-database.sqlite

You do not need to create it yourself; it will be created if it does not exist.

#### Sentry (optional)

    [SENTRY]
    DSN = https://<key>@sentry.io/<project>

### Python Logging Config File (optional)

This should be placed at `~/.config/ocdskingfisher-archive/logging.json`.

It's contents should be standard Python logging configuration in JSON - for more see https://docs.python.org/3/library/logging.config.html#logging-config-dictschema

To download the default configuration:

    curl https://raw.githubusercontent.com/open-contracting/kingfisher-archive/master/samples/logging.json -o ~/.config/ocdskingfisher-archive/logging.json

To download a different configuration that includes debug messages:

    curl https://raw.githubusercontent.com/open-contracting/kingfisher-archive/master/samples/logging-debug.json -o ~/.config/ocdskingfisher-archive/logging.json

### S3 (required)

#### Getting Credentials

Go to IAM service.

Click users.

Click Add.

Enter a Username.

For "Access type" pick "Programmatic access" and not "AWS Management Console access".

For Set Permissions, don't select anything. Just continue.

No Tags. Just continue.

Review and create.

Copy access key and Secret access key somewhere safe.

Close creation Wizard.

You should see user in the list. 

Click new user name.

For policies use the wizard and on the relevant S3 bucket make sure the user has:
* s3:ListBucket
* s3:PutObject
* s3:GetObject
* s3:DeleteObject

#### Specifying Config and Credentials (required)

For more see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

What follows describes one of several methods - you can use any other method compatible with boto3 if you prefer.

Create `~/.aws/config`:

    [default]
    region = us-east-1
    aws_access_key_id = xxxxxxxxxxxxx
    aws_secret_access_key = yyyyyyyyyyyyyyyyyyyy

## Run

This will run with process locking as provided by https://pypi.org/project/python-pidfile/ so only one instance of this can run at once. (There are algorithms to decide whether to back something up based on what has already been backed up, and these may get confused if 2 processes run at once.)

    python manage.py archive

You con run a test mode ("dry mode"):

    python manage.py archive --dry-run

To see options (including a dry run)

    python manage.py archive --help

