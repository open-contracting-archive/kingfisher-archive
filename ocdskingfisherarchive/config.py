import configparser
import os
import sys

import pgpasslib


"""This holds configuration information for Kingfisher.
Whatever tool is calling it - CLI or other code - should create one of these, set it up as required and pass it around.
"""


class Config:

    def __init__(self):
        self.database_host = ''
        self.database_port = 5432
        self.database_user = ''
        self.database_name = ''
        self.database_password = ''

        self.data_directory = None
        self.logs_directory = None

        self.database_archive_filepath = None

        self.s3_bucket_name = None

        self.sentry_dsn = ''

    def load_user_config(self):
        # First, try and load any config in the ini files
        self._load_user_config_ini()
        # Second, loook for password in .pggass
        self._load_user_config_pgpass()
        # Third, try and load any config in the env (so env overwrites ini)
        self._load_user_config_env()

    def _load_user_config_pgpass(self):
        if not self.database_name or not self.database_user:
            return

        try:
            password = pgpasslib.getpass(
                self.database_host,
                self.database_port,
                self.database_name,
                self.database_user
            )
            if password:
                self.database_password = password

        except pgpasslib.FileNotFound:
            # Fail silently when no files found.
            return
        except pgpasslib.InvalidPermissions:
            print('Your pgpass file has the wrong permissions, for your safety this file will be ignored. Please fix '
                  'the permissions and try again.')
            return
        except pgpasslib.PgPassException:
            print("Unexpected error:", sys.exc_info()[0])
            return

    def _load_user_config_env(self):
        pass

    def _load_user_config_ini(self):
        config = configparser.ConfigParser()

        if os.path.isfile(os.path.expanduser('~/.config/ocdskingfisher-archive/config.ini')):
            config.read(os.path.expanduser('~/.config/ocdskingfisher-archive/config.ini'))
        else:
            return

        self.database_host = config.get('DBHOST', 'HOSTNAME')
        self.database_port = config.get('DBHOST', 'PORT')
        self.database_user = config.get('DBHOST', 'USERNAME')
        self.database_name = config.get('DBHOST', 'DBNAME')
        self.database_password = config.get('DBHOST', 'PASSWORD', fallback='')

        self.database_archive_filepath = config.get('DBARCHIVE', 'FILEPATH')

        self.data_directory = config.get('DIRECTORIES', 'DATA')
        self.logs_directory = config.get('DIRECTORIES', 'LOGS')

        self.s3_bucket_name = config.get('S3', 'BUCKETNAME')

        self.sentry_dsn = config.get('SENTRY', 'DSN', fallback='')

    def get_database_connection_params(self):
        return {
            'user': self.database_user,
            'password': self.database_password,
            'host': self.database_host,
            'port': self.database_port,
            'dbname': self.database_name,
        }
