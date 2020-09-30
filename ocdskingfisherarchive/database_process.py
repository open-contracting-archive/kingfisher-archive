import psycopg2

from ocdskingfisherarchive.collection import Collection


class DataBaseProcess:

    def __init__(self, config):
        self.connection = psycopg2.connect(**config.get_database_connection_params())
        self.cursor = self.connection.cursor()
        self.config = config

    def get_collections_to_consider_archiving(self):
        sql = "SELECT id, source_id, data_version  FROM collection " +\
              "WHERE sample IS FALSE AND store_end_at IS NOT NULL AND transform_type = '' AND deleted_at IS NULL " +\
              "ORDER BY id ASC"
        collections = []
        self.cursor.execute(sql)
        for row in self.cursor.fetchall():
            collections.append(
                Collection(
                    config=self.config,
                    database_id=row[0],
                    source_id=row[1],
                    data_version=row[2],
                )
            )
        return collections
