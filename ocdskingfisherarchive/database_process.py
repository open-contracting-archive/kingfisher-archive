import psycopg2

from ocdskingfisherarchive.collection import Collection


class DataBaseProcess:

    def __init__(self, database_connection_parameters):
        self.connection = psycopg2.connect(**database_connection_parameters)
        self.cursor = self.connection.cursor()

    def get_collections_to_consider_archiving(self):
        sql = "SELECT id, source_id, data_version  FROM collection " \
              "WHERE sample IS FALSE AND store_end_at IS NOT NULL AND transform_type = '' AND deleted_at IS NULL " \
              "ORDER BY id ASC"
        collections = []
        self.cursor.execute(sql)
        for row in self.cursor.fetchall():
            collections.append(Collection(row[0], row[1], row[2], self.data_directory,, self.logs_directory))
        return collections
