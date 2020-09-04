import json
import os
import tempfile


class Collection:

    def __init__(self, config, database_id, source_id, data_version):
        self.config = config
        self.database_id = database_id
        self.source_id = source_id
        self.data_version = data_version

    def write_meta_data_file(self):
        data = {
            'database_id': self.database_id,
        }
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.json')
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)
        os.close(file_descriptor)
        return filename

    def write_data_file(self):
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.tar')
        os.close(file_descriptor)

        dir_to_add = self.config.directory_data + '/' + self.source_id + '/' + \
            self.data_version.strftime("%Y%m%d_%H%M%S")
        command1 = 'tar -cvf ' + filename + ' ' + dir_to_add
        return1 = os.system(command1)
        if return1 != 0:
            raise Exception(command1 + ' Got Return ' + str(return1))

        command2 = 'lzip -c ' + filename + ' > ' + filename+'.lz'
        return2 = os.system(command2)
        if return2 != 0:
            raise Exception(command2 + ' Got Return ' + str(return2))

        return filename+'.lz'

    def get_s3_directory(self):
        # Including the database_id is temporary until we have sorted the code to only upload one per source/year/month
        return self.source_id + '/' + str(self.data_version.year) + '/' + str(self.data_version.month).zfill(2) \
            + '/' + str(self.database_id)
