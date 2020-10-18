import ast
import datetime
import os

from logparser import parse


class ScrapyLogFile():
    @classmethod
    def find(cls, logs_directory, source_id, data_version):
        source_directory = os.path.join(logs_directory, source_id)
        if os.path.isdir(source_directory):
            for filename in os.listdir(source_directory):
                if filename.endswith(".log"):
                    scrapy_log_file = ScrapyLogFile(os.path.join(source_directory, filename))
                    if scrapy_log_file.does_match_date_version(data_version):
                        return scrapy_log_file

    def __init__(self, name):
        self.name = name
        # We lazy-parse any data we need
        # This may be called on a directory of very big logs and we want it to be efficient.
        # None means not parsed
        self._logparser_data = None
        self._errors_sent_to_process_count = None
        self._spider_arguments = None

    def delete(self):
        if os.path.isfile(self.name):
            os.remove(self.name)
            if os.path.isfile(f'{self.name}.stats'):
                os.remove(f'{self.name}.stats')

    # Logparser Processing

    def _process_logparser(self):
        with open(self.name) as fp:
            text = fp.read()
        # If you pass headlines=0, taillines=0 the returned dict has all the raw lag data in, which may be very large
        # So we request 1 line for each even tho we don't need it
        self._logparser_data = parse(text, headlines=1, taillines=1)

    def does_match_date_version(self, date_version):
        if self._logparser_data is None:
            self._process_logparser()
        start_time = datetime.datetime.strptime(self._logparser_data['first_log_time'], '%Y-%m-%d %H:%M:%S')
        diff = abs(start_time.timestamp() - date_version.timestamp())
        return diff < 3

    def is_finished(self):
        if self._logparser_data is None:
            self._process_logparser()
        return self._logparser_data.get('finish_reason') == 'finished'

    # Line By Line Processing

    def _process_line_by_line(self):
        """
        Process log file line by line (Do not load all at once to keep memory use down!). Look for and cache in object
        variables: error count and spider arguments.
        """
        self._errors_sent_to_process_count = 0
        self._spider_arguments = {}
        spider_arguments_search_string = '] INFO: Spider arguments: '
        data_block_content_string = ''
        with open(self.name) as fp:
            while True:
                line = fp.readline()
                if not line:
                    break
                # Looking for data blocks
                if data_block_content_string or line.startswith('{'):
                    data_block_content_string += line.strip()
                if data_block_content_string and line.strip().endswith('}'):
                    # Use of eval would parse the final block of "Dumping Scrapy stats" but
                    # A) ast.literal_eval is safer
                    # B) that is available in self.log_data / crawler_stats anyway
                    # but does mean we need to ignore ValueError for when it crashes on that final block
                    try:
                        data_block_content = ast.literal_eval(data_block_content_string)
                        if 'errors' in data_block_content:
                            self._errors_sent_to_process_count += 1
                    except ValueError:
                        pass
                    data_block_content_string = ''
                # Looking for Spider arguments
                find_spider_arguments_search_string = line.find(spider_arguments_search_string)
                if find_spider_arguments_search_string > -1:
                    spider_arguments_data = \
                        line[find_spider_arguments_search_string + len(spider_arguments_search_string):]
                    try:
                        # This data should only have strings, no datetimes
                        # so we can use ast.literal_eval with no issues
                        self._spider_arguments = ast.literal_eval(spider_arguments_data)
                    except ValueError:
                        pass

    @property
    def errors_count(self):
        if self._errors_sent_to_process_count is None:
            self._process_line_by_line()
        return self._errors_sent_to_process_count

    def is_subset(self):
        if self._spider_arguments is None:
            self._process_line_by_line()

        # Older spider log files may not have this data, so make sure it can deal with that case.
        return bool(self._spider_arguments.get('sample')) or \
            bool(self._spider_arguments.get('from_date')) or \
            bool(self._spider_arguments.get('until_date'))
