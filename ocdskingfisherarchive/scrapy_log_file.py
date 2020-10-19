import ast
import datetime
import os

from logparser import parse


class ScrapyLogFile():
    """
    A representation of a Scrapy log file.
    """

    @classmethod
    def find(cls, logs_directory, source_id, data_version):
        """
        Finds and returns the log file for the given crawl.

        :param str logs_directory: Kingfisher Collect's project directory within Scrapyd's logs_dir directory
        :param str source_id: the spider's name
        :param datetime.datetime data_version: the crawl directory's name, parsed as a datetime
        :returns: the first matching log file
        :rtype: ocdskingfisherarchive.scrapy.ScrapyLogFile
        """
        source_directory = os.path.join(logs_directory, source_id)
        if os.path.isdir(source_directory):
            for filename in os.listdir(source_directory):
                if filename.endswith(".log"):
                    scrapy_log_file = ScrapyLogFile(os.path.join(source_directory, filename))
                    if scrapy_log_file.match(data_version):
                        return scrapy_log_file

    def __init__(self, name):
        """
        :param str name: the full path to the log file
        """
        self.name = name

        self._logparser_data = None
        self._errors_count = None
        self._spider_arguments = None

    def delete(self):
        """
        Deletes the log file and any log summary ending in ``.stats``.
        """
        if os.path.isfile(self.name):
            os.remove(self.name)
        summary = f'{self.name}.stats'
        if os.path.isfile(summary):
            os.remove(summary)

    # Logparser Processing

    def _process_logparser(self):
        """
        Parses the log file with ``logparser``.
        """
        with open(self.name) as f:
            text = f.read()

        # `taillines=0` sets the 'tail' key to all lines, so we set it to 1.
        self._logparser_data = parse(text, headlines=0, taillines=1)

    def match(self, data_version):
        """
        :returns: whether the crawl directory's name, parsed as a datetime, is less than 3 seconds after the log file's
                  start time
        :rtype: bool
        """
        if self._logparser_data is None:
            self._process_logparser()

        start_time = datetime.datetime.strptime(self._logparser_data['first_log_time'], '%Y-%m-%d %H:%M:%S')
        return 0 <= data_version.timestamp() - start_time.timestamp() < 3

    def is_finished(self):
        """
        :returns: whether the crawl finished cleanly
        :rtype: bool
        """
        if self._logparser_data is None:
            self._process_logparser()

        return self._logparser_data.get('finish_reason') == 'finished'

    # Line By Line Processing

    def _process_line_by_line(self):
        """
        Process log file line by line. Look for and cache in object variables: error count and spider arguments.
        """
        self._errors_count = 0
        self._spider_arguments = {}
        spider_arguments_search_string = '] INFO: Spider arguments: '
        data_block_content_string = ''
        with open(self.name) as f:
            while True:
                line = f.readline()
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
                            self._errors_count += 1
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
        """
        :returns: the number of retrieval errors, according to the log file
        :rtype: int
        """
        if self._errors_count is None:
            self._process_line_by_line()

        return self._errors_count

    def is_subset(self):
        """
        :returns: whether the crawl collected a subset of the dataset, according to the log file
        :rtype: bool
        """
        if self._spider_arguments is None:
            self._process_line_by_line()

        # Older spider log files may not have this data, so make sure it can deal with that case.
        return bool(
            self._spider_arguments.get('sample') or
            self._spider_arguments.get('from_date') or
            self._spider_arguments.get('until_date')
        )
