import ast
import datetime
import os

from logparser import parse

# Kingfisher Collect logs an INFO message starting with "Spider arguments:".
SPIDER_ARGUMENTS_SEARCH_STRING = ' INFO: Spider arguments: '


class ScrapyLogFile():
    """
    A representation of a Scrapy log file.
    """

    @classmethod
    def find(cls, logs_directory, source_id, data_version):
        """
        Finds and returns the first matching log file for the given crawl.

        :param str logs_directory: Kingfisher Collect's project directory within Scrapyd's logs_dir directory
        :param str source_id: the spider's name
        :param datetime.datetime data_version: the crawl directory's name, parsed as a datetime
        :returns: the first matching log file
        :rtype: ocdskingfisherarchive.scrapy.ScrapyLogFile
        """
        source_directory = os.path.join(logs_directory, source_id)
        if os.path.isdir(source_directory):
            for filename in sorted(os.listdir(source_directory)):
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

    # Logparser processing

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

    def _process_logparser(self):
        """
        Parses the log file with ``logparser``.
        """
        with open(self.name) as f:
            text = f.read()

        # `taillines=0` sets the 'tail' key to all lines, so we set it to 1.
        self._logparser_data = parse(text, headlines=0, taillines=1)

    # Line-by-line processing

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

        return any(self._spider_arguments.get(arg) for arg in (
            'from_date', 'until_date', 'year', 'start_page', 'publisher', 'system', 'sample'
        ))

    def _process_line_by_line(self):
        self._errors_count = 0
        self._spider_arguments = {}

        buf = []
        with open(self.name) as f:
            for line in f:
                if buf or line.startswith('{'):
                    buf.append(line.rstrip())
                if buf and buf[-1].endswith('}'):
                    try:
                        # Scrapy logs items as dicts. FileError items, representing retrieval errors, are identified by
                        # an 'errors' key. FileError items use only simple types, so `ast.literal_eval` can be used.
                        item = ast.literal_eval(''.join(buf))
                        if 'errors' in item:
                            self._errors_count += 1
                    except ValueError:
                        # Scrapy dumps stats as a dict, which uses `datetime.datetime` types that can't be parsed with
                        # `ast.literal_eval`.
                        pass
                    buf = []

                index = line.find(SPIDER_ARGUMENTS_SEARCH_STRING)
                if index > -1:
                    # `eval` is used, because the string can contain `datetime.date` and is written by trusted code in
                    # Kingfisher Collect. Otherwise, we can modify the string so that `ast.literal_eval` can be used.
                    self._spider_arguments = eval(line[index + len(SPIDER_ARGUMENTS_SEARCH_STRING):])
