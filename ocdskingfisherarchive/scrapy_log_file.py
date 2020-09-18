import datetime

from logparser import parse


class ScrapyLogFile():

    def __init__(self, file_name):
        with open(file_name) as fp:
            text = fp.read()
        # If you pass headlines=0, taillines=0 the returned dict has all the raw lag data in, which may be very large
        # So we request 1 line for each even tho we don't need it
        self.log_data = parse(text, headlines=1, taillines=1)

    def does_match_date_version(self, date_version):
        start_time = datetime.datetime.strptime(self.log_data['first_log_time'], '%Y-%m-%d %H:%M:%S')
        diff = abs(start_time.timestamp() - date_version.timestamp())
        return diff < 3
