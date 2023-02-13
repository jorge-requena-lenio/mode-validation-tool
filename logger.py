import logging
import os


class ReportLogger:
    def __init__(self, report_log_folder):
        if not os.path.exists(report_log_folder):
            os.makedirs(report_log_folder)

        self.logger = logging.getLogger(report_log_folder)
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(message)s")

        file_handler = logging.FileHandler(report_log_folder + "/report.log")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)

    def get_logger(self):
        return self.logger


class QueryLogger:
    def __init__(self, report_log_file):
        self.logger = logging.getLogger(report_log_file)
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(message)s")

        file_handler = logging.FileHandler(report_log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)

    def get_logger(self):
        return self.logger
