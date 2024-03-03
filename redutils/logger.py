"""
Script Name :    core_logger.py

Created on  :    January 31, 2022

Author      :    Jeff Howard
_______________________________________________________________________________
Purpose     :    Core logging setup for all applications

stdout is redirected to streamhandler.

Copyright 2022 @Data-Methods.LLC an Alaskan LLC - All rights reserved
"""

import logging
import os
import re


class Logger:
    """Standardizes logging to track Python code used in various classes.

    Setups up the file logging, the console logging and functions to
    parse returned data and pass it the logger.


    """

    def __init__(self, log_folder: str, log_file: str):
        """
        :param log_folder: (str) - Location of folder to store log in.
        :param log_file: (str) - Name of the log file
        """
        self.log_folder = log_folder
        self.log_file = log_file

    def logger(self, name: str) -> logging.Logger:
        """Sets up logging to be extended into multiple classes.

        :param name: (str) - Name of the logger

        :return: (logging.Logger) - the setup logger to be extended to other classes
        """
        # file_formatter = logging.Formatter(
        #     "{asctime} {name} {levelname:^8s} {message}", style="{"
        # )
        file_formatter = logging.Formatter(
            "[%(levelname)s] - %(asctime)s - %(name)s\n\t%(message)s"
        )
        console_formatter = logging.Formatter(
            "{asctime} {name} {levelname:^8s} {message}", style="{"
        )
        log_filename = os.path.join(self.log_folder, self.log_file)

        file_handler = logging.FileHandler(log_filename, mode="a")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARN)
        console_handler.setFormatter(console_formatter)

        logger = logging.getLogger(name)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.setLevel(logging.DEBUG)

        return logger

    @staticmethod
    def fmt_results(
        sfqid: str, row_count: int, description: str, logger: logging.Logger
    ) -> None:
        """Format and log attribute values from the Snowflake cursor
           when a query is executed.

        :param sfqid: (str) - Read-only attribute that returns the Snowflake
        query ID in the last execute or execute_async executed

        :param row_count: (int) - Number of rows the query returned
        :param description: (str) - List of ResultMetadata objects that show metadata
        a column(s) in the result
        :param logger: (logging.Logger) - logging object

        """
        # Get the column names by extracting strings between pairs of single quotes in text.
        matches = re.findall(r"\'(.*?)\'", str(description))
        # log the results on one line
        logger.info("sfqid: %s, row_count: %s, columns: %s", sfqid, row_count, matches)
