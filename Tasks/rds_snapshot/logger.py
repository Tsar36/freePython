# logger.py

import logging


class Logger:
    @staticmethod
    def info(message):
        logging.basicConfig(level=logging.INFO)
        logging.info(message)

    @staticmethod
    def error(message):
        logging.basicConfig(level=logging.ERROR)
        logging.error(message)