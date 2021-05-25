import logging


class Logger:
    def __int__(self, name, level=logging.DEBUG, format="%(message)s"):
        logging.basicConfig(level=level, format=format)
        self.logger = logging.getLogger(name)

    def log(self, message):
        self.logger.info(message)