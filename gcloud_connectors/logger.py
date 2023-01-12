import logging


class Logger:
    def __init__(self, logger_name):
        logger = logging.getLogger(logger_name)

        logger.setLevel(logging.INFO)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # create formatter and add it to the handlers
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        # add the handlers to logger
        logger.addHandler(ch)
        self.logger = logger

    def get_logger(self):
        return self.logger


class EmptyLogger:
    @staticmethod
    def info(msg):
        pass

    @staticmethod
    def warn(msg):
        pass

    @staticmethod
    def warning(msg):
        pass

    @staticmethod
    def debug(msg):
        pass

    @staticmethod
    def critical(msg):
        pass

    @staticmethod
    def error(msg):
        pass
