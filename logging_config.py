import logging
import sys


def configure_logger(name: str) -> logging.Logger:
    """
    Настраивает и возвращает логгер для данного модуля (name) 
    в соответствии с PEP 8 и требуемым форматом вывода в sys.stdout.
    """

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger
