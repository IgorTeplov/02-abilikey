import logging
from uuid import uuid4
from pathlib import Path

BASE_DIR = Path('./folders/')

EXEC_ID = uuid4()
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(module)s|%(process)s] - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

LOG_DIR = BASE_DIR / 'log'
if not LOG_DIR.is_dir():
    LOG_DIR.mkdir()

EXECUTION_LOG_DIR = LOG_DIR / "executions"
if not EXECUTION_LOG_DIR.is_dir():
    EXECUTION_LOG_DIR.mkdir()

REQUEST_LOG_DIR = LOG_DIR / "requests"
if not REQUEST_LOG_DIR.is_dir():
    REQUEST_LOG_DIR.mkdir()

EXECUTION_LOG_DIR /= str(EXEC_ID)
REQUEST_LOG_DIR /= str(EXEC_ID)

if not EXECUTION_LOG_DIR.is_dir():
    EXECUTION_LOG_DIR.mkdir()

if not REQUEST_LOG_DIR.is_dir():
    REQUEST_LOG_DIR.mkdir()


def get_base_logger():
    LOG_FILE = "execution.log"

    logger = logging.getLogger("ExecutionLog")
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(LOG_DIR / LOG_FILE)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger


def get_request_logger():
    LOG_FILE = f"requests.log"

    logger = logging.getLogger("RequestsIDLog")
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(REQUEST_LOG_DIR / LOG_FILE)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger


def get_logger_for_executions():
    LOG_FILE = f"execution.log"

    logger = logging.getLogger("ExecutionIDLog")
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(EXECUTION_LOG_DIR / LOG_FILE)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger


def get_logger_for_error_executions():
    LOG_FILE = f"execution.error.log"

    logger = logging.getLogger("ExecutionIDErrorLog")
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(EXECUTION_LOG_DIR / LOG_FILE)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger


logger = get_base_logger()
execution_logger = get_logger_for_executions()
error_logger = get_logger_for_error_executions()
request_logger = get_request_logger()
