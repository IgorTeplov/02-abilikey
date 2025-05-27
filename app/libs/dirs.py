from pathlib import Path


BASE_DIR = Path('./folders/')
if not BASE_DIR.is_dir():
    BASE_DIR.mkdir()


PIPE_DIR = BASE_DIR / 'pipeline'
if not PIPE_DIR.is_dir():
    PIPE_DIR.mkdir()


LOG_DIR = BASE_DIR / 'log'
if not LOG_DIR.is_dir():
    LOG_DIR.mkdir()


REQUEST_DIR = BASE_DIR / 'requests'
if not REQUEST_DIR.is_dir():
    REQUEST_DIR.mkdir()


EXECUTION_LOG_DIR = LOG_DIR / "executions"
if not EXECUTION_LOG_DIR.is_dir():
    EXECUTION_LOG_DIR.mkdir()

REQUEST_LOG_DIR = LOG_DIR / "requests"
if not REQUEST_LOG_DIR.is_dir():
    REQUEST_LOG_DIR.mkdir()
