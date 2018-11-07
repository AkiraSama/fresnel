import logging
from pathlib import Path

ANSI_RESET = '\x1b[0m'
ANSI_RED = '\x1b[31m'
ANSI_GREEN = '\x1b[32m'
ANSI_YELLOW = '\x1b[33m'
ANSI_BLUE = '\x1b[34m'
ANSI_MAGENTA = '\x1b[35m'
ANSI_CYAN = '\x1b[36m'

LOGGING_COLORS = (
    (logging.DEBUG, ANSI_CYAN),
    (logging.INFO, ANSI_GREEN),
    (logging.WARN, ANSI_YELLOW),
    (logging.ERROR, ANSI_RED)
)

LOG_FORMAT_STR = '{asctime} {name} [{levelname}] {message}'
LOG_FORMAT_STR_COLOR = (
    f'{ANSI_BLUE}{{asctime}}{ANSI_RESET} '
    f'{ANSI_MAGENTA}{{name}}{ANSI_RESET} '
    '[{levelname}] {message}'
)

DESCRIPTION = "fresnel bot for Lighthouse 9"
PSQL_INFO_STR = 'dbname={dbname} user={user} password={password} host={host}'
PSQL_DEFAULT_DICT = {
    'dbname': None,
    'user': None,
    'password': None,
    'host': '127.0.0.1',
}
DEFAULT_CONFIG_PATH = Path('./config.yaml')
FILE_MODE = 0o755
