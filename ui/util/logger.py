import logging
import os


def setup_logger(
    name: str = None,
    log_file: str = "logfile.log",
    verbose: bool = False,
) -> logging.Logger:
    """Sets up a logger that outputs messages to both the console and a log file.

    This logger can be named or use the root logger if no name is provided.
    Handlers are only added once to prevent duplicate messages. If `verbose`
    is True, DEBUG messages are shown and filename/line info is included.

    Args:
        name (str, optional): Name of the logger. If None, the root logger is used.
        log_file (str, optional): Path to the log file. Default is 'logfile.log'.
        verbose (bool, optional): If True, sets level to DEBUG
        and includes filename and line number.

    Returns:
        logging.Logger: Configured logger instance with console and file handlers.
    """
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    if logger.handlers:
        return logger

    if verbose:
        fmt = "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    else:
        fmt = "%(asctime)s - %(levelname)s - %(message)s"

    # Console handler setup
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(console_handler)

    # File handler setup
    os.makedirs(".logs", exist_ok=True)
    file_handler = logging.FileHandler(os.path.join(".logs", log_file), mode="w")
    file_handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(file_handler)

    return logger

logger = setup_logger("log_parser")