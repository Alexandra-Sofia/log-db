import logging
import os


def setup_logger(name: str = None, log_file: str = 'logfile.log') -> logging.Logger:
    """
    Sets up a logger that outputs messages to both the console and a log file.
    Always overwrites the log file.

    :param name: Optional name for the logger. If None, the root logger will be used.
    :param log_file: Path to the log file. Default is 'logfile.log'.
    :return: A configured logging.Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Console handler setup
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(name)s %(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # Ensure the .input-logfiles directory exists
    os.makedirs(".input-logfiles", exist_ok=True)

    # Create the full path for the log file
    log_file_path = os.path.join(".input-logfiles", log_file)

    # File handler setup to overwrite the log file
    file_handler = logging.FileHandler(log_file_path, mode='w')  # 'w' mode overwrites the file
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger

logger = setup_logger("log_parser")