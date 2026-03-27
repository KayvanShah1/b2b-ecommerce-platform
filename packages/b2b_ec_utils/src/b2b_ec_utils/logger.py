import logging
import logging.handlers
import os

from rich.logging import RichHandler

from b2b_ec_utils.settings import settings


def get_logger(name):
    logger = logging.getLogger(name)

    # Prevent adding handlers multiple times
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        # Console handler (Rich)
        ch = RichHandler(rich_tracebacks=True)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter("'%(name)s' - %(message)s"))
        logger.addHandler(ch)

        # File handler (Rotating)
        fh = logging.handlers.RotatingFileHandler(
            os.path.join(settings.log_dir, f"{settings.project_name}.log"),
            maxBytes=2000 * 1024,  # 1 MB
            backupCount=5,
            delay=True,
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(fh)

    return logger
