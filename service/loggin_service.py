import gzip
import logging
import os
import shutil
import datetime
from logging.handlers import TimedRotatingFileHandler

from util.properties_reader import property_value

logger = logging.getLogger(__name__)

def configure_logging():

    logging_path = property_value('Logging', 'path')
    logging_file = property_value('Logging', 'file')
    log_file = f'{logging_path}{logging_file}'

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",  # Rotate logs daily
        interval=1,  # One day interval
        backupCount=30  # Keep up to 7 backup log files
    )

    handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.addHandler(handler)

    # Create a stream handler (console output)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.setLevel(logging.INFO)


def compact_yesterday_log_file():
    logging_path = property_value('Logging', 'path')
    logging_file = property_value('Logging', 'file')
    old_logging_file = property_value('Logging', 'old_file')

    # Get yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # Create a compressed filename for yesterday's logs
    compressed_filename = old_logging_file.format(yesterday_str)  # f"carrefour-collector-{yesterday_str}.log"

    if not os.path.exists(f'{logging_path}{compressed_filename}'):
        logger.info(f'File not found to compress {logging_path}{compressed_filename}')
        return

    src_log_file = os.path.join(logging_path, f'{logging_file}.{yesterday_str}')

    # Compress the log file using gzip
    with open(src_log_file, "rb") as f_in:
        with gzip.open(f"{src_log_file}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Remove the original uncompressed log file
    os.remove(src_log_file)

