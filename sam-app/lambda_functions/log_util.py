# set logging level
import os
import logging

# Configure the logger
logger = logging.getLogger()
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('urllib').setLevel(logging.CRITICAL)
logging.getLogger('requests').setLevel(logging.CRITICAL)

# Set the logging level based on environment variable, default to INFO if not set
logging_level = os.getenv("LOGGING_LEVEL", "INFO").upper()
logger.setLevel(logging.getLevelName(logging_level))

# Ensure logger is configured properly
logger.info(f"Logging level set to {logging_level}")

import re

_CONTROL_CHAR_RE = re.compile(r'[\x00-\x08\x0a-\x1f\x7f\x85\u2028\u2029]+')

def sanitize_log(value):
    """Replace control characters to prevent log injection (CWE-117)."""
    if isinstance(value, str):
        return _CONTROL_CHAR_RE.sub('[SANITIZED]', value)
    return value