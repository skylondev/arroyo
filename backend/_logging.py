import logging
import os

# Create the logger.
logger = logging.getLogger("arroyo")

if os.getenv("ARROYO_BACKEND_DEVELOPMENT") is not None:
    logger.setLevel(logging.DEBUG)

# Set up the formatter.
formatter = logging.Formatter(
    fmt=r"%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s",
    datefmt=r"%Y-%m-%d %H:%M:%S",
)

# Create a handler.
c_handler = logging.StreamHandler()
c_handler.setFormatter(formatter)

# Link handler to logger.
logger.addHandler(c_handler)
