import logging
import os
import mizuba as mz  # type: ignore

# Create the logger.
logger = logging.getLogger("arroyo")

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

# Activate verbose output in development mode.
if os.getenv("ARROYO_BACKEND_DEVELOPMENT") is not None:
    logger.setLevel(logging.DEBUG)
    logging.getLogger("mizuba").setLevel(logging.DEBUG)
    mz.set_logger_level_trace()
