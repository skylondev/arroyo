from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from typing import Any
from contextlib import asynccontextmanager

# NOTE: import _logging first so that we trigger the creation
# of the logger.
from . import _logging  # noqa
from . import public_conjunctions


# Use a context manager to start and stop the data processor thread. See:
#
# https://fastapi.tiangolo.com/advanced/events/#lifespan-function
#
# NOTE: the return type should be defined more precisely, but for now we use Any
# in order to quench mypy warnings.
@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:
    # NOTE: this is a workaround for an issue
    # in erfa, which could in principle lead to
    # crashes in case astropy UTC/TAI conversions
    # are performed in a multithreaded context:
    #
    # https://github.com/liberfa/erfa/issues/103
    #
    # By triggering a UTC->TAI conversion at startup
    # time, we are at least ensuring that the builtin
    # leap seconds table has been correctly initialised.
    from astropy.time import Time  # type: ignore

    Time(2460669.0, format="jd", scale="utc").tai

    logger = logging.getLogger("arroyo")

    # Start the data processor thread.
    logger.debug("Creating the data processor thread")
    dp = public_conjunctions._data._data_processor()
    logger.debug("Starting the data processor thread")
    dp.start()

    yield

    # Stop the data processor thread.
    logger.debug("Stopping the data processor thread")
    dp._stop()
    logger.debug("Joining the data processor thread")
    dp.join()


origins = [
    "http://localhost:5173",
]

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(public_conjunctions.router)
