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
