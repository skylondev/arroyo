import polars as pl
import threading
import pathlib
import logging
import os
from dataclasses import dataclass, field
import pickle
from astropy.time import Time  # type: ignore
from ._create_new_conj import _create_new_conj

# Determine the absolute path to the cache dir.
_cache_dir = (pathlib.Path(__file__).parent.parent / "cache").resolve()

assert _cache_dir.is_dir()

# Compute the path to the conjunctions data file in the cache.
_conj_path = _cache_dir / "conj.pickle"

# The conjunctions dataframe schema.
_conj_df_schema = pl.Schema(
    [
        ("norad_id_i", pl.UInt64),
        ("norad_id_j", pl.UInt64),
        ("object_name_i", pl.String),
        ("object_name_j", pl.String),
        ("ops_status_i", pl.String),
        ("ops_status_j", pl.String),
        ("tca", pl.Datetime(time_unit="ns", time_zone="UTC")),
        ("dca", pl.Float64),
        ("relative_speed", pl.Float64),
        ("tca_diff", pl.Float64),
        ("dca_diff", pl.Float64),
        ("relative_speed_diff", pl.Float64),
    ]
)


# Conjunctions data class.
@dataclass
class conjunction_data:
    n_missed_conj: int = 0
    df: pl.DataFrame = field(
        default_factory=lambda: pl.DataFrame([], schema=_conj_df_schema)
    )
    timestamp: str | None = None
    comp_time: float = 0
    date_begin: str | None = None
    date_end: str | None = None


# Helper for the initial setup of the conjunctions data.
def _conj_init_setup() -> conjunction_data:
    logger = logging.getLogger("arroyo")

    # Check if we have existing data in the cache.
    if _conj_path.exists():
        # There is conjunction data in the cache, load it.

        assert _conj_path.is_file()

        logger.debug(
            f"Existing conjunctions data with UTC mtime {Time(val=os.path.getmtime(_conj_path), format='unix').utc.iso} found on startup"
        )

        try:
            with open(_conj_path, "rb") as f:
                ret: conjunction_data = pickle.load(f)
        except Exception:
            logger.error(
                "Exception caught while attempting to load cached conjunction data, deleting it and returning empty data instead",
                exc_info=True,
                stack_info=True,
            )

            # Delete the existing data.
            _conj_path.unlink()

            return conjunction_data()

        if ret.df.schema != _conj_df_schema:
            # Schema mismatch.
            logger.debug(
                "The existing conjunction data has an invalid schema, deleting it and returning empty data"
            )

            # Delete the existing data.
            _conj_path.unlink()

            # Return empty data.
            return conjunction_data()

        return ret
    else:
        # No data in the cache, return empty data.
        logger.debug(
            "No existing conjunctions data found on startup, initialising empty data"
        )

        return conjunction_data()


# Initial setup of the conjunctions data.
_conj_lock = threading.Lock()
_conj = _conj_init_setup()


# Thread-safe getter for the conjunctions data.
def _get_conjunctions() -> conjunction_data:
    with _conj_lock:
        return _conj


# Thread-safe setter for the conjunctions data.
def _set_conjunctions(new_conj: conjunction_data) -> None:
    global _conj

    with _conj_lock:
        _conj = new_conj


# The data processor thread.
class _data_processor(threading.Thread):
    def __init__(self) -> None:
        super().__init__()
        self._stop_event = threading.Event()

    def run(self) -> None:
        logger = logging.getLogger("arroyo")

        logger.debug("socrates_comparison data processor thread started")

        # Maximum age for the conjunctions data (in seconds).
        MAX_AGE = 4 * 3600

        while not self._stop_event.is_set():
            try:
                # As a first step, we must determine if the current cached
                # data (if existing) is fresh enough.
                conj_age = None
                if _conj_path.exists():
                    # The conjunctions data file exists, fetch its modification time
                    # and determine its age in seconds.
                    conj_mtime = Time(val=os.path.getmtime(_conj_path), format="unix")
                    conj_age = (Time.now() - conj_mtime).to_value("s")

                if conj_age and conj_age < MAX_AGE:
                    # The existing conjunctions data is fresh enough, go to sleep
                    # and try again when the conjunctions data is outdated.
                    sleep_time = MAX_AGE - conj_age

                    logger.debug(
                        f"Found suitable conjunctions data with age {conj_age}s, sleeping for {sleep_time}s"
                    )
                    self._stop_event.wait(sleep_time)

                    # Continue to the next loop iteration.
                    continue
                elif conj_age:
                    logger.debug(
                        f"The existing conjunctions data is too old ({conj_age}s), creating new data"
                    )
                else:
                    logger.debug(
                        "No conjunctions data found in the cache, creating new data"
                    )

                # We need new conjunctions data. Create it, with timing.
                ts_start = Time.now()
                n_missed_conj, df, date_begin, date_end = _create_new_conj()
                assert df.schema == _conj_df_schema
                ts_stop = Time.now()

                # Calculate the computation time.
                comp_time = (ts_stop - ts_start).to_value("s")

                # Change precisions for string conversions.
                ts_stop.precision = 0
                date_begin.precision = 0
                date_end.precision = 0

                # Build the conjunctions dataclass.
                cdata = conjunction_data(
                    n_missed_conj=n_missed_conj,
                    df=df,
                    timestamp=ts_stop.utc.iso,
                    comp_time=comp_time,
                    date_begin=date_begin.utc.iso,
                    date_end=date_end.utc.iso,
                )

                logger.debug("New conjunctions data successfully created")

                # Save it into the cache.
                # NOTE: it is *important* to do this *before* invoking _set_conjunctions(),
                # otherwise we may end up pickling a dataframe while we are operating on it
                # from another thread (i.e., from the socrates_comparison endpoint).
                # This results in "RuntimeError: Already mutably borrowed" errors and possibly
                # worse, as I am not sure what goes on exactly with the pickling...
                with open(_conj_path, "wb") as f:
                    pickle.dump(cdata, f)

                logger.debug(
                    f"New conjunctions data successfully saved into the cache at '{_conj_path}'"
                )

                # Assign it.
                _set_conjunctions(cdata)

                logger.debug("New conjunctions data successfully assigned")

                logger.debug(f"Sleeping for {MAX_AGE}s")
                self._stop_event.wait(MAX_AGE)
            except Exception:
                logger.error(
                    "Exception caught in the data processor thread, re-trying in 10 seconds",
                    exc_info=True,
                    stack_info=True,
                )
                self._stop_event.wait(10)

    def _stop(self) -> None:
        logger = logging.getLogger("arroyo")

        logger.debug("Setting the stop event on the data processor thread")

        self._stop_event.set()
