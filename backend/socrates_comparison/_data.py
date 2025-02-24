import polars as pl
import threading
import pathlib
import logging
import os
from dataclasses import dataclass, field
import pickle
from astropy.time import Time  # type: ignore
from ._create_new_conj import _create_new_conj
import weakref
import shutil
import mizuba as mz  # type: ignore
import numpy as np

# Determine the absolute path to the cache dir.
_cache_dir = (pathlib.Path(__file__).parent / "cache").resolve()

assert _cache_dir.is_dir()

# Build the path to the pickled conjunctions data in the cache.
_cd_path = _cache_dir / "cd.pickle"

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


# Current version of the conjunctions data class.
# NOTE: this needs to be bumped when the conjunctions data class changes.
# This also includes changes in _conj_df_schema.
_cd_cur_version = 1


# Conjunctions data class. This is the class that holds the results of
# conjunction detection, including the conjunctions dataframe and several
# other metadata.
@dataclass(frozen=True)
class conjunction_data:
    # Version of the class.
    version: int = _cd_cur_version
    # The conjunctions dataframe.
    df: pl.DataFrame = field(
        default_factory=lambda: pl.DataFrame([], schema=_conj_df_schema)
    )
    # The number of missed conjunctions wrt socrates.
    n_missed_conj: int = 0
    # Computation timestamp (UTC string).
    timestamp: str | None = None
    # The total computation time (in seconds).
    comp_time: float = 0
    # Begin/end dates of the time period covered by
    # the computation (UTC strings).
    date_begin: str | None = None
    date_end: str | None = None
    # The name of the directory (within the cache dir)
    # storing the data of the polyjectory that was used
    # during conjunction detection.
    pj_dir_name: str | None = None
    # The list of norad IDs for the polyjectory that
    # was used during conjunction detection.
    norad_ids: np.typing.NDArray[np.uint64] | None = None


# The prefix of the directories (within the cache dir)
# storing the data of the polyjectories.
_pj_data_prefix = "mizuba_polyjectory"


# Helper for the initial setup of the conjunctions data.
# NOTE: this function only checks for the existence of a pickled conjunction_data
# instance, it does not attempt any check on the polyjectory data.
def _conj_init_setup() -> tuple[conjunction_data, mz.polyjectory | None]:
    logger = logging.getLogger("arroyo")

    # Check if we have existing data in the cache.
    if _cd_path.exists():
        # There is conjunctions data in the cache, load it.
        assert _cd_path.is_file()

        logger.debug(
            f"Existing conjunctions data with UTC mtime {Time(val=os.path.getmtime(_cd_path), format='unix').utc.iso} found on startup"
        )

        try:
            with open(_cd_path, "rb") as f:
                ret: conjunction_data = pickle.load(f)

            # Check the version of the data class.
            if ret.version != _cd_cur_version:
                raise ValueError(
                    f"Invalid existing conjunctions data version detected during unpickling: the version in the archive is {ret.version} but the expected version is {_cd_cur_version}"
                )
            assert ret.pj_dir_name is not None

            # Try to mount a polyjectory on ret.pj_dir_name.
            mount_point = _cache_dir / ret.pj_dir_name
            logger.debug(
                f"Attempting to mount a polyjectory on the '{mount_point}' directory"
            )
            pj = mz.polyjectory.mount(mount_point)
            logger.debug(
                f"polyjectory successfully mounted on the '{mount_point}' directory"
            )

        except Exception:
            logger.error(
                "Exception caught while attempting to load cached conjunctions data, deleting all cached data and returning empty data instead",
                exc_info=True,
                stack_info=True,
            )

            # Delete the existing conjunctions data.
            _cd_path.unlink()

            return conjunction_data(), None

        return ret, pj
    else:
        # No data in the cache, return empty data.
        logger.debug(
            "No existing conjunctions data found on startup, initialising empty data"
        )

        return conjunction_data(), None


# Initial setup of the conjunctions data.
_conj_lock = threading.Lock()
_conj, _pj = _conj_init_setup()


# Thread-safe getter for the conjunctions data.
def _get_conjunctions() -> tuple[conjunction_data, mz.polyjectory | None]:
    with _conj_lock:
        return _conj, _pj


# Thread-safe setter for the conjunctions data.
def _set_conjunctions(new_conj: conjunction_data, new_pj: mz.polyjectory) -> None:
    global _conj, _pj

    with _conj_lock:
        _conj = new_conj
        _pj = new_pj


# The data processor thread.
class _data_processor(threading.Thread):
    def __init__(self) -> None:
        super().__init__()
        # Setup the stop event.
        self._stop_event = threading.Event()
        # Setup the polyjectory archive. Here we store weak references
        # to polyjectories, associated to the *absolute* locations on disk
        # of their data. We will periodically iterate over this list and
        # remove the data of expired polyjectories.
        self._pj_archive: list[tuple[weakref.ref[mz.polyjectory], pathlib.Path]] = []

        # Fetch the polyjectory that was initialised at startup. If not None,
        # add it to the archive.
        cur_pj = _get_conjunctions()[1]
        if cur_pj is not None:
            self._pj_archive.append((weakref.ref(cur_pj), cur_pj.data_dir))

    def _cache_pj_cleanup(self) -> None:
        # This is a method to cleanup old/invalid polyjectory data in the cache.
        # Old data results from replacing an existing polyjectory with a new one
        # via _set_conjunctions(). Invalid data can arise if, after the creation
        # of a polyjectory, an exception was thrown before the polyjectory could be
        # registered in _pj_archive. This method is called periodically during
        # the main thread loop and before shutting the thread down.
        logger = logging.getLogger("arroyo")

        logger.debug("Cache cleanup started")

        # As a first step, we iterate over the polyjectories in the archive
        # and we remove the corresponding dir if the polyjectory is expired.
        new_pj_archive: list[tuple[weakref.ref[mz.polyjectory], pathlib.Path]] = []
        for wr, path in self._pj_archive:
            if wr() is None:
                logger.debug(f"Removing expired polyjectory data at the path '{path}'")
                # NOTE: this should never fail because there are no more references to the
                # polyjectory, meaning that there is nobody owning the data dir.
                shutil.rmtree(path)
            else:
                logger.debug(
                    f"Leaving non-expired polyjectory data at the path '{path}'"
                )
                new_pj_archive.append((wr, path))

        # Assign the new archive.
        self._pj_archive = new_pj_archive

        # Build a set out of the paths in the new archive.
        pj_path_set = set(_[1] for _ in self._pj_archive)

        # As a second step, we iterate over the contents of the cache dir, looking for
        # polyjectory dirs not appearing in pj_path_set. This can happen if an exception
        # was thrown in the main loop after the creation of a new polyjectory - the new
        # polyjectory was not registered in the archive but its data is persisting on disk.
        for cur_dir in _cache_dir.iterdir():
            # Make extra sure we are operating on a fully-resolved path.
            cur_dir = cur_dir.resolve()

            if not cur_dir.is_dir():
                # Not a directory, skip it.
                continue

            # Fetch the directory name.
            dir_name = cur_dir.parts[-1]

            if not dir_name.startswith(_pj_data_prefix):
                # Not a polyjectory data dir, skip it.
                continue

            if cur_dir not in pj_path_set:
                # Found a polyjectory data dir not showing up
                # in pj_path_set. Attempt to remove it.
                logger.debug(
                    f"Removing polyjectory data dir not showing up in the archive at the path '{cur_dir}'"
                )

                # NOTE: there is a small corner case here: if the polyjectory has not been garbage-collected,
                # on some platforms (e.g., Windows) deletion will fail because the data files have not been
                # unmapped yet. If this happens, log and ignore the error, we will try again next time.
                try:
                    shutil.rmtree(cur_dir)
                except Exception:
                    logger.debug(
                        f"Could not remove the polyjectory data dir '{cur_dir}' - will try again next time"
                    )

        logger.debug("Cache cleanup successfully finished")

    def run(self) -> None:
        # Main thread loop.
        logger = logging.getLogger("arroyo")

        logger.debug("socrates_comparison data processor thread started")

        # Maximum age for the conjunctions data (in seconds).
        MAX_AGE = 4 * 3600

        while not self._stop_event.is_set():
            try:
                # As a first step, we must determine if the current cached
                # data (if existing) is fresh enough.
                conj_age = None
                if _cd_path.exists():
                    # The conjunctions data file exists, fetch its modification time
                    # and determine its age in seconds.
                    conj_mtime = Time(val=os.path.getmtime(_cd_path), format="unix")
                    conj_age = (Time.now() - conj_mtime).to_value("s")

                if conj_age and conj_age < MAX_AGE:
                    # The existing conjunctions data is fresh enough, go to sleep
                    # and try again when the conjunctions data is outdated.
                    sleep_time = MAX_AGE - conj_age

                    # Trigger a cache cleanup before sleeping.
                    self._cache_pj_cleanup()

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
                n_missed_conj, df, pj, norad_ids, date_begin, date_end = (
                    _create_new_conj()
                )
                assert df.schema == _conj_df_schema
                ts_stop = Time.now()

                # Calculate the computation time.
                comp_time = (ts_stop - ts_start).to_value("s")

                # Change precisions for string conversions.
                ts_stop.precision = 0
                date_begin.precision = 0
                date_end.precision = 0

                # Fetch the directory name of the polyjectory data dir.
                pj_dir_name = pj.data_dir.parts[-1]

                # Build the new conjunctions dataclass.
                cdata = conjunction_data(
                    n_missed_conj=n_missed_conj,
                    df=df,
                    timestamp=ts_stop.utc.iso,
                    comp_time=comp_time,
                    date_begin=date_begin.utc.iso,
                    date_end=date_end.utc.iso,
                    pj_dir_name=pj_dir_name,
                    norad_ids=norad_ids,
                )

                logger.debug("New conjunctions data successfully created")

                # Save the conjunctions data into the cache.
                #
                # NOTE: here it is important to make sure that either the entire block
                # succeeds, or, in case of failure, that _cd_path is removed from disk.
                # This is because the mtime of _cd_path is used to establish whether or not
                # the conjunction data is fresh enough, and we want to avoid a situation
                # in which we have written to _cd_path but we never reached _set_conjunctions().
                # In such a case, we would be operating on old data without knowing it.
                #
                # In other words, the goal is to achieve consistency between the conjunctions
                # data saved to disk and the in-memory conjunctions data.
                try:
                    with open(_cd_path, "wb") as f:
                        pickle.dump(cdata, f)

                    logger.debug(
                        f"New conjunctions data successfully saved into the cache at '{_cd_path}'"
                    )

                    # Register the new polyjectory in the archive.
                    self._pj_archive.append((weakref.ref(pj), pj.data_dir))

                    # Assign the new conjunctions data.
                    # NOTE: normally this should trigger the old pj ref to die, unless
                    # someone else is holding a reference to it.
                    _set_conjunctions(cdata, pj)
                except Exception:
                    # NOTE: _cd_path could be missing, ignore errors if it is.
                    _cd_path.unlink(missing_ok=True)

                    # NOTE: at this point, we have polyjectory data written to disk
                    # that may or may not have been registered in the polyjectory archive.
                    # This is ok: in either case, the next cleanup should get rid of it.
                    # NOTE: after the removal of _cd_path, we are in a situation in which
                    # the on-disk conjunction data is empty, but the in-memory conjunction
                    # data is not. This is ok, as the lack of on-disk conjunction data will
                    # trigger the computation of new conjunctions at the next iteration.
                    raise

                logger.debug("New conjunctions data successfully assigned")

                # Trigger a cache cleanup before sleeping.
                self._cache_pj_cleanup()

                logger.debug(f"Sleeping for {MAX_AGE}s")

                self._stop_event.wait(MAX_AGE)
            except Exception:
                logger.error(
                    "Exception caught in the data processor thread, re-trying in 10 seconds",
                    exc_info=True,
                    stack_info=True,
                )

                # Trigger a cache cleanup before sleeping.
                self._cache_pj_cleanup()

                self._stop_event.wait(10)

        # Trigger a final cache cleanup before shutting down.
        self._cache_pj_cleanup()

    def _stop(self) -> None:
        logger = logging.getLogger("arroyo")

        logger.debug("Setting the stop event on the data processor thread")

        self._stop_event.set()
