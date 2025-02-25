import polars as pl
import requests as rq  # type: ignore
from io import StringIO
from astropy.time import Time  # type: ignore
import numpy as np
import re
import logging
from concurrent.futures import ThreadPoolExecutor
import mizuba as mz  # type: ignore


# Helper to download the orbit data used in the latest socrates run.
def _download_socrates_on_orbit() -> pl.DataFrame:
    logger = logging.getLogger("arroyo")

    logger.debug("Downloading the latest socrates on-orbit data")

    # Download and parse the latest data.
    download_url = r"https://celestrak.org/pub/on-orbit.csv"
    download_response = rq.get(download_url)
    on_orbit = pl.read_csv(
        StringIO(download_response.text),
        # NOTE: most data in this column is '0', and polars
        # incorrectly infers an integral data type.
        schema_overrides={"MEAN_MOTION_DDOT": pl.Float64},
    )

    # Rename the columns.
    rename_map = {
        "NORAD_CAT_ID": "norad_id",
        "MEAN_MOTION": "n0",
        "ECCENTRICITY": "e0",
        "INCLINATION": "i0",
        "RA_OF_ASC_NODE": "node0",
        "ARG_OF_PERICENTER": "omega0",
        "MEAN_ANOMALY": "m0",
        "BSTAR": "bstar",
    }
    on_orbit = on_orbit.rename(rename_map)

    # Setup the epoch jd columns.
    utc_dates = Time(on_orbit["EPOCH"], format="isot", scale="utc", precision=9)
    on_orbit = on_orbit.with_columns(
        pl.Series(name="epoch_jd1", values=utc_dates.jd1),
        pl.Series(name="epoch_jd2", values=utc_dates.jd2),
    )

    # Change units of measurement.
    deg2rad = 2.0 * np.pi / 360.0
    on_orbit = on_orbit.with_columns(
        n0=pl.col("n0") * (2.0 * np.pi / 1440.0),
        i0=pl.col("i0") * deg2rad,
        node0=pl.col("node0") * deg2rad,
        omega0=pl.col("omega0") * deg2rad,
        m0=pl.col("m0") * deg2rad,
    )

    logger.debug("Socrates on-orbit data successfully downloaded and parsed")

    return on_orbit


# Helper to download the conjunctions detected by socrates.
def _download_socrates_conjunctions() -> pl.DataFrame:
    logger = logging.getLogger("arroyo")

    logger.debug("Downloading the latest socrates conjunctions data")

    # Download the conjunction data from socrates.
    download_url = r"https://celestrak.org/SOCRATES/sort-minRange.csv"
    download_response = rq.get(download_url)
    soc_df = pl.read_csv(StringIO(download_response.text))

    # Ensure correct ordering of norad IDs.
    soc_df = soc_df.with_columns(
        norad_id_i=pl.when(pl.col("NORAD_CAT_ID_1") < pl.col("NORAD_CAT_ID_2"))
        .then(pl.col("NORAD_CAT_ID_1"))
        .otherwise(pl.col("NORAD_CAT_ID_2")),
        norad_id_j=pl.when(pl.col("NORAD_CAT_ID_1") < pl.col("NORAD_CAT_ID_2"))
        .then(pl.col("NORAD_CAT_ID_2"))
        .otherwise(pl.col("NORAD_CAT_ID_1")),
    )

    # Rename.
    soc_df = soc_df.with_columns(
        pl.col("TCA").alias("tca"),
        pl.col("TCA_RANGE").alias("dca"),
        pl.col("TCA_RELATIVE_SPEED").alias("relative_speed"),
    )

    # Transform the tca column into datetime.
    soc_df = soc_df.with_columns(
        pl.col("tca")
        # NOTE: parse with ms resolution from socrates, then cast
        # to nanoseconds resolution.
        .str.to_datetime(format="%Y-%m-%d %H:%M:%S%.3f", time_zone="UTC")
        .cast(pl.Datetime("ns", "UTC"))
    )

    # Clean up.
    soc_df = soc_df.drop(
        "NORAD_CAT_ID_1",
        "NORAD_CAT_ID_2",
        "OBJECT_NAME_1",
        "OBJECT_NAME_2",
        "DSE_1",
        "DSE_2",
        "MAX_PROB",
        "DILUTION",
        "TCA",
        "TCA_RANGE",
        "TCA_RELATIVE_SPEED",
    )
    soc_df = soc_df.cast({"norad_id_i": pl.UInt64, "norad_id_j": pl.UInt64})

    logger.debug("Socrates conjunctions data successfully downloaded and parsed")

    return soc_df


# Helper to infer the time range used in the latest socrates run.
def _determine_socrates_time_range() -> tuple[Time, Time]:
    logger = logging.getLogger("arroyo")

    logger.debug("Determining the socrates start/stop time range")

    # Download the 'search' web page.
    download_url = r"https://celestrak.org/SOCRATES/search.php"
    download_response = rq.get(download_url)
    socrates_search = download_response.text

    # We will be parsing the web page source code in order to infer the time range.

    # Build a dictionary to associate a mont abbreviation to a number.
    months_list = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    months_dict = {_[1]: _[0] for _ in enumerate(months_list, start=1)}

    # This is the line we are looking for. The start/stop dates are expected to be enclosed in the two groups.
    start_end_pattern = re.compile(
        ".*Computation Interval: Start = (.*) UTC, Stop = (.*) UTC.*", re.IGNORECASE
    )

    date_begin = None
    date_end = None
    err_msg = (
        "Error detected while trying to determine the socrates start/stop time range"
    )
    for ln in socrates_search.split("\n"):
        m = start_end_pattern.match(ln)

        if m:
            # We found the line we were looking for. Now we need to parse the start/stop groups.
            start_str, end_str = m.groups()
            # NOTE: we are trying to parse dates in this format: "2025 Feb 10 08:00:00".
            data_pattern = re.compile(
                rf"^(\d{{4}}) ({'|'.join(months_list)}) (\d{{2}}) (\d{{2}}):(\d{{2}}):(\d{{2}})$",
                re.IGNORECASE,
            )

            m_start = data_pattern.match(start_str)
            m_end = data_pattern.match(end_str)

            if not m_start or not m_end:
                raise ValueError(err_msg)

            # Convert the month abbreviations into numbers.
            s_groups = list(m_start.groups())
            e_groups = list(m_end.groups())
            s_groups[1] = months_dict[s_groups[1]]
            e_groups[1] = months_dict[e_groups[1]]

            # Build start/stop times as astropy Time objects.
            date_begin = Time(
                f"{s_groups[0]}-{s_groups[1]}-{s_groups[2]} {s_groups[3]}:{s_groups[4]}:{s_groups[5]}",
                format="iso",
                scale="utc",
                precision=9,
            )
            date_end = Time(
                f"{e_groups[0]}-{e_groups[1]}-{e_groups[2]} {e_groups[3]}:{e_groups[4]}:{e_groups[5]}",
                format="iso",
                scale="utc",
                precision=9,
            )

            logger.debug(
                f"Socrates start/stop time range successfully determined: [{date_begin}, {date_end}]"
            )

            return date_begin, date_end

    # NOTE: if we get here, it means that we did not find the line containing
    # the start/stop dates.
    raise ValueError(err_msg)


# Initial setup of the mizuba conjunctions dataframe.
def _create_mz_conj_init(
    cj: mz.conjunctions, pj: mz.polyjectory, norad_ids: np.typing.NDArray[np.uint64]
) -> pl.DataFrame:
    # Fetch the array of conjunctions.
    conj = cj.conjunctions

    # Fetch the norad ids for the objects involved in the conjunctions.
    norad_id_i = norad_ids[conj["i"]]
    norad_id_j = norad_ids[conj["j"]]

    # Build the tca column, representing it as a ISO UTC
    # string with ns resolution.
    pj_epoch1, pj_epoch2 = pj.epoch
    tca = Time(
        val=pj_epoch1,
        val2=pj_epoch2 + conj["tca"],
        format="jd",
        scale="tai",
        precision=9,
    ).utc.iso

    # Build the dca column.
    dca = conj["dca"]

    # Build the relative speed column.
    rel_speed = np.linalg.norm(conj["vi"] - conj["vj"], axis=1)

    # Start assembling the dataframe.
    cdf = pl.DataFrame(
        {
            "norad_id_i": norad_id_i,
            "norad_id_j": norad_id_j,
            "tca": tca,
            "dca": dca,
            "relative_speed": rel_speed,
        }
    )

    # Transform the tca column into datetime.
    cdf = cdf.with_columns(
        pl.col("tca").str.to_datetime(format="%Y-%m-%d %H:%M:%S%.9f", time_zone="UTC")
    )

    return cdf


# Augment a mizuba conjunctions dataframe with data from the satcat.
def _create_mz_conj_satcat_augment(
    cdf: pl.DataFrame, satcat: pl.DataFrame
) -> pl.DataFrame:
    # Attach the status codes from satcat.
    # NOTE: in the joined dataframe, replace all null status codes
    # with '?', following https://celestrak.org/satcat/status.php.
    cdf = (
        cdf.join(
            satcat.select(["NORAD_CAT_ID", "OPS_STATUS_CODE"]),
            how="left",
            left_on="norad_id_i",
            right_on="NORAD_CAT_ID",
        )
        .with_columns(
            pl.col("OPS_STATUS_CODE").fill_null(pl.lit("?")).alias("ops_status_i")
        )
        .drop("OPS_STATUS_CODE")
    )
    cdf = (
        cdf.join(
            satcat.select(["NORAD_CAT_ID", "OPS_STATUS_CODE"]),
            how="left",
            left_on="norad_id_j",
            right_on="NORAD_CAT_ID",
        )
        .with_columns(
            pl.col("OPS_STATUS_CODE").fill_null(pl.lit("?")).alias("ops_status_j")
        )
        .drop("OPS_STATUS_CODE")
    )

    # Attach the object names from satcat.
    # NOTE: in the joined dataframe, replace all null object names
    # with 'unknown', otherwise having potentially-null object names complicates
    # the schema of the response to the frontend.
    cdf = (
        cdf.join(
            satcat.select(["NORAD_CAT_ID", "OBJECT_NAME"]),
            how="left",
            left_on="norad_id_i",
            right_on="NORAD_CAT_ID",
        )
        .with_columns(
            pl.col("OBJECT_NAME").fill_null(pl.lit("unknown")).alias("object_name_i")
        )
        .drop("OBJECT_NAME")
    )
    cdf = (
        cdf.join(
            satcat.select(["NORAD_CAT_ID", "OBJECT_NAME"]),
            how="left",
            left_on="norad_id_j",
            right_on="NORAD_CAT_ID",
        )
        .with_columns(
            pl.col("OBJECT_NAME").fill_null(pl.lit("unknown")).alias("object_name_j")
        )
        .drop("OBJECT_NAME")
    )

    return cdf


# Create a dataframe merging the results of mizuba's and socrates' conjunction detection.
def _create_mz_conj_merged(
    cdf: pl.DataFrame,
    soc_df: pl.DataFrame,
    date_begin: Time,
) -> tuple[int, pl.DataFrame]:
    # Construct the joined dataframe. This will match all the conjunctions
    # detected by socrates to corresponding conjunctions detected by mizuba.
    cdf = soc_df.sort("tca").join_asof(
        cdf.sort("tca"),
        by=["norad_id_i", "norad_id_j"],
        on="tca",
        strategy="nearest",
        coalesce=False,
        check_sortedness=True,
    )

    # Determine the number of missed conjunctions (hopefully zero).
    n_missed_conj = cdf.select(pl.col("tca_right").is_null().sum()).item()

    # Drop the missed conjunctions. If we do not do this, we will have issues
    # because the dataframe will contain null values.
    cdf = cdf.filter(~pl.col("tca_right").is_null())

    # Add columns with tca, dca and relative speed differences.
    cdf = cdf.with_columns(
        (
            (pl.col("tca") - pl.col("tca_right"))
            .abs()
            .cast(pl.Duration("ns"))
            .cast(float)
            / 1e6
        ).alias("tca_diff"),
        ((pl.col("dca") - pl.col("dca_right")) * 1000.0).abs().alias("dca_diff"),
        ((pl.col("relative_speed") - pl.col("relative_speed_right")) * 1000.0)
        .abs()
        .alias("relative_speed_diff"),
    )

    # Add columns representing the tcas measured in days from date_begin.
    # NOTE: here we are creating a dataframe containing a single value of type
    # pl.Datetime. The intent is to convert from an astropy Time object into
    # the polars datatype without involving the datetime Python module (which we
    # want to avoid due to its deficiencies).
    date_begin_df = pl.DataFrame({"date": date_begin.iso}).with_columns(
        pl.col("date")
        .str.to_datetime(format="%Y-%m-%d %H:%M:%S%.9f", time_zone="UTC")
        .cast(pl.Datetime("ns", "UTC"))
    )
    cdf = cdf.with_columns(
        (
            # NOTE: the single value in date_begin_df will be broadcasted
            # (i.e., splatted) to the shape of tca_right in this subtraction.
            (pl.col("tca_right") - date_begin_df["date"])
            .dt.total_nanoseconds()
            .cast(float)
            / (86400 * 1e9)
        ).alias("tca_days"),
        (
            (pl.col("tca") - date_begin_df["date"]).dt.total_nanoseconds().cast(float)
            / (86400 * 1e9)
        ).alias("tca_socrates_days"),
    )

    # Rename the foo_right columns to foo. That is, we are keeping only the results
    # from mizuba (except for tca_socrates_days).
    cdf = cdf.with_columns(
        pl.col("tca_right").alias("tca"),
        pl.col("dca_right").alias("dca"),
        pl.col("relative_speed_right").alias("relative_speed"),
    ).drop("tca_right", "dca_right", "relative_speed_right")

    # Reorder the columns.
    cdf = cdf.select(
        "norad_id_i",
        "norad_id_j",
        "object_name_i",
        "object_name_j",
        "ops_status_i",
        "ops_status_j",
        "tca",
        "dca",
        "relative_speed",
        "tca_diff",
        "dca_diff",
        "relative_speed_diff",
        "tca_days",
        "tca_socrates_days",
    )

    return n_missed_conj, cdf


# Helper to construct a conjunctions dataframe from the results of mizuba's
# conjunction detection, the satcat and the results of socrates' conjunction
# detection.
def _create_mz_conj(
    cj: mz.conjunctions,
    pj: mz.polyjectory,
    norad_ids: np.typing.NDArray[np.uint64],
    satcat: pl.DataFrame,
    soc_df: pl.DataFrame,
    date_begin: Time,
) -> tuple[int, pl.DataFrame]:
    # Initial setup.
    cdf = _create_mz_conj_init(cj, pj, norad_ids)

    # Augment with properties from the satcat.
    cdf = _create_mz_conj_satcat_augment(cdf, satcat)

    # Merge the results with socrates' and return
    return _create_mz_conj_merged(cdf, soc_df, date_begin)


# Main function to create a new conjunctions dataframe.
def _create_new_conj() -> (
    tuple[int, pl.DataFrame, mz.polyjectory, np.typing.NDArray[np.uint64], Time, Time]
):
    from ._data import _cache_dir

    logger = logging.getLogger("arroyo")

    # Fetch the data from celestrak.
    with ThreadPoolExecutor() as executor:
        soc_df_fut = executor.submit(_download_socrates_conjunctions)
        on_orbit_fut = executor.submit(_download_socrates_on_orbit)
        time_range_fut = executor.submit(_determine_socrates_time_range)
        satcat_fut = executor.submit(mz.data_sources.download_satcat_celestrak)
    soc_df = soc_df_fut.result()
    on_orbit = on_orbit_fut.result()
    date_begin, date_end = time_range_fut.result()

    # NOTE: because we are downloading different (but related) datasets independently
    # of each other, there is a small chance of inconsistencies if one dataset is updated
    # on celestrak in the middle of our parallel downloads. If that happens, we will throw
    # an exception and we will let the data processor thread retry.
    if soc_df["tca"].min() < date_begin or soc_df["tca"].max() > date_end:
        raise ValueError(
            f"Inconsistent socrates data: conjunctions outside the expected range [{date_begin}, {date_end}] were detected"
        )
    if (
        not soc_df["norad_id_i"].is_in(on_orbit["norad_id"]).all()
        or not soc_df["norad_id_j"].is_in(on_orbit["norad_id"]).all()
    ):
        raise ValueError(
            "Inconsistent socrates data: conjunctions involving objects not present in the on-orbit data were detected"
        )

    logger.debug("Building the polyjectory")

    # Build the polyjectory, using the cache dir as tmpdir and making
    # sure that the data persists to disk.
    pj, norad_ids = mz.make_sgp4_polyjectory(
        on_orbit, date_begin.jd, date_end.jd, tmpdir=_cache_dir, persist=True
    )

    # Mark the norad IDs array as read-only.
    norad_ids.flags.writeable = False

    logger.debug(f"New polyjectory built with data dir '{pj.data_dir}'")

    logger.debug("Running conjunction detection")

    # Run conjunction detection.
    cj = mz.conjunctions(pj, 5.0, 3.0 / 1440.0)

    logger.debug("Creating new conjunctions dataframe")

    # Construct and return the conjunctions dataframe.
    ret = _create_mz_conj(cj, pj, norad_ids, satcat_fut.result(), soc_df, date_begin)

    logger.debug("New conjunctions dataframe created")

    return *ret, pj, norad_ids, date_begin, date_end
