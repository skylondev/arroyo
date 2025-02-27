import mizuba as mz  # type: ignore
from ._data import conjunction_data
import polars as pl
import numpy as np


# Function to compute the data to be displayed when the rows are expanded.
def _compute_expanded_rows_data(
    pj: mz.polyjectory | None,
    cdata: conjunction_data,
    df: pl.DataFrame,
) -> list[list[dict[str, float]]]:
    # Exit early if there's no polyjectory (i.e., we have
    # no conjunctions data).
    if pj is None:
        return []

    # If pj is not None, the content of cdata must also be not None.
    assert cdata.norad_ids is not None

    # Fetch the norad IDs of the objects.
    norad_id_i = df["norad_id_i"].to_numpy()
    norad_id_j = df["norad_id_j"].to_numpy()

    # Compute the indices of the objects in the polyjectory.
    pj_id_i = np.searchsorted(cdata.norad_ids, norad_id_i).astype(np.uintp)
    pj_id_j = np.searchsorted(cdata.norad_ids, norad_id_j).astype(np.uintp)

    # Convert the date_begin/date_end strings into a polars datetime dataframe with nanoseconds precision.
    # The intent here is to perform time calculations without involving the Python datetime module.
    # NOTE: date_begin is also by definition the epoch of the polyjectory.
    date_df = pl.DataFrame(
        {"date_begin": cdata.date_begin, "date_end": cdata.date_end}
    ).with_columns(
        pl.col("date_begin")
        # NOTE: here we know that the precision of the begin/end datetimes
        # is seconds, as we enforce it in the data processing thread.
        .str.to_datetime(format="%Y-%m-%d %H:%M:%S", time_zone="UTC")
        .cast(pl.Datetime("ns", "UTC")),
        pl.col("date_end")
        .str.to_datetime(format="%Y-%m-%d %H:%M:%S", time_zone="UTC")
        .cast(pl.Datetime("ns", "UTC")),
    )

    # Compute the total duration of the conjunction detection interval (in days).
    duration = date_df.select(
        (pl.col("date_end") - pl.col("date_begin")).dt.total_nanoseconds().cast(float)
    ).item() / (86400 * 1e9)

    # Add columns to df containing the tca in days from the polyjectory
    # epoch according to mizuba.
    df = df.with_columns(
        (
            # NOTE: the single value in date_begin is splatted into the shape
            # of pl.col("tca") when subtracting.
            (pl.col("tca") - date_df["date_begin"]).dt.total_nanoseconds().cast(float)
            / (86400 * 1e9)
        ).alias("tca_days")
    )

    # Extract the tcas in days as a numpy array.
    tca_days = df["tca_days"].to_numpy()

    # Create timespans around the mizuba tcas.
    tspan_delta = 2.0
    N_tp = 50
    tspans = np.linspace(
        # NOTE: max/min are necessary in order to avoid
        # getting out of the polyjectory time bounds.
        np.maximum(tca_days - tspan_delta / 86400, 0.0),
        np.minimum(tca_days + tspan_delta / 86400, duration),
        N_tp,
    ).T
    tspans = np.ascontiguousarray(tspans)

    # Compute the states of the objects within the timespans.
    st_i = pj.state_meval(tspans, obj_idx=pj_id_i)
    st_j = pj.state_meval(tspans, obj_idx=pj_id_j)

    # Compute the mutual distances within the timespans.
    dist = np.linalg.norm(st_i[:, :, :3] - st_j[:, :, :3], axis=2)

    # We now proceed to construct the dataframe containing the output data.
    out_df_ld = pl.DataFrame().lazy()

    # First, we convert the time points in tspans to dates for visualisation.
    out_df_ld = out_df_ld.with_columns(
        list(
            (
                (pl.Series("tspan", tspans[idx]) * 86400 * 1e9)
                .cast(pl.UInt64)
                .cast(pl.Duration("ns"))
                + date_df["date_begin"]
            )
            .dt.strftime("%Y-%m-%d %H:%M:%S.%3f")
            .alias(f"date_{idx}")
            for idx in range(tspans.shape[0])
        )
    )

    # Then we add columns for the mutual distances.
    out_df_ld = out_df_ld.with_columns(
        list(pl.Series(f"dist_{idx}", dist[idx]) for idx in range(tspans.shape[0]))
    )

    # Finally, we collect and transform into the output format.
    out_df = out_df_ld.collect()

    out = [
        out_df.select(
            [
                pl.col(f"date_{idx}").alias("date"),
                pl.col(f"dist_{idx}").alias("dist"),
            ]
        ).to_dicts()
        for idx in range(tspans.shape[0])
    ]

    return out
