from fastapi import APIRouter
from ._data import _get_conjunctions
from typing import Any, cast
from ._response_models import rows_response
from ._request_models import rows_request, range_filter_fns
from ._expanded_rows_data import _compute_expanded_rows_data
import polars as pl
import logging

router = APIRouter(
    prefix="/socrates_comparison",
    tags=["socrates_comparison"],
)


@router.post("/", response_model=rows_response)
def get_conjunctions(
    params: rows_request,
) -> Any:
    logger = logging.getLogger("arroyo")

    logger.debug("Processing get_conjunctions() request")

    # Fetch the conjunction data.
    cdata, pj = _get_conjunctions()

    # Fetch the dataframe and a lazy version of it.
    conj = cdata.df
    df = conj.lazy()

    # If we have filtering to do, we will collect the filtering expressions
    # here and apply them all at once later.
    filters: list[pl.Expr] = []

    # Handle column filtering.
    if params.filters:
        for cur_filter in params.filters:
            # Extract the column's name, the filter function
            # and the filter value.
            col = cur_filter.id
            filter_f = getattr(params.filter_fns, col)
            filter_v = cur_filter.value

            # The filter values are passed as strings or lists of strings.
            # Try to convert them to the appropriate types based on the column.
            # If the conversion fails, ignore the filter and move to the next column.
            match col:
                case "norad_ids":
                    # Filtering based on the exact match of one of the two norad IDs.
                    # We need to convert the filter value to an int.
                    try:
                        norad_id = int(cast(str, filter_v))

                        # NOTE: negative norad IDs are not valid.
                        if norad_id < 0:
                            continue
                    except Exception:
                        continue

                    # Add the filter.
                    filters.append(
                        (pl.col("norad_id_i") == norad_id)
                        | (pl.col("norad_id_j") == norad_id)
                    )

                case "object_names":
                    # Filtering based on object names.
                    substr = cast(str, filter_v)

                    # Check if the user specified names for one or both objects.
                    str_l = substr.split(":")

                    if len(str_l) == 2:
                        # The user passed a string of type "a:b". We interpret this
                        # as the user asking for conjunctions in which one object name
                        # contains "a" and the other object name contains "b".

                        # Strip the individual names.
                        str_l = list(s.strip() for s in str_l)

                        # Apply the filter.
                        filters.append(
                            (
                                (
                                    pl.col("object_name_i").str.contains_any(
                                        [str_l[0]], ascii_case_insensitive=True
                                    )
                                )
                                & (
                                    pl.col("object_name_j").str.contains_any(
                                        [str_l[1]], ascii_case_insensitive=True
                                    )
                                )
                            )
                            | (
                                (
                                    pl.col("object_name_i").str.contains_any(
                                        [str_l[1]], ascii_case_insensitive=True
                                    )
                                )
                                & (
                                    pl.col("object_name_j").str.contains_any(
                                        [str_l[0]], ascii_case_insensitive=True
                                    )
                                )
                            )
                        )
                    else:
                        # Interpret substr as a single object name.

                        # Strip it.
                        substr = substr.strip()

                        # Apply the filter.
                        filters.append(
                            (
                                pl.col("object_name_i").str.contains_any(
                                    [substr], ascii_case_insensitive=True
                                )
                            )
                            | (
                                pl.col("object_name_j").str.contains_any(
                                    [substr], ascii_case_insensitive=True
                                )
                            )
                        )

                case _:
                    # In all the other cases, we are dealing with range-based filtering.
                    if isinstance(filter_v, list):
                        # A filter of type 'list' means that the filter function
                        # is 'between' or 'between_inclusive'.

                        # Attempt to convert the filter value to a list of floats.
                        try:
                            value_range = list(float(_) for _ in filter_v)
                        except Exception:
                            continue

                        # Add the filter.
                        if filter_f == range_filter_fns.between:
                            filters.append(pl.col(col) > value_range[0])
                            filters.append(pl.col(col) < value_range[1])
                        else:
                            assert filter_f == range_filter_fns.between_inclusive
                            filters.append(pl.col(col) >= value_range[0])
                            filters.append(pl.col(col) <= value_range[1])
                    else:
                        # A filter of type 'str' means that the filter function
                        # is greater/less_than.

                        # Attempt to convert the filter value to a float.
                        try:
                            value = float(filter_v)
                        except Exception:
                            continue

                        # Add the filter.
                        if filter_f == range_filter_fns.greater_than:
                            filters.append(pl.col(col) > value)
                        else:
                            assert filter_f == range_filter_fns.less_than
                            filters.append(pl.col(col) < value)

    # Apply the filter(s), if any.
    if filters:
        df = df.filter(*filters)

    # Handle sorting.
    if params.sorting:
        sort_cols = [_.id for _ in params.sorting]
        desc = [_.desc for _ in params.sorting]

        df = df.sort(by=sort_cols, descending=desc)

    # Fetch the requested row range.
    sub_df = df[params.begin : params.begin + params.nrows]

    # Collect it.
    sub_df_coll = sub_df.collect()

    # Compute the expanded rows data.
    # NOTE: as an alternative to computing this data for each request, we could
    # precompute it in the data processor thread.
    expanded_rows_data = _compute_expanded_rows_data(pj, cdata, sub_df_coll)

    # Compress the norad id columns into a single string column.
    sub_df_coll = sub_df_coll.with_columns(
        pl.concat_str(
            pl.col("norad_id_i").cast(str),
            pl.col("norad_id_j").cast(str),
            separator=" | ",
        ).alias("norad_ids")
    ).drop("norad_id_i", "norad_id_j")

    # Compress object names and statuses into a single column.
    sub_df_coll = sub_df_coll.with_columns(
        pl.concat_str(
            pl.col("object_name_i") + " [" + pl.col("ops_status_i") + "]",
            pl.col("object_name_j") + " [" + pl.col("ops_status_j") + "]",
            separator=" | ",
        ).alias("object_names")
    ).drop("object_name_i", "ops_status_i", "object_name_j", "ops_status_j")

    # Convert the tca column to UTC ISO string with ms precision.
    sub_df_coll = sub_df_coll.with_columns(
        pl.col("tca").dt.strftime("%Y-%m-%d %H:%M:%S.%3f")
    )

    # Convert to dicts.
    rows = sub_df_coll.to_dicts()

    # Add the expanded rows data.
    rows = [
        {
            **row,
            "expanded_data": expanded_rows_data[row_idx],
        }
        for row_idx, row in enumerate(rows)
    ]

    ret = {
        "rows": rows,
        # NOTE: this is a contraption to compute the length of a lazy dataframe:
        #
        # https://stackoverflow.com/questions/75523498/python-polars-how-to-get-the-row-count-of-a-lazyframe
        #
        # It is important that we do this instead of just len(conj)
        # in order to account for filtering.
        "tot_nrows": df.select(pl.len()).collect().item(),
        "tot_nconj": len(conj),
        "threshold": cdata.threshold,
        "conj_ts": cdata.timestamp,
        "comp_time": cdata.comp_time,
        "n_missed_conj": cdata.n_missed_conj,
        "date_begin": cdata.date_begin,
        "date_end": cdata.date_end,
    }

    logger.debug("get_conjunctions() request processed")

    return ret
