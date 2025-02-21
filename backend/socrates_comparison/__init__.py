from fastapi import APIRouter
from ._data import _get_conjunctions
from typing import Any, cast
from ._response_models import response
from ._request_models import request, range_filter_fns
import polars as pl
import logging

router = APIRouter(
    prefix="/socrates_comparison",
    tags=["socrates_comparison"],
)


@router.post("/", response_model=response)
def get_conjunctions(
    params: request,
) -> Any:
    logger = logging.getLogger("arroyo")

    logger.debug("Processing get_conjunctions() request")

    # Fetch the conjunction data.
    cdata = _get_conjunctions()

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
                    # Filtering based on either object name containing a substring.
                    substr = cast(str, filter_v)

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

    # Compress the norad id columns into a single string column.
    sub_df = sub_df.with_columns(
        pl.concat_str(
            pl.col("norad_id_i").cast(str),
            pl.col("norad_id_j").cast(str),
            separator=" | ",
        ).alias("norad_ids")
    ).drop("norad_id_i", "norad_id_j")

    # Compress object names and statuses into a single column.
    sub_df = sub_df.with_columns(
        pl.concat_str(
            pl.col("object_name_i") + " [" + pl.col("ops_status_i") + "]",
            pl.col("object_name_j") + " [" + pl.col("ops_status_j") + "]",
            separator=" | ",
        ).alias("object_names")
    ).drop("object_name_i", "ops_status_i", "object_name_j", "ops_status_j")

    # Convert the tca column to UTC ISO string with ms precision.
    sub_df = sub_df.with_columns(pl.col("tca").dt.strftime("%Y-%m-%d %H:%M:%S.%3f"))

    # Collect and convert to dicts the requested row range.
    rows = sub_df.collect().to_dicts()

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
        "conj_ts": cdata.timestamp,
        "comp_time": cdata.comp_time,
        "n_missed_conj": cdata.n_missed_conj,
        "date_begin": cdata.date_begin,
        "date_end": cdata.date_end,
    }

    logger.debug("get_conjunctions() request processed")

    return ret
