from fastapi import APIRouter
from ._data import _get_conjunctions
from typing import Any, cast
from ._models import conjunctions_params, conjunctions, range_filter_fns
import polars as pl

router = APIRouter(
    prefix="/public_conjunctions",
    tags=["public_conjunctions"],
)


@router.post("/", response_model=conjunctions)
def get_conjunctions(
    params: conjunctions_params,
) -> Any:
    _conj = _get_conjunctions()

    df = _conj.lazy()

    # If we have filtering to do, we will collect the filtering expressions
    # here and apply them all at once later.
    filters: list[pl.Expr] = []

    # Handle global filtering.
    if params.global_filter is not None:
        gf_str = params.global_filter.strip()
        filters.append(
            (
                pl.col("norad_id_i")
                .cast(str)
                .str.contains_any([gf_str], ascii_case_insensitive=True)
            )
            | (
                pl.col("norad_id_j")
                .cast(str)
                .str.contains_any([gf_str], ascii_case_insensitive=True)
            )
        )

    # Handle column filtering.
    if params.conjunctions_filters:
        for cur_filter in params.conjunctions_filters:
            # Extract the column's name, the filter function
            # and the filter value.
            col = cur_filter.id
            filter_f = getattr(params.conjunctions_filter_fns, col)
            filter_v = cur_filter.value

            # The filter values are passed as strings.
            # Try to convert them to the appropriate types based on the column.
            # If the conversion fails, ignore the filter and move to the next column.
            match col:
                case "norad_id_i" | "norad_id_j":
                    # Filtering based on exact match of norad IDs.
                    # We need to convert the filter value to an int.
                    try:
                        norad_id = int(cast(str, filter_v))

                        # NOTE: negative norad IDs are not valid.
                        if norad_id < 0:
                            continue
                    except Exception:
                        continue

                    # Add the filter.
                    filters.append(pl.col(col) == norad_id)

                case "dca" | "relative_speed":
                    if isinstance(filter_v, list):
                        # A filter of type 'list' means that the filter function
                        # is 'between' (possibly inclusive).

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

    # Fetch the requested row range, collect and convert to dicts.
    rows = df[params.begin : params.begin + params.nrows].collect().to_dicts()

    return {
        "rows": rows,
        # NOTE: this is a contraption to compute the length of a lazy dataframe:
        #
        # https://stackoverflow.com/questions/75523498/python-polars-how-to-get-the-row-count-of-a-lazyframe
        #
        # It is important that we do this instead of just len(_conj)
        # because the filtering may have changed the total number of rows
        # in the dataframe.
        "tot_nrows": df.select(pl.len()).collect().item(),
    }
