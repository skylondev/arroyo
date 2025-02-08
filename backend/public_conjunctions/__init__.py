from fastapi import APIRouter
from ._data import _get_conjunctions
from typing import Any
from ._models import conjunctions_params, conjunctions

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

    if params.sorting:
        sort_cols = [_.id for _ in params.sorting]
        desc = [_.desc for _ in params.sorting]

        df = df.sort(by=sort_cols, descending=desc)

    # Fetch the requested row range, collect and convert to dicts.
    rows = df[params.begin : params.begin + params.nrows].collect().to_dicts()

    return {
        "rows": rows,
        "tot_nrows": len(_conj),
    }
