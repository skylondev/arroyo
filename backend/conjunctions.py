import polars as pl
import datetime
from typing import Literal
from pydantic import BaseModel, Field
from fastapi import APIRouter

router = APIRouter(
    prefix="/conjunctions",
    tags=["conjunctions"],
)


def _get_conjunctions() -> pl.DataFrame:
    import pathlib

    cur_dir = pathlib.Path(__file__).parent.resolve()

    return pl.read_parquet(cur_dir / "conj.parquet")


class Conjunction(BaseModel):
    norad_id_i: int = Field(..., ge=0)
    norad_id_j: int = Field(..., ge=0)
    tca: datetime.datetime
    tca_pj: float
    dca: float
    relative_speed: float


class Conjunctions(BaseModel):
    rows: list[Conjunction]
    tot_nrows: int = Field(..., ge=0)


class ColumnSort(BaseModel):
    desc: bool
    # NOTE: it looks like here for static checking/validation we cannot
    # pass a dynamically-computed list of strings.
    id: Literal["norad_id_i", "norad_id_j", "tca", "tca_pj", "dca", "relative_speed"]


class ConjunctionsParams(BaseModel):
    begin: int = Field(..., ge=0)
    nrows: int = Field(..., ge=0)
    sorting: list[ColumnSort] = []


@router.post("/")
def get_conjunctions(
    params: ConjunctionsParams,
) -> Conjunctions:
    begin, nrows, sorting = params.model_dump().values()

    _conj = _get_conjunctions()

    df = _conj.lazy()

    if sorting:
        sort_cols = [_["id"] for _ in sorting]
        desc = [_["desc"] for _ in sorting]

        df = df.sort(by=sort_cols, descending=desc)

    # Fetch row range from pagination, collect and convert
    # to dicts.
    rows = df[begin : begin + nrows].collect().to_dicts()

    return Conjunctions.model_validate(
        {
            "rows": rows,
            "tot_nrows": len(_conj),
        }
    )
