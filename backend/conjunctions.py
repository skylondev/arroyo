import polars as pl
import datetime
from typing import Literal, Annotated
from pydantic import BaseModel, Field
from fastapi import APIRouter
import threading
import pathlib

router = APIRouter(
    prefix="/conjunctions",
    tags=["conjunctions"],
)

_conj_lock = threading.Lock()
_conj = pl.read_parquet(pathlib.Path(__file__).parent.resolve() / "conj.parquet")


def _get_conjunctions() -> pl.DataFrame:
    with _conj_lock:
        return _conj


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


NoradIDFilterFns = Literal["contains"]
DCAFilterFns = Literal["greaterThan", "lessThan", "between", "betweenInclusive"]
RelativeSpeedFilterFns = DCAFilterFns


class ConjunctionsFilterFns(BaseModel):
    norad_id_i: NoradIDFilterFns
    norad_id_j: NoradIDFilterFns
    dca: DCAFilterFns
    relative_speed: RelativeSpeedFilterFns


class NoradIDIFilter(BaseModel):
    id: Literal["norad_id_i"]
    value: str


class NoradIDJFilter(BaseModel):
    id: Literal["norad_id_j"]
    value: str


class DCAFilter(BaseModel):
    id: Literal["dca"]
    value: Annotated[list[str | None], Field(min_items=1, max_items=2)] | str


class RelativeSpeedFilter(BaseModel):
    id: Literal["relative_speed"]
    value: Annotated[list[str | None], Field(min_items=1, max_items=2)] | str


class ConjunctionsParams(BaseModel):
    begin: int = Field(..., ge=0)
    nrows: int = Field(..., ge=0, le=500)
    sorting: list[ColumnSort] = []
    columnFilterFns: ConjunctionsFilterFns
    columnFilters: list[
        NoradIDIFilter | NoradIDJFilter | DCAFilter | RelativeSpeedFilter
    ]


@router.post("/")
def get_conjunctions(
    params: ConjunctionsParams,
) -> Conjunctions:
    begin, nrows, sorting, columnFilterFns, columnFilters = params.model_dump().values()

    _conj = _get_conjunctions()

    df = _conj.lazy()

    if sorting:
        sort_cols = [_["id"] for _ in sorting]
        desc = [_["desc"] for _ in sorting]

        df = df.sort(by=sort_cols, descending=desc)

    # Fetch row range from pagination, collect and convert to dicts.
    rows = df[begin : begin + nrows].collect().to_dicts()

    return Conjunctions.model_validate(
        {
            "rows": rows,
            "tot_nrows": len(_conj),
        }
    )
