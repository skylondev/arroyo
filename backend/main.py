from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import polars as pl
from typing import Literal
from pydantic import BaseModel, Field
import datetime

origins = [
    "http://localhost:5173",
]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

conj = pl.read_parquet("conj.parquet")


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


@app.post("/conjunctions/")
def get_conjunctions(
    params: ConjunctionsParams,
) -> Conjunctions:
    begin, nrows, sorting = params.model_dump().values()

    df = conj.lazy()

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
            "tot_nrows": len(conj),
        }
    )
