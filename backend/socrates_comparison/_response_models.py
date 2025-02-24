from pydantic import BaseModel, Field


# NOTE: this represents a single row in the conjunctions table
# that is sent to the frontend.
class single_row(BaseModel):
    model_config = {"extra": "forbid"}

    norad_ids: str
    object_names: str
    tca: str
    dca: float
    relative_speed: float
    tca_diff: float
    dca_diff: float
    relative_speed_diff: float


# NOTE: this is the response that will be sent to the frontend.
class response(BaseModel):
    model_config = {"extra": "forbid"}

    # The list of conjunctions to be visualised in the current page.
    rows: list[single_row]
    # The total number of rows.
    tot_nrows: int = Field(..., ge=0)
    # The total number of conjunctions.
    tot_nconj: int = Field(..., ge=0)
    # The timestamp.
    conj_ts: str | None
    # The total computation time (in seconds).
    comp_time: float
    # The number of missed conjunctions.
    n_missed_conj: int
    # The time period covered by the computation.
    date_begin: str | None
    date_end: str | None
