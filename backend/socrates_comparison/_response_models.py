from pydantic import BaseModel, Field


# NOTE: single encounter data point, containing a UTC date
# (in string format) and the corresponding distance between
# two objects involved in a conjunction.
class encounter_data_point(BaseModel):
    model_config = {"extra": "forbid"}

    date: str
    dist: float


# NOTE: this represents a single row in the conjunctions table
# that is sent to the frontend.
class single_row(BaseModel):
    model_config = {"extra": "forbid"}

    conj_index: int
    norad_ids: str
    object_names: str
    tca: str
    dca: float
    relative_speed: float
    tca_diff: float
    dca_diff: float
    relative_speed_diff: float
    # NOTE: this is the data which is visualised only
    # if the row is expanded.
    expanded_data: list[encounter_data_point]


# NOTE: this is the set of rows that will be sent to the frontend.
class rows_response(BaseModel):
    model_config = {"extra": "forbid"}

    # The conjunctions to be visualised in the current page.
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
