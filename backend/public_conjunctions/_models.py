import datetime
from pydantic import BaseModel, Field, model_validator
from typing import Literal, Annotated, Self
from enum import Enum


# NOTE: this represents a single row in the conjunctions table.
class conjunction(BaseModel):
    norad_id_i: int = Field(..., ge=0)
    norad_id_j: int = Field(..., ge=0)
    tca: datetime.datetime
    tca_pj: float
    dca: float
    relative_speed: float


# NOTE: this is the response that will be sent to the frontend.
# It consists of the list of conjunctions to be visualised in the current
# page plus the total number of conjunctions.
class conjunctions(BaseModel):
    rows: list[conjunction]
    tot_nrows: int = Field(..., ge=0)


# NOTE: this is sent by the frontend. It represent a column with
# respect to which the sorting will take place and whether or not
# the sorting should be descending.
class column_sort(BaseModel):
    desc: bool
    # NOTE: Here we would prefer to in general to fetch the list of fields from the
    # conjunction model, but it looks like for static checking/validation we cannot
    # pass a dynamically-computed list of strings.
    id: Literal["norad_id_i", "norad_id_j", "tca", "tca_pj", "dca", "relative_speed"]


norad_id_filter_fns = Literal["equals"]


# NOTE: use enum in place of Literal so that we can fetch
# the enumerations at runtime.
class range_filter_fns(Enum):
    # NOTE: camel case on the strings here as they are
    # coming from the frontend.
    greater_than = "greaterThan"
    less_than = "lessThan"
    between = "between"
    between_inclusive = "betweenInclusive"


dca_filter_fns = range_filter_fns
relative_speed_filter_fns = range_filter_fns


class conjunctions_filter_fns(BaseModel):
    norad_id_i: norad_id_filter_fns
    norad_id_j: norad_id_filter_fns
    dca: dca_filter_fns
    relative_speed: relative_speed_filter_fns


class norad_id_i_filter(BaseModel):
    id: Literal["norad_id_i"]
    value: str


class norad_id_j_filter(BaseModel):
    id: Literal["norad_id_j"]
    value: str


class dca_filter(BaseModel):
    id: Literal["dca"]
    value: Annotated[list[str | None], Field(min_length=2, max_length=2)] | str


class relative_speed_filter(BaseModel):
    id: Literal["relative_speed"]
    value: Annotated[list[str | None], Field(min_length=2, max_length=2)] | str


class conjunctions_params(BaseModel):
    begin: int = Field(..., ge=0)
    nrows: int = Field(..., ge=0, le=500)
    sorting: list[column_sort]
    conjunctions_filter_fns: conjunctions_filter_fns
    conjunctions_filters: list[
        norad_id_i_filter | norad_id_j_filter | dca_filter | relative_speed_filter
    ]

    @model_validator(mode="after")
    def check_unique_filter_ids(self) -> Self:
        # The ids in conjunctions_filters must be unique (that is,
        # we cannot be applying two filters to the same column).
        id_list = list(_.id for _ in self.conjunctions_filters)
        if len(set(id_list)) != len(id_list):
            raise ValueError("The list of ids in 'conjunctions_filters' must be unique")

        return self

    @model_validator(mode="after")
    def check_filters_consistency(self) -> Self:
        # For the DCA and relative speed filters, we have to
        # make sure that the selected filter function is consistent
        # with the filter.
        for flt in self.conjunctions_filters:
            if flt.id in ["dca", "relative_speed"]:
                cur_flt_fn = getattr(self.conjunctions_filter_fns, flt.id)

                if cur_flt_fn in [
                    range_filter_fns.between,
                    range_filter_fns.between_inclusive,
                ]:
                    # The filter function is 'between'/'between_inclusive':
                    # the filter value must be a list.
                    if not isinstance(flt.value, list):
                        raise ValueError(
                            f"A non-list filter value was detected for the '{cur_flt_fn}' filter function"
                        )
                else:
                    # The filter function is 'less_than'/'greater_than':
                    # the filter value must be a list.
                    if not isinstance(flt.value, str):
                        raise ValueError(
                            f"A non-string filter value was detected for the '{cur_flt_fn}' filter function"
                        )

        return self

    @model_validator(mode="after")
    def check_unique_sorting(self) -> Self:
        # Check that the 'sorting' list does not contain duplicate column names.
        id_list = list(_.id for _ in self.sorting)
        if len(set(id_list)) != len(id_list):
            raise ValueError("The list of ids in 'sorting' must be unique")

        return self
