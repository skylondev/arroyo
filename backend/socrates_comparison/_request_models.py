from pydantic import BaseModel, Field, model_validator
from typing import Literal, Annotated, Self, TypeAlias
from enum import Enum


# NOTE: this is sent by the frontend. It represents a single column with
# respect to which the sorting will take place and whether or not
# the sorting should be descending.
class column_sort(BaseModel):
    desc: bool
    # NOTE: these are the columns with respect to which we allow sorting.
    id: Literal[
        "tca", "dca", "relative_speed", "tca_diff", "dca_diff", "relative_speed_diff"
    ]


# NOTE: these are the allowed range-based predicates.
class range_filter_fns(Enum):
    # NOTE: camel case on the strings here as they are
    # coming from a javascript library in the frontend.
    greater_than = "greaterThan"
    less_than = "lessThan"
    between = "between"
    between_inclusive = "betweenInclusive"


# Definition of the allowed filter predicates for each column.
class filter_fns(BaseModel):
    # NOTE: filtering on norad ids and object names is restricted
    # to the 'contains' predicate.
    norad_ids: Literal["contains"]
    object_names: Literal["contains"]
    # NOTE: the other columns (except for tca) allow for range-based filtering.
    dca: range_filter_fns
    relative_speed: range_filter_fns
    tca_diff: range_filter_fns
    dca_diff: range_filter_fns
    relative_speed_diff: range_filter_fns


# Definition of the filter values.
class norad_ids_filter(BaseModel):
    id: Literal["norad_ids"]
    value: str


class object_names_filter(BaseModel):
    id: Literal["object_names"]
    value: str


# NOTE: this is the value that is passed in the range-based filters.
range_based_fv: TypeAlias = (
    Annotated[list[str | None], Field(min_length=2, max_length=2)] | str
)


class dca_filter(BaseModel):
    id: Literal["dca"]
    value: range_based_fv


class relative_speed_filter(BaseModel):
    id: Literal["relative_speed"]
    value: range_based_fv


class tca_diff_filter(BaseModel):
    id: Literal["tca_diff"]
    value: range_based_fv


class dca_diff_filter(BaseModel):
    id: Literal["dca_diff"]
    value: range_based_fv


class relative_speed_diff_filter(BaseModel):
    id: Literal["relative_speed_diff"]
    value: range_based_fv


# NOTE: this is the data sent by the frontend.
class request(BaseModel):
    model_config = {"extra": "forbid"}

    begin: int = Field(..., ge=0)
    nrows: int = Field(..., ge=0, le=500)
    sorting: list[column_sort]
    filter_fns: filter_fns
    filters: list[
        norad_ids_filter
        | object_names_filter
        | dca_filter
        | relative_speed_filter
        | tca_diff_filter
        | dca_diff_filter
        | relative_speed_diff_filter
    ]

    @model_validator(mode="after")
    def check_unique_filter_ids(self) -> Self:
        # The ids in filters must be unique (that is,
        # we cannot be applying two filters to the same column).
        id_list = list(_.id for _ in self.filters)
        if len(set(id_list)) != len(id_list):
            raise ValueError("The list of ids in 'filters' must be unique")

        return self

    @model_validator(mode="after")
    def check_filters_consistency(self) -> Self:
        # For the range-based filters, we have to
        # make sure that the selected filter function is consistent
        # with the filter.
        for flt in self.filters:
            if flt.id in [
                "dca",
                "relative_speed",
                "tca_diff",
                "dca_diff",
                "relative_speed_diff",
            ]:
                cur_flt_fn = getattr(self.filter_fns, flt.id)

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
                    # the filter value must be a string.
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
