#####################################################
# Packages                                          #
#####################################################

from typing import Any, List, Literal
from typing_extensions import Annotated, Self
from pydantic import model_validator, StrictBool, BeforeValidator
from dependencies.entities.models.standard_schema import StandardModel
from dependencies.functions.core.config_validator import ConfigValidator


#####################################################
# Classes                                           #
#####################################################


class NoParamModel(StandardModel):

    @model_validator(mode = "before")
    def no_data_allowed(cls, values):
        if values:
            raise ValueError("No data is allowed for this task rule.")
        return values
    

class MatchCountApiParamModel(StandardModel):

    api_response_path: str = None


class CheckCoulmnsTblParamModel(StandardModel):

    columns: List[str]


class CheckValuesTblParamModel(StandardModel):

    column: str
    values: List[Any]


class CheckNullsTblParamModel(StandardModel):

    columns: List[str] = None
    include_key_columns: StrictBool = None


class CheckDuplicateTblParamModel(StandardModel):

    columns: List[str] = None


class MatchAggregateTblParamModel(StandardModel):

    src_group_columns: List[str]
    src_agg_column: str
    src_agg_method: Annotated[str, BeforeValidator(ConfigValidator.to_lowercase)]
    tgt_group_columns: List[str]
    tgt_agg_column: str
    tgt_agg_method: Annotated[str, BeforeValidator(ConfigValidator.to_lowercase)]


class MatchRowTblParamModel(StandardModel):

    join_columns: List[str]


class CheckThresholdTblParamModel(StandardModel):

    min: int = None
    max: int = None
    column: str = None

    @model_validator(mode = "after")
    def validate_model(self: Self):

        if not self.min and not self.max:
            raise ValueError("At least one of 'min' or 'max' must be provided.")
        
        return self