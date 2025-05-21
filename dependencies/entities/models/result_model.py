#####################################################
# Packages                                          #
#####################################################

from datetime import datetime
from typing_extensions import Self
from typing_extensions import Annotated
from typing import List, Literal, Optional
from dependencies.entities.models.standard_schema import StandardModel
from dependencies.functions.core.config_validator import ConfigValidator
from pydantic import StrictBool


#####################################################
# Classes                                           #
#####################################################

class ValidationResultsModel(StandardModel):

    class __ValidationResultModel(StandardModel):
        success: StrictBool
        result: dict
        

    success: StrictBool
    results: List[__ValidationResultModel]