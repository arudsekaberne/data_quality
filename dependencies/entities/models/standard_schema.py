#####################################################
# Packages                                          #
#####################################################

from enum import Enum
from typing import Any, List, Optional
from pydantic import BaseModel, ConfigDict, model_validator


#####################################################
# Classes                                           #
#####################################################


class StandardEnum(Enum):
    
    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value

    def __eq__(self, other: Any) -> bool:
        return self.value == other
    
    def __hash__(self) -> int:
        return hash((self.__class__, self.value))
    

class StandardModel(BaseModel):

    """A Pydantic model that standardizes string formatting within data inputs."""

    @model_validator(mode = "before")
    def format_string_values(cls, data: dict) -> dict:

        """Formats input data by converting empty strings to `None` and trimming whitespace."""


        formatted_data: dict = data

        def _empty_strings_to_none(value: str) -> Optional[str]:
            return value.strip() if value.strip() != "" else None

        for key, value in data.items():

            # String handle
            if isinstance(value, str):
                formatted_data[key] = _empty_strings_to_none(value)

            # List handle
            elif isinstance(value, List):
                list_values: List[Any] = []

                for each in value:

                    # List[String] handle
                    if isinstance(each, str):
                        list_values.append(_empty_strings_to_none(each))

                    else:
                        list_values.append(each)

                formatted_data[key] = list_values

            # Dict handle
            elif isinstance(value, dict):
                dict_values: dict[Any, Any] = {}

                for obj_key, obj_value in value.items():

                    obj_key_fmt: Any = obj_key.strip() if isinstance(obj_key, str) else obj_key

                    # Dict[Any, String] handle
                    if isinstance(obj_value, str):
                        dict_values[obj_key_fmt] = _empty_strings_to_none(obj_value)

                    else:
                        dict_values[obj_key_fmt] = obj_value

                formatted_data[key] = dict_values

        return formatted_data


    def _force_set_attribute(self, name: str, value: Any) -> None:
        
        """Internal method to bypass frozen=True during validation."""

        object.__setattr__(self, name, value)

        
    # Model configuration settings
    model_config = ConfigDict(extra = "forbid", frozen = True)