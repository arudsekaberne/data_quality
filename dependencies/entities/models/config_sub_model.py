#####################################################
# Packages                                          #
#####################################################

from typing_extensions import Self
from typing import Literal, Optional
from typing_extensions import Annotated
from pydantic import model_validator, BeforeValidator
from dependencies.entities.models.process_enum import ApiAuthKeyEnum, ApiAuthTypeEnum
from dependencies.entities.models.standard_schema import StandardModel
from dependencies.functions.core.config_validator import ConfigValidator


#####################################################
# Classes                                           #
#####################################################

class SourceApiTaskConfigModel(StandardModel):

    """Configuration model for source API tasks."""

    src_base_url: str
    src_auth_key: Annotated[ApiAuthKeyEnum, BeforeValidator(ConfigValidator.to_uppercase)]


class SourceTableTaskConfigModel(StandardModel):

    """Configuration model for source table tasks."""

    src_dbtype: Annotated[
        Literal["MYSQL", "POSTGRE"],
        BeforeValidator(ConfigValidator.to_uppercase)
    ]
    src_dbname: str
    src_schema: Optional[str]
    src_table: str
    src_query: Optional[str]


    @model_validator(mode = "after")
    def validate_field(self: Self):
        
        ConfigValidator.validate_schema(self.src_dbtype, self.src_schema)
        
        if self.src_query is not None:
            ConfigValidator.validate_table_used_in_query(self.src_schema, self.src_table, self.src_query)

        return self
    

class TargetTableActiveTaskConfigModel(StandardModel):

    """Configuration model for active target table tasks."""

    tgt_dbtype: Annotated[
        Literal["MYSQL", "POSTGRE"],
        BeforeValidator(ConfigValidator.to_uppercase)
    ]
    tgt_dbname: str
    tgt_schema: Optional[str]
    tgt_table: str
    tgt_query: Optional[str]


    @model_validator(mode = "after")
    def validate_field(self: Self):

        ConfigValidator.validate_schema(self.tgt_dbtype, self.tgt_schema)

        if self.tgt_query is not None:
            ConfigValidator.validate_table_used_in_query(self.tgt_schema, self.tgt_table, self.tgt_query)
        
        return self
    
    
class TargetTableInActiveTaskConfigModel(StandardModel):

    """Configuration model for in-active target table tasks."""

    tgt_dbtype: Literal[None]
    tgt_dbname: Literal[None]
    tgt_schema: Literal[None]
    tgt_table : Literal[None]
    tgt_query : Literal[None]