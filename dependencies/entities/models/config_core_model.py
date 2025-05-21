#####################################################
# Packages                                          #
#####################################################

from datetime import datetime
from typing_extensions import Self
from typing_extensions import Annotated
from typing import Dict, List, Optional, Tuple
from dependencies.entities.models.config_prm_model import *
from dependencies.entities.models.standard_schema import StandardModel
from dependencies.functions.core.config_validator import ConfigValidator
from dependencies.entities.models.process_enum import TaskRuleEnum, ConfigTypeEnum
from pydantic import model_validator, AfterValidator, BeforeValidator, Field, StrictInt, StrictBool
from dependencies.entities.models.config_sub_model import SourceApiTaskConfigModel, SourceTableTaskConfigModel, TargetTableActiveTaskConfigModel, TargetTableInActiveTaskConfigModel


#####################################################
# Classes                                           #
#####################################################

class JobConfigModel(StandardModel):

    job_id: StrictInt = Field(ge = 0)
    job_name: str
    email_to: Annotated[List[str], AfterValidator(ConfigValidator.validate_email)]
    email_cc: Annotated[Optional[List[str]], AfterValidator(ConfigValidator.validate_email)]
    alert_channel: str
    job_wait_minute: int = Field(ge = 0)
    is_restart: StrictBool
    is_active: StrictBool
    dw_created_ts: Annotated[datetime, AfterValidator(ConfigValidator.convert_utc_to_ist)]
    dw_updated_ts: Annotated[Optional[datetime], AfterValidator(ConfigValidator.convert_utc_to_ist)]
    

class TaskConfigModel(StandardModel):

    job_id: StrictInt = Field(ge = 0)
    task_id: StrictInt = Field(ge = 1)
    task_name: str
    task_rule: Annotated[TaskRuleEnum, BeforeValidator(ConfigValidator.to_uppercase)]
    config_type: Annotated[ConfigTypeEnum, BeforeValidator(ConfigValidator.to_uppercase)]
    src_reference: str
    tgt_reference: Optional[str]
    src_config: dict
    tgt_config: dict
    task_parameter: Optional[dict]
    fail_fast: StrictBool
    is_active: StrictBool
    dw_created_ts: Annotated[datetime, AfterValidator(ConfigValidator.convert_utc_to_ist)]
    dw_updated_ts: Annotated[Optional[datetime], AfterValidator(ConfigValidator.convert_utc_to_ist)]
    

    @model_validator(mode = "after")
    def validate_model(self: Self):

        if self.config_type == ConfigTypeEnum.API:
            
            _src_config = SourceApiTaskConfigModel(**self.src_config).model_dump()
            _tgt_config = TargetTableActiveTaskConfigModel(**self.tgt_config).model_dump()

        elif self.config_type == ConfigTypeEnum.TBL:
            
            _src_config = SourceTableTaskConfigModel(**self.src_config).model_dump()

            if self.task_rule.value.startswith("MATCH_"):
                _tgt_config = TargetTableActiveTaskConfigModel(**self.tgt_config).model_dump()

            else:
                _tgt_config = TargetTableInActiveTaskConfigModel(**self.tgt_config).model_dump()

        else:
            raise NotImplementedError(
                f"Implementation for config_type '{self.config_type}' is not yet available. "
                "Please ensure that this type is supported in the framework before using it. "
                "If this is a new requirement, update the model implementation accordingly."
            )
        

        # Validate task parameter
        task_parameter_model_config: Dict[Tuple[ConfigTypeEnum, TaskRuleEnum], StandardModel] = {
            (ConfigTypeEnum.API, TaskRuleEnum.MATCH_COUNT): MatchCountApiParamModel,
            (ConfigTypeEnum.TBL, TaskRuleEnum.MATCH_COUNT): NoParamModel,
            (ConfigTypeEnum.TBL, TaskRuleEnum.CHECK_COLUMNS): CheckCoulmnsTblParamModel,
            (ConfigTypeEnum.TBL, TaskRuleEnum.CHECK_VALUES): CheckValuesTblParamModel,
            (ConfigTypeEnum.TBL, TaskRuleEnum.CHECK_NULLS): CheckNullsTblParamModel,
            (ConfigTypeEnum.TBL, TaskRuleEnum.CHECK_DUPLICATE): CheckDuplicateTblParamModel,
            (ConfigTypeEnum.TBL, TaskRuleEnum.MATCH_AGGREGATION): MatchAggregateTblParamModel,
            (ConfigTypeEnum.TBL, TaskRuleEnum.MATCH_ROW): MatchRowTblParamModel,
            (ConfigTypeEnum.TBL, TaskRuleEnum.CHECK_THRESHOLD): CheckThresholdTblParamModel
        }

        _task_parameter_model: StandardModel = task_parameter_model_config.get((self.config_type, self.task_rule))

        if not _task_parameter_model:
            raise NotImplementedError(
                f"Implementation for config_type '{self.config_type}' and task_rule '{self.task_rule}' is not yet available. "
                "Please ensure that this type is supported in the framework before using it. "
                "If this is a new requirement, update the model implementation accordingly."
            )
        
        _task_parameter: dict = _task_parameter_model(**self.task_parameter).model_dump()

        # Forces the setting of an attribute, and set values to it.
        self._force_set_attribute("src_config", _src_config)
        self._force_set_attribute("tgt_config", _tgt_config)
        self._force_set_attribute("task_parameter", _task_parameter)

        return self