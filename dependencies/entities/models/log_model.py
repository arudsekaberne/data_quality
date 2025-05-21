#####################################################
# Packages                                          #
#####################################################

from typing_extensions import Annotated
from typing import List, Literal, Optional
from datetime import date, datetime, timedelta
from dependencies.entities.models.standard_schema import StandardModel
from dependencies.functions.core.config_validator import ConfigValidator
from pydantic import StrictBool, StrictInt, AfterValidator, BeforeValidator
from dependencies.entities.models.process_enum import JobStatusEnum, TaskRuleEnum, TaskStatusEnum


#####################################################
# Classes                                           #
#####################################################

class JobLogModel(StandardModel):

    """Log model for job."""

    batch_id: str
    batch_date: date
    job_id: StrictInt
    batch_seq: StrictInt
    batch_type: Annotated[
        Literal["AUTO", "MANUAL"],
        BeforeValidator(ConfigValidator.to_uppercase)
    ]
    job_name: str
    job_status: JobStatusEnum
    validation_status: Optional[TaskStatusEnum]
    fail_fast: Optional[StrictBool]
    is_restart: Optional[StrictBool]
    job_exception_type: Optional[str]
    job_exception_message: Optional[str]
    config_passed: dict
    dw_created_ts: Annotated[datetime, AfterValidator(ConfigValidator.convert_utc_to_ist)]
    dw_updated_ts: Annotated[Optional[datetime], AfterValidator(ConfigValidator.convert_utc_to_ist)]
    time_taken: timedelta


class TaskLogModel(StandardModel):

    """Log model for Task."""

    batch_id: str
    task_id: StrictInt
    task_name: str
    task_rule: TaskRuleEnum
    task_status: TaskStatusEnum
    task_results: Optional[List[dict]]
    config_passed: dict
    start_time: Annotated[datetime, AfterValidator(ConfigValidator.convert_utc_to_ist)]
    end_time: Annotated[datetime, AfterValidator(ConfigValidator.convert_utc_to_ist)]
    time_taken: timedelta