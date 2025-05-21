#####################################################
# Packages                                          #
#####################################################

import json
import logging
import pandas as pd
from datetime import datetime
from typing import List, Optional
from dependencies.utilities.df_util import DfUtil
from sqlalchemy.dialects.postgresql import ARRAY, JSON
from dependencies.utilities.const_util import ConstUtil
from dependencies.entities.models.process_enum import TaskStatusEnum
from dependencies.entities.models.config_core_model import TaskConfigModel
from dependencies.entities.models.result_model import ValidationResultsModel


#####################################################
# Main Class                                        #
#####################################################

logger = logging.getLogger(__name__)


class LogAuditorTask:
    
    """
    A class to handle reading and validating log from a database.
    """    

    def __init__(self, p_job_batch_id: str, p_task_config: TaskConfigModel) -> None:

        """
        Initialize a task-level batch ID using the job-level batch ID and task configuration.
        """
        
        self.__task_config   : TaskConfigModel = p_task_config
        self.__task_batch_id : str = f"{p_job_batch_id}_{self.__task_config.task_id}"

        logger.info(f"Task Batch ID '{self.__task_batch_id}' -> Task Name '{self.__task_config.task_name}'")
    

    @property
    def task_batch_id(self) -> str:

        """
        Returns the fully qualified task-level batch ID.
        """
        
        return self.__task_batch_id

    
    def create_log(self, p_start_datetime: datetime, p_end_datetime: Optional[datetime] = None, p_validation_results: Optional[ValidationResultsModel] = None) -> None:
        
        """
        Inserts a task trigger log entry into the data quality task log table.
        """

        task_log_status: TaskStatusEnum = (
            (TaskStatusEnum.SUCCESS if p_validation_results.success else TaskStatusEnum.FAILURE)
                if p_validation_results else TaskStatusEnum.SKIPPED
        )

        task_log_results: Optional[List[dict]] = (
            [_diagnose_result.model_dump() for _diagnose_result in p_validation_results.results]
                if p_validation_results else None
        )

        task_log_df: pd.DataFrame = pd.DataFrame([
            {
                "batch_id": self.__task_batch_id,
                "task_id": self.__task_config.task_id,
                "task_name": self.__task_config.task_name,
                "task_rule": self.__task_config.task_rule.value,
                "task_status": task_log_status.value,
                "task_results": task_log_results,
                "config_passed": json.dumps(self.__task_config.model_dump(), default = str),
                "start_time": p_start_datetime,
                "end_time": p_end_datetime or p_start_datetime
            }
        ])

        DfUtil.insert_df_to_sql(
            p_df = task_log_df,
            p_schema = ConstUtil.PRCS_DB_SCHEMA,
            p_table  = ConstUtil.PRCS_TASK_LOG_TBL_NAME,
            p_engine = ConstUtil.PRCS_DB_ENGINE,
            p_dtype  = {
                "task_results": ARRAY(JSON)
            }
        )   

        logger.info(f"Task log inserted with the values {{'task_status': {task_log_status}}} along with other parameters.")