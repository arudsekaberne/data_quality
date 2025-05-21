#####################################################
# Packages                                          #
#####################################################

import json
import logging
import pandas as pd
from datetime import datetime
from typing import List, Optional
from collections import namedtuple
from dependencies.utilities.df_util import DfUtil
from dependencies.utilities.dt_util import DtUtil
from dependencies.utilities.const_util import ConstUtil
from dependencies.functions.core.helper_job import HelperJob
from dependencies.entities.models.log_model import TaskLogModel
from dependencies.entities.factories.f_diagnose import FDiagnose
from dependencies.entities.interfaces.i_diagnose import IDiagnose
from dependencies.functions.core.log_auditor_job import LogAuditorJob
from dependencies.functions.core.log_auditor_task import LogAuditorTask
from dependencies.entities.models.config_core_model import TaskConfigModel
from great_expectations.exceptions import GreatExpectationsValidationError
from dependencies.entities.models.result_model import ValidationResultsModel
from dependencies.entities.models.process_enum import JobStatusEnum, TaskStatusEnum


#####################################################
# Main Class                                        #
#####################################################

logger = logging.getLogger(__name__)


class HelperTask:


    @staticmethod
    def __get_first_failed_task_id(p_job_batch_id: str) -> int:

        """
        Returns the task ID of the first failed task for a given job batch ID.
        """

        task_batch_id_pattern: str = p_job_batch_id.replace("_", r"\_") + r"\_%"

        failed_task_df: pd.DataFrame = DfUtil.read_sql(
            p_query = f"""
                SELECT
                    MIN(task_id) AS first_failed_task_id
                FROM {ConstUtil.PRCS_DB_SCHEMA}.{ConstUtil.PRCS_TASK_LOG_TBL_NAME}
                WHERE batch_id LIKE '{task_batch_id_pattern}'
                  AND UPPER(TRIM(task_status)) = '{TaskStatusEnum.FAILURE}'
                ;
            """,
            p_engine = ConstUtil.PRCS_DB_ENGINE
        )


        if failed_task_df.empty:
            raise Exception(
                f"No failed tasks found for job_batch_id = {p_job_batch_id}. "
                "This function is expected to be called only when a failure has occurred."
            )
        
        return failed_task_df.values[0][0]


    @staticmethod
    def get_starting_task_id(p_job_id: int, p_job_batch_id: str, p_scheduled: bool, p_restart_enabled: bool) -> int:

        """
        Determines whether the current job is a restart of a previous failed validation job.
            If so, retrieves the ID of the first failed task from the previous job batch.
        """

        if p_scheduled or not p_restart_enabled:
            
            LogAuditorJob.update_log(is_restart = False)

            return 1

        # Retrieve information about the most recently run job
        previous_job_info: namedtuple = HelperJob.get_previous_job_info(p_job_id, p_job_batch_id)

        # Determine if the current job should be treated as a restart
        restart_condition: bool = (
                previous_job_info.batch_id is not None
            and previous_job_info.job_status == JobStatusEnum.COMPLETED
            and previous_job_info.validation_status == TaskStatusEnum.FAILURE
        )

        if restart_condition:

            # If this is a restart, fetch the first failed task ID from the previous batch

            LogAuditorJob.update_log(is_restart = True)

            return HelperTask.__get_first_failed_task_id(p_job_batch_id = previous_job_info.batch_id)
        
        LogAuditorJob.update_log(is_restart = False)

        return 1


    @staticmethod
    def diagnose(p_job_batch_id: str, p_task_config: TaskConfigModel) -> None:

        """
        Executes diagnostic validation for a given task configuration within a job batch.
        """

        # Initialize a task log auditor for the current task
        task_log_auditor: LogAuditorTask = LogAuditorTask(p_job_batch_id, p_task_config)

        if not p_task_config.is_active:
            
            task_log_auditor.create_log(p_start_datetime = DtUtil.get_current_ist_datetime())

            return
            
        # Get the corresponding diagnose instance
        diagnose_instance: IDiagnose = FDiagnose().get_instance(
            p_config_type = p_task_config.config_type,
            p_task_rule = p_task_config.task_rule
        )

        logger.info(f"Task picked function for ('{p_task_config.config_type}', '{p_task_config.task_rule}'): {type(diagnose_instance).__name__}")

        # Execute validation with source and target configurations

        diagnose_start_datetime: datetime = DtUtil.get_current_ist_datetime()

        diagnose_results: dict = diagnose_instance.evaluate(
            p_task_name = p_task_config.task_name,
            p_src_config = p_task_config.src_config,
            p_tgt_config = p_task_config.tgt_config,
            p_task_parameter = p_task_config.task_parameter
        )

        diagnose_end_datetime: datetime = DtUtil.get_current_ist_datetime()

        # Validate diagnostic results
        validation_results: ValidationResultsModel = ValidationResultsModel(**diagnose_results)

        # Create task status
        task_log_auditor.create_log(
            p_start_datetime = diagnose_start_datetime,
            p_end_datetime = diagnose_end_datetime,
            p_validation_results = validation_results
        )

        if p_task_config.fail_fast and not validation_results.success:

            raise GreatExpectationsValidationError(
                f"Validation failed for task id: '{p_task_config.task_id}' with `fail_fast = True`."
            )


    @staticmethod
    def __get_status_count(p_job_batch_id: str) -> dict:

        """
        Retrieves all distinct task statuses for a given job batch ID.
        """

        task_batch_id_pattern: str = p_job_batch_id.replace("_", r"\_") + r"\_%"

        task_status_df: pd.DataFrame = DfUtil.read_sql(
            p_engine = ConstUtil.PRCS_DB_ENGINE,
            p_query  = f"""
                SELECT
                    UPPER(TRIM(task_status)) AS task_status, COUNT(*) AS count_status
                FROM {ConstUtil.PRCS_DB_SCHEMA}.{ConstUtil.PRCS_TASK_LOG_TBL_NAME}
                WHERE batch_id LIKE '{task_batch_id_pattern}'
                GROUP BY 1
                ;
            """
        )
        
        task_status_indexed_df: pd.DataFrame = task_status_df.set_index("task_status")
        task_status_count_df = task_status_indexed_df["count_status"]

        return task_status_count_df.to_dict()
    

    @staticmethod
    def get_validation_status(p_job_batch_id: str) -> TaskStatusEnum:

        """
        Determines the final validation status of a job batch based on the statuses of its individual tasks.
        """

        # Fetch a list of unique task statuses for the given batch ID
        task_status_info: dict = HelperTask.__get_status_count(p_job_batch_id)

        task_statuses: List[str] = list(task_status_info.keys())

        # Return 'FAILURE' if any task has failed
        if TaskStatusEnum.FAILURE in task_statuses:
            return TaskStatusEnum.FAILURE

        # Return 'WARNING' if no failures but at least one task has a warning
        elif TaskStatusEnum.WARNING in task_statuses:
            return TaskStatusEnum.WARNING
        
        # Return 'SUCCESS' only if all tasks are marked as SUCCESS
        elif TaskStatusEnum.SUCCESS in task_statuses:
            return TaskStatusEnum.SUCCESS
        
        else:
            return TaskStatusEnum.SKIPPED


    @staticmethod
    def parse_log(p_job_batch_id: int) -> Optional[pd.DataFrame]:

        """
        Parses and validates the final task logs associated with a specific job batch ID.
        """

        task_batch_id_pattern: str = p_job_batch_id.replace("_", r"\_") + r"\_%"

        task_log_df: Optional[pd.DataFrame] = DfUtil.read_sql(
            p_engine = ConstUtil.PRCS_DB_ENGINE,
            p_query  = f"""
                SELECT
                    *,
                    end_time - start_time AS time_taken
                FROM {ConstUtil.PRCS_DB_SCHEMA}.{ConstUtil.PRCS_TASK_LOG_TBL_NAME}
                WHERE batch_id LIKE '{task_batch_id_pattern}'
                ORDER BY start_time
                ;
            """
        )

        # Validate log

        if DfUtil.find_duplicate_records(p_df = task_log_df, p_subset = ["batch_id"]).has_duplicate:
            raise RuntimeError(f"Duplicate Batch IDs found for Batch Job ID '{p_job_batch_id}' in the log table.")
        
        if DfUtil.find_duplicate_records(p_df = task_log_df, p_subset = ["task_name"]).has_duplicate:
            raise RuntimeError(f"Duplicate Task names found for Batch Job ID '{p_job_batch_id}' in the log table.")
        

        # Create a TaskLogModel instance from the configuration
        logger.info("Task Log Validation:")

        for row in task_log_df.to_dict(orient = "records"):

            logger.info(f"[{row['batch_id']}]")
            logger.info(f"   RAW - {row}")
            
            task_model: TaskLogModel = TaskLogModel(**row)
            logger.info(f"   MDL - {task_model}")

        logger.info("***\n")

        return task_log_df