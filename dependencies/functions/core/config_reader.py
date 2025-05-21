#####################################################
# Packages                                          #
#####################################################

import logging
import numpy as np
import pandas as pd
from typing import List
from dependencies.utilities.df_util import DfUtil
from dependencies.utilities.const_util import ConstUtil
from dependencies.entities.models.config_core_model import JobConfigModel, TaskConfigModel


#####################################################
# Main Class                                        #
#####################################################

logger = logging.getLogger(__name__)


class ConfigReader:

    """A class to handle reading and validating job configurations from a database."""

    @staticmethod
    def get_job_config(p_job_id: int) -> JobConfigModel:

        """Validates and retrieves job configuration details for a given job ID."""

        job_df: pd.DataFrame = DfUtil.read_sql(
            p_engine = ConstUtil.PRCS_DB_ENGINE,
            p_query = f"""
                SELECT * FROM {ConstUtil.PRCS_DB_SCHEMA}.{ConstUtil.PRCS_JOB_CONFIG_TBL_NAME}
                WHERE job_id = {p_job_id}
                ;
            """
        )


        # Validate configuration
        if job_df.empty:
            raise ValueError(f"Job ID '{p_job_id}' not found in the configuration table.")
        
        if DfUtil.find_duplicate_records(p_df = job_df, p_subset = ["job_id"]).has_duplicate:
            raise RuntimeError(f"Multiple entries found for Job ID '{p_job_id}' in the configuration table.")
        

        # Create a JobConfigModel instance from the configuration
        logger.info("Job Configuration Validation:")

        job_config_raw: dict = job_df.to_dict(orient = "records")[0]
        logger.info(f"   RAW - {job_config_raw}")

        job_config_model: JobConfigModel = JobConfigModel(**job_config_raw)
        logger.info(f"   MDL - {job_config_model}\n")

        return job_config_model
    

    @staticmethod
    def get_task_configs(p_job_id: int) -> List[TaskConfigModel]:

        """Validates and retrieves task configuration details for a given job ID."""

        task_df: pd.DataFrame = DfUtil.read_sql(
            p_engine = ConstUtil.PRCS_DB_ENGINE,
            p_query = f"""
                SELECT
                    job_id, task_id, task_name, task_rule, config_type, src_reference, tgt_reference, src_config, tgt_config, COALESCE(task_parameter, '{{}}'::JSON) AS task_parameter, fail_fast, is_active, dw_created_ts, dw_updated_ts
                FROM {ConstUtil.PRCS_DB_SCHEMA}.{ConstUtil.PRCS_TASK_CONFIG_TBL_NAME}
                WHERE job_id = {p_job_id}
                ORDER BY job_id, task_id
                ;
            """
        )

        task_df = task_df.replace(np.nan, None)


        # Validate configuration
        if task_df.empty:
            raise ValueError(f"No tasks associated with Job ID '{p_job_id}' in the configuration table.")

        if DfUtil.find_duplicate_records(p_df = task_df, p_subset = ["task_id"]).has_duplicate:
            raise RuntimeError(f"Duplicate Task IDs found for Job ID '{p_job_id}' in the configuration table.")

        if DfUtil.find_duplicate_records(p_df = task_df, p_subset = ["task_name"]).has_duplicate:
            raise RuntimeError(f"Duplicate Task names found for Job ID '{p_job_id}' in the configuration table.")
        

        # Create a list of TaskConfigModel instance from the configuration       
        logger.info("Tasks Configuration Validation:")

        task_models: List[TaskConfigModel] = []

        for row in task_df.to_dict(orient = "records"):

            logger.info(f"[{row['job_id']}.{row['task_id']}]")
            logger.info(f"   RAW - {row}")
            
            task_model: TaskConfigModel = TaskConfigModel(**row)
            logger.info(f"   MDL - {task_model}")

            task_models.append(task_model)

        logger.info("***\n")
            
        return task_models