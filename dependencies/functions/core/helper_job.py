#####################################################
# Packages                                          #
#####################################################

import time
import logging
import numpy as np
import pandas as pd
from typing import List
from collections import namedtuple
from dependencies.utilities.df_util import DfUtil
from dependencies.utilities.const_util import ConstUtil
from dependencies.entities.models.log_model import JobLogModel
from dependencies.entities.models.process_enum import JobStatusEnum
from dependencies.functions.core.log_auditor_job import LogAuditorJob


#####################################################
# Main Class                                        #
#####################################################

logger = logging.getLogger(__name__)


class HelperJob:

    @staticmethod
    def validate_previous_jobs(p_job_id: int, p_job_batch_id: str, p_job_wait_minute: int) -> None:

        """
        Validates whether there is an active or unexpected job status for a given job ID.
        """

        job_end_statuses: List[JobStatusEnum] = [
            JobStatusEnum.ERROR,
            JobStatusEnum.STOPPED,
            JobStatusEnum.TIMEOUT,
            JobStatusEnum.IN_ACTIVE,
            JobStatusEnum.COMPLETED
        ]

        validation_query: str = f"""
            SELECT
                ARRAY_LENGTH(ARRAY_AGG(DISTINCT job_status), 1) AS unexpected_statuses_count
            FROM {ConstUtil.PRCS_DB_SCHEMA}.{ConstUtil.PRCS_JOB_LOG_TBL_NAME}
            WHERE job_id = {p_job_id} AND
                batch_id != '{p_job_batch_id}' AND
                UPPER(TRIM(job_status)) NOT IN ({
                    ", ".join([f"'{status.value}'" for status in job_end_statuses])
                })
            ;
        """

        def _get_active_job_count() -> int:

            """
            Retrieves the count of active or unexpected job runs for a given job ID.
            """

            job_log_df: pd.DataFrame = DfUtil.read_sql(
                p_engine = ConstUtil.PRCS_DB_ENGINE,
                p_query = validation_query
            )

            return job_log_df.values[0][0] or 0
            
        active_runs: int = _get_active_job_count()

        if active_runs > 0:

            LogAuditorJob.update_log(job_status = JobStatusEnum.WAITING)

            time.sleep(p_job_wait_minute * 60)

            active_runs_after_wait: int = _get_active_job_count()

            if active_runs_after_wait > 0:
            
                raise TimeoutError(
                    f"An active or unexpected job run is still detected for Job ID '{p_job_id}' "
                    f"after waiting for '{p_job_wait_minute} minute{'s' if p_job_wait_minute > 1 else ''}'."
                    f"\nPlease ensure previous job execution is completed before proceeding."
                    f"\nQuery used for validation: {validation_query}"
                )
                

    @staticmethod
    def get_previous_job_info(p_job_id: int, p_job_batch_id: str) -> namedtuple:

        """
        Retrieves information about the most recent previous job run for the same job ID.
        """

        prev_job_df: pd.DataFrame = DfUtil.read_sql(
            p_query = f"""
                SELECT
                    batch_id, UPPER(TRIM(job_status)) AS job_status, UPPER(TRIM(validation_status)) AS validation_status
                FROM {ConstUtil.PRCS_DB_SCHEMA}.{ConstUtil.PRCS_JOB_LOG_TBL_NAME}
                WHERE job_id = {p_job_id} AND batch_id != '{p_job_batch_id}'
                ORDER BY batch_date DESC, batch_seq DESC
                LIMIT 1
                ;
            """,
            p_engine = ConstUtil.PRCS_DB_ENGINE
        )

        job_info: np.ndarray = np.array([None, None, None]) if prev_job_df.empty else prev_job_df.values[0]
        JobInfo = namedtuple("JobInfo", ["batch_id", "job_status", "validation_status"])
        job_info_ntuple: namedtuple = JobInfo(batch_id = job_info[0], job_status = job_info[1], validation_status = job_info[2])

        logger.info(f"Job details previous: {job_info_ntuple}")

        return job_info_ntuple


    @staticmethod
    def parse_log(p_job_batch_id: str) -> JobLogModel:

        """
        Parses and validates job log from the log table.
        """

        job_log_df: pd.DataFrame = DfUtil.read_sql(
            p_engine = ConstUtil.PRCS_DB_ENGINE,
            p_query  = f"""
                SELECT
                    *,
                    COALESCE(dw_updated_ts, dw_created_ts) - dw_created_ts AS time_taken
                FROM {ConstUtil.PRCS_DB_SCHEMA}.{ConstUtil.PRCS_JOB_LOG_TBL_NAME}
                WHERE batch_id = '{p_job_batch_id}'
                ;
            """
        )

        # Validate log
        if job_log_df.empty:
            raise ValueError(f"Job Batch ID '{p_job_batch_id}' not found in the log table.")
        
        if DfUtil.find_duplicate_records(p_df = job_log_df, p_subset = ["batch_id"]).has_duplicate:
            raise RuntimeError(f"Multiple entries found for Job Batch ID '{p_job_batch_id}' in the log table.")
        
        
        # Create a JobLogModel instance from the configuration
        logger.info(""); logger.info("***")
        logger.info("Job Log Validation:")

        job_log_raw: dict = job_log_df.to_dict(orient = "records")[0]
        logger.info(f"   RAW - {job_log_raw}")

        job_log_model: JobLogModel = JobLogModel(**job_log_raw)
        logger.info(f"   MDL - {job_log_model}\n")

        return job_log_model