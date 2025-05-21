#####################################################
# Packages                                          #
#####################################################

import json
import logging
import pandas as pd
from typing import List
from datetime import datetime
from dependencies.utilities.df_util import DfUtil
from dependencies.utilities.dt_util import DtUtil
from dependencies.utilities.const_util import ConstUtil
from dependencies.entities.models.process_enum import JobStatusEnum
from dependencies.entities.models.config_core_model import JobConfigModel


#####################################################
# Main Class                                        #
#####################################################

logger = logging.getLogger(__name__)


class LogAuditorJob:
    
    """
    A class to handle reading and validating log from a database.
    """

    @classmethod
    def initialize(cls, p_job_config: JobConfigModel) -> str:

        """
        Initialize the log auditing job with the provided job configuration and returns the generated batch ID.
        """

        cls.__job_config   : JobConfigModel = p_job_config
        cls.__batch_date   : datetime.date  = DtUtil.get_current_ist_datetime().date()
        cls.__batch_seq    : int = cls.__generate_batch_seq(p_job_id = cls.__job_config.job_id, p_batch_date = cls.__batch_date)
        cls.__job_batch_id : str = f"{cls.__batch_date.strftime('%Y%m%d')}_{cls.__job_config.job_id}_{cls.__batch_seq}"

        logger.info(f"Job Batch ID '{cls.__job_batch_id}', Batch Date '{cls.__batch_date}', and Batch Sequence '{cls.__batch_seq}'")

        return cls.__job_batch_id


    @classmethod
    def __generate_batch_seq(cls, p_job_id: int, p_batch_date: datetime.date) -> int:

        """
        Generate a new batch sequence number for a given job ID and batch date.
        """

        batch_seq_number: int = DfUtil.read_sql(
            p_query = f"""
                SELECT COALESCE(MAX(batch_seq), 0) + 1
                FROM {ConstUtil.PRCS_DB_SCHEMA}.{ConstUtil.PRCS_JOB_LOG_TBL_NAME}
                WHERE job_id = {p_job_id} AND batch_date = '{p_batch_date}'::DATE
                ;
            """,
            p_engine = ConstUtil.PRCS_DB_ENGINE
        ).values[0][0]

        return batch_seq_number
    

    @classmethod
    def insert_log(cls, p_job_scheduled: bool) -> None:
        
        """
        Inserts a job trigger log entry into the data governance job log table.
        """

        job_trigger_df: pd.DataFrame = pd.DataFrame([
            {
                "batch_id": cls.__job_batch_id,
                "job_id": cls.__job_config.job_id,
                "batch_date": cls.__batch_date,
                "batch_seq": cls.__batch_seq,
                "batch_type": "AUTO" if p_job_scheduled else "MANUAL",
                "job_name": cls.__job_config.job_name,
                "job_status": JobStatusEnum.TRIGGERED.value,
                "validation_status": None,
                "fail_fast": False,
                "is_restart": None,
                "job_exception_type": None,
                "job_exception_message": None,
                "config_passed": json.dumps(cls.__job_config.model_dump(), default = str),
                "dw_created_ts": DtUtil.get_current_ist_datetime(),
                "dw_updated_ts": None
            }
        ])

        DfUtil.insert_df_to_sql(
            p_df = job_trigger_df,
            p_schema = ConstUtil.PRCS_DB_SCHEMA,
            p_table  = ConstUtil.PRCS_JOB_LOG_TBL_NAME,
            p_engine = ConstUtil.PRCS_DB_ENGINE
        )   

        logger.info(f"Job log inserted with the values {{'job_status': {JobStatusEnum.TRIGGERED}}} along with initial parameters.")


    @classmethod
    def update_log(cls, **kwargs) -> None:
        
        """
        Updates the job log entry corresponding to the current batch ID with the provided fields.
        """

        # Reserved columns that are either auto-handled or should not be overridden
        reserved_columns: List[str] = ["batch_id", "dw_updated_ts"]

        # Build SQL SET clause for user-provided fields (excluding reserved columns)
        set_clause = ",\n                    ".join(
            f"{column} = '{value}'"
            for column, value in kwargs.items()
            if column not in reserved_columns
        )
        
        ConstUtil.PRCS_DB_INSTANCE.execute_query(
            p_dbname = ConstUtil.PRCS_DB_NAME,
            p_query  = f"""
                UPDATE {ConstUtil.PRCS_DB_SCHEMA}.{ConstUtil.PRCS_JOB_LOG_TBL_NAME}
                SET
                    dw_updated_ts = '{DtUtil.get_current_ist_datetime()}'{',' if set_clause else ''}
                    {set_clause}
                WHERE batch_id  = '{cls.__job_batch_id}'
                ;
            """
        )

        logger.info(f"Job log updated with the values {kwargs}.")

