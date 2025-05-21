#####################################################
# Packages                                          #
#####################################################

from typing import Final
from sqlalchemy.engine.base import Engine
from dependencies.utilities.env_util import EnvUtil
from dependencies.entities.factories.f_database import FDatabase


#####################################################
# Class                                             #
#####################################################


class ConstUtil:
    
    """
    A utility class for storing constant value.
    """

    # ✦--- Process DB Information ---✧
    PRCS_DB_NAME: Final[str] = "mgdb"
    PRCS_DB_SCHEMA: Final[str] = "public" if EnvUtil.is_dev() else "dq"
    PRCS_DB_INSTANCE: Final[FDatabase] = FDatabase("POSTGRE")
    PRCS_DB_ENGINE: Final[Engine] = PRCS_DB_INSTANCE.make_connection(PRCS_DB_NAME).engine

    PRCS_JOB_CONFIG_TBL_NAME: Final[str] = "data_quality_job_config"
    PRCS_TASK_CONFIG_TBL_NAME: Final[str] = "v_data_quality_task_config"
    PRCS_JOB_LOG_TBL_NAME: Final[str] = "data_quality_job_log"
    PRCS_TASK_LOG_TBL_NAME: Final[str] = "data_quality_task_log"
    
    
