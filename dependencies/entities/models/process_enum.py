#####################################################
# Models                                            #
#####################################################

from enum import unique
from dependencies.entities.models.standard_schema import StandardEnum


#####################################################
# Classes                                           #
#####################################################
    

@unique
class TaskRuleEnum(StandardEnum):

    MATCH_ROW: str = "MATCH_ROW"
    MATCH_COUNT: str = "MATCH_COUNT"
    CHECK_NULLS: str = "CHECK_NULLS"
    CHECK_VALUES: str = "CHECK_VALUES"
    CHECK_COLUMNS: str = "CHECK_COLUMNS"
    CHECK_DUPLICATE: str = "CHECK_DUPLICATE"
    CHECK_THRESHOLD: str = "CHECK_THRESHOLD"
    MATCH_AGGREGATION: str = "MATCH_AGGREGATION"


@unique
class ConfigTypeEnum(StandardEnum):

    API: str = "API"
    TBL: str = "TBL"


@unique
class ApiAuthTypeEnum(StandardEnum):

    BASIC: str = "BASIC"
    BEARER: str = "BEARER"

@unique
class ApiAuthKeyEnum(StandardEnum):

    SFDC: str = "SFDC"
    SAPSF: str = "SAPSF"
    DEX_TC: str = "DEX_TC"


@unique
class JobStatusEnum(StandardEnum):

    ERROR: str = "ERROR"
    STOPPED: str = "STOPPED"
    TIMEOUT: str = "TIMEOUT"
    WAITING: str = "WAITING"
    COMPLETED: str = "COMPLETED"
    TRIGGERED: str = "TRIGGERED"
    IN_ACTIVE: str = "IN_ACTIVE"
    IN_PROGRESS: str = "IN_PROGRESS"
    

@unique
class TaskStatusEnum(StandardEnum):

    SUCCESS: str = "SUCCESS"
    FAILURE: str = "FAILURE"
    WARNING: str = "WARNING"
    SKIPPED: str = "SKIPPED"
