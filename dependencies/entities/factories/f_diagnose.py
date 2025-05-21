#####################################################
# Packages                                          #
#####################################################

from typing import Dict, Tuple
from dependencies.functions.matches.match_row import MatchRow
from dependencies.functions.checks.check_nulls import CheckNulls
from dependencies.entities.interfaces.i_diagnose import IDiagnose
from dependencies.functions.checks.check_values import CheckValues
from dependencies.functions.checks.check_columns import CheckColumns
from dependencies.functions.checks.check_duplicate import CheckDuplicate
from dependencies.functions.checks.check_threshold import CheckThreshold
from dependencies.functions.matches.match_aggregation import MatchAggregation
from dependencies.functions.matches.match_count_tables import MatchCountTables
from dependencies.entities.models.process_enum import ConfigTypeEnum, TaskRuleEnum
from dependencies.functions.matches.match_count_api_table import MatchCountApiTable


#####################################################
# Class                                             #
#####################################################

class FDiagnose:

    """
    A factory class for retrieving the appropriate diagnose function instance based on 
        the provided diagnose key.
    """

    # Class Private Variables
    __DIAGNOSE_INSTANCE: Dict[Tuple[ConfigTypeEnum, TaskRuleEnum], IDiagnose] = {
        (ConfigTypeEnum.TBL, TaskRuleEnum.MATCH_ROW): MatchRow,
        (ConfigTypeEnum.TBL, TaskRuleEnum.CHECK_NULLS): CheckNulls,
        (ConfigTypeEnum.TBL, TaskRuleEnum.CHECK_VALUES): CheckValues,
        (ConfigTypeEnum.TBL, TaskRuleEnum.CHECK_COLUMNS): CheckColumns,
        (ConfigTypeEnum.API, TaskRuleEnum.MATCH_COUNT): MatchCountApiTable,
        (ConfigTypeEnum.TBL, TaskRuleEnum.MATCH_COUNT): MatchCountTables,
        (ConfigTypeEnum.TBL, TaskRuleEnum.CHECK_DUPLICATE): CheckDuplicate,
        (ConfigTypeEnum.TBL, TaskRuleEnum.CHECK_THRESHOLD): CheckThreshold,
        (ConfigTypeEnum.TBL, TaskRuleEnum.MATCH_AGGREGATION): MatchAggregation
    }


    @classmethod
    def get_instance(cls, p_config_type: str, p_task_rule: str) -> IDiagnose:

        """Retrieves an instance of the appropriate diagnose function."""

        return cls.__DIAGNOSE_INSTANCE[(p_config_type, p_task_rule)]()