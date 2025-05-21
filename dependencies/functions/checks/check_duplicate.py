#####################################################
# Packages                                          #
#####################################################

import logging
import pandas as pd
from typing import List, Optional
import great_expectations.expectations as gxe
from dependencies.utilities.df_util import DfUtil
from dependencies.entities.factories.f_database import FDatabase
from dependencies.entities.interfaces.i_database import IDatabase
from dependencies.entities.interfaces.i_diagnose import IDiagnose
from dependencies.entities.classes.expectations.df_expectation import DfExpectation
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult


#####################################################
# Main Class                                        #
#####################################################

logger = logging.getLogger(__name__)


class CheckDuplicate(IDiagnose):
        

    @classmethod
    def __prepare_df(cls, p_dbtype: str, p_dbname: str, p_schema: Optional[str], p_table: str, p_query: Optional[str]) -> pd.DataFrame:

        """Prepares a dataframe by querying a database table."""

        f_database: IDatabase = FDatabase(p_dbtype)

        return DfUtil.read_sql(
            p_query = f_database.prepare_read_query(p_schema, p_table, p_query),
            p_engine = f_database.make_connection(p_dbname).engine
        )


    @classmethod
    def evaluate(cls, p_task_name: str, p_src_config: dict, p_tgt_config: dict, p_task_parameter: dict) -> dict:


        # Load parsed inputs
        inp_dbtype : str           = p_src_config["src_dbtype"]
        inp_dbname : str           = p_src_config["src_dbname"]
        inp_schema : Optional[str] = p_src_config["src_schema"]
        inp_table  : str           = p_src_config["src_table"]
        inp_query  : Optional[str] = p_src_config["src_query"]
        inp_columns: Optional[List[str]] = p_task_parameter.get("columns") if p_task_parameter else None

        # Load source and target data
        src_df: pd.DataFrame = cls.__prepare_df(inp_dbtype, inp_dbname, inp_schema, inp_table, inp_query)

        # Identify duplicates
        duplicate_df: pd.DataFrame = DfUtil.find_duplicate_records(p_df = src_df, p_subset = inp_columns or src_df.columns).df
        logger.debug(f"Duplicate dataframe:\n{duplicate_df}")


        # Initiate validation
        validation_engine: DfExpectation = DfExpectation(
            p_name = inp_table,
            p_df   = duplicate_df
        )

        # Set data asset expectations
        validation_engine.add_expectation(
            p_expectation = gxe.ExpectTableRowCountToEqual(value = 0)
        )

        # Run validation
        validation_result: ExpectationSuiteValidationResult = validation_engine.run()

        # Parse validation result
        validation_result_object: dict = validation_result.to_json_dict()

        validation_result_output: dict = {
            "success": validation_result_object["success"],
            "results": [
                {
                    "success": _result["success"],
                    "result": {
                        "observed_count": src_df.shape[0],
                        "duplicate_count": _result["result"]["observed_value"]
                    }
                } for _result in validation_result_object["results"]
            ]
        }

        return validation_result_output
    