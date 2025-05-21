#####################################################
# Packages                                          #
#####################################################

import requests
from typing import List
from sqlalchemy.engine.base import Engine
import great_expectations.expectations as gxe
from dependencies.utilities.js_util import JsUtil
from dependencies.utilities.df_util import DfUtil
from dependencies.entities.factories.f_request import FApiAuth
from dependencies.entities.factories.f_database import FDatabase
from dependencies.entities.interfaces.i_diagnose import IDiagnose
from dependencies.entities.interfaces.i_request import IRequestAuth
from dependencies.entities.classes.expectations.sql_expectation import SqlExpectation
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult


#####################################################
# Main Class                                        #
#####################################################


class MatchCountTables(IDiagnose):
    

    @classmethod
    def evaluate(cls, p_task_name: str, p_src_config: dict, p_tgt_config: dict, p_task_parameter: dict) -> dict:

        """Executes validation by comparing row counts between a source api and a target database table."""
        

        # Extract expectation metrics
        metric_validation_engine: SqlExpectation = SqlExpectation(
            p_dbtype = p_src_config["src_dbtype"],
            p_dbname = p_src_config["src_dbname"],
            p_schema = p_src_config["src_schema"],
            p_table  = p_src_config["src_table"],
            p_query  = p_src_config["src_query"]
        )

        metric_validation_engine.add_expectation(
            p_expectation = gxe.ExpectTableRowCountToEqual(value = 0)
        )
        
        metric_validation_result: ExpectationSuiteValidationResult = metric_validation_engine.run()
        
        src_table_observed_count: int = metric_validation_result["results"][0]["result"]["observed_value"]

        
        # Initiate validation
        main_validation_engine: SqlExpectation = SqlExpectation(
            p_dbtype = p_tgt_config["tgt_dbtype"],
            p_dbname = p_tgt_config["tgt_dbname"],
            p_schema = p_tgt_config["tgt_schema"],
            p_table  = p_tgt_config["tgt_table"],
            p_query  = p_tgt_config["tgt_query"]
        )

        # Set data asset expectations
        main_validation_engine.add_expectation(
            p_expectation = gxe.ExpectTableRowCountToEqual(value = src_table_observed_count)
        )

        # Run validation
        main_validation_result: ExpectationSuiteValidationResult = main_validation_engine.run()

        # Parse validation result
        validation_result_object: dict = main_validation_result.to_json_dict()

        validation_result_output: dict = {
            "success": validation_result_object["success"],
            "results": [
                {
                    "success": _result["success"],
                    "result": {
                        "observed_source_value": src_table_observed_count,
                        "observed_target_value": _result["result"]["observed_value"]
                    }
                } for _result in validation_result_object["results"]
            ]
        }

        return validation_result_output