#####################################################
# Packages                                          #
#####################################################

import requests
from typing import Any, Optional
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


class MatchCountApiTable(IDiagnose):

    @classmethod
    def __extract_api_count(cls, p_response: Any) -> int:

        """Returns the count of elements based on the type of the given response."""
        
        if isinstance(p_response, int):
            return p_response
        
        elif isinstance(p_response, list) or isinstance(p_response, dict):
            return len(p_response)
        
        raise NotImplementedError(
            f"Unsupported response type detected: {type(p_response)}. "
            "Supported types: int, list, and dict."
        )
    

    @classmethod
    def evaluate(cls, p_task_name: str, p_src_config: dict, p_tgt_config: dict, p_task_parameter: dict) -> dict:

        """Executes validation by comparing row counts between a source api and a target database table."""
        

        # Sets up API authentication parameters
        auth_instance: IRequestAuth = FApiAuth().get_auth_instance(p_auth_key = p_src_config["src_auth_key"])
        
        get_config: dict = auth_instance.get_config(
            auth_key = p_src_config["src_auth_key"]
        )


        # Make source API request
        response = requests.get(
            url = p_src_config["src_base_url"],
            **get_config
        )

        response.raise_for_status()
        data = response.json()


        # Process API response
        if isinstance(data, dict):
            
            data: Any = JsUtil.drill_down_dict(
                p_object = data,
                p_nested_keys = p_task_parameter["api_response_path"].split(".")
            )

        src_api_observed_count: int = cls.__extract_api_count(data)

        # Initiate validation
        tgt_validation_engine: SqlExpectation = SqlExpectation(
            p_dbtype = p_tgt_config["tgt_dbtype"],
            p_dbname = p_tgt_config["tgt_dbname"],
            p_schema = p_tgt_config["tgt_schema"],
            p_table  = p_tgt_config["tgt_table"],
            p_query  = p_tgt_config["tgt_query"]
        )

        # Set data asset expectations
        tgt_validation_engine.add_expectation(
            p_expectation = gxe.ExpectTableRowCountToEqual(value = src_api_observed_count)
        )

        # Run validation
        validation_result: ExpectationSuiteValidationResult = tgt_validation_engine.run()

        # Parse validation result
        validation_result_object: dict = validation_result.to_json_dict()

        validation_result_output: dict = {
            "success": validation_result_object["success"],
            "results": [
                {
                    "success": _result["success"],
                    "result": {
                        "observed_source_value": src_api_observed_count,
                        "observed_target_value": _result["result"]["observed_value"]
                    }
                } for _result in validation_result_object["results"]
            ]
        }

        return validation_result_output