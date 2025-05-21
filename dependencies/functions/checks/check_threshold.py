#####################################################
# Packages                                          #
#####################################################

import logging
from typing import Optional, Union
import great_expectations.expectations as gxe
from dependencies.entities.interfaces.i_diagnose import IDiagnose
from dependencies.entities.classes.expectations.sql_expectation import SqlExpectation
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult


#####################################################
# Main Class                                        #
#####################################################


class CheckThreshold(IDiagnose):
        

    @classmethod
    def evaluate(cls, p_task_name: str, p_src_config: dict, p_tgt_config: dict, p_task_parameter: dict) -> dict:


        # Input task parameter
        inp_provided_min: Optional[Union[int, float]] = p_task_parameter.get("min")
        inp_provided_max: Optional[Union[int, float]] = p_task_parameter.get("max")
        inp_provided_column: Optional[str] = p_task_parameter.get("column")


        # Initiate validation
        validation_engine: SqlExpectation = SqlExpectation(
            p_dbtype = p_src_config["src_dbtype"],
            p_dbname = p_src_config["src_dbname"],
            p_schema = p_src_config["src_schema"],
            p_table  = p_src_config["src_table"],
            p_query  = p_src_config["src_query"]
        )

        # Pick right expectation
        if inp_provided_column:
            picked_expectation = gxe.ExpectColumnValuesToBeBetween(
                column    = p_task_parameter["column"],
                min_value = inp_provided_min,
                max_value = inp_provided_max,
                strict_min = False,
                strict_max = False
            )
        
        else:
            picked_expectation = gxe.ExpectTableRowCountToBeBetween(
                min_value = inp_provided_min,
                max_value = inp_provided_max
            )



        # Set data asset expectations            
        validation_engine.add_expectation(
            p_expectation = picked_expectation
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
                    "result": (
                        {
                            "observed_count": _result["result"]["element_count"],
                            "unexpected_count": _result["result"]["unexpected_count"]
                        }
                        if inp_provided_column
                        else {
                            "observed_count": _result["result"]["observed_value"]
                        }
                    )
                } for _result in validation_result_object["results"]
            ]
        }

        return validation_result_output
    