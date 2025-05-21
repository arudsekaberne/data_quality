#####################################################
# Packages                                          #
#####################################################

import great_expectations.expectations as gxe
from dependencies.utilities.js_util import JsUtil
from dependencies.entities.interfaces.i_diagnose import IDiagnose
from dependencies.entities.classes.expectations.sql_expectation import SqlExpectation
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult


#####################################################
# Main Class                                        #
#####################################################


class CheckColumns(IDiagnose):
        

    @classmethod
    def evaluate(cls, p_task_name: str, p_src_config: dict, p_tgt_config: dict, p_task_parameter: dict) -> dict:


        # Initiate validation
        validation_engine: SqlExpectation = SqlExpectation(
            p_dbtype = p_src_config["src_dbtype"],
            p_dbname = p_src_config["src_dbname"],
            p_schema = p_src_config["src_schema"],
            p_table  = p_src_config["src_table"],
            p_query  = p_src_config["src_query"]
        )

        # Set data asset expectations
        validation_engine.add_expectation(
            p_expectation = gxe.ExpectTableColumnsToMatchSet(
                column_set = p_task_parameter["columns"],
                exact_match = True
            )
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
                        "observed_columns"  : _result["result"]["observed_value"],
                        "missing_columns"   : JsUtil.drill_down_dict(_result["result"], ["details", "mismatched", "missing"], strict = False),
                        "unexpected_columns": JsUtil.drill_down_dict(_result["result"], ["details", "mismatched", "unexpected"], strict = False)
                    }
                } for _result in validation_result_object["results"]
            ]
        }

        return validation_result_output
    