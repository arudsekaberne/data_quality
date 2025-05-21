#####################################################
# Packages                                          #
#####################################################

from typing import List, Optional
import great_expectations.expectations as gxe
from dependencies.entities.interfaces.i_diagnose import IDiagnose
from dependencies.entities.classes.expectations.sql_expectation import SqlExpectation
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult


#####################################################
# Main Class                                        #
#####################################################


class CheckNulls(IDiagnose):
        

    @classmethod
    def evaluate(cls, p_task_name: str, p_src_config: dict, p_tgt_config: dict, p_task_parameter: Optional[dict]) -> dict:


        # Extract expectation metrics
        metric_validation_engine: SqlExpectation = SqlExpectation(
            p_dbtype = p_src_config["src_dbtype"],
            p_dbname = p_src_config["src_dbname"],
            p_schema = p_src_config["src_schema"],
            p_table  = p_src_config["src_table"],
            p_query  = p_src_config["src_query"]
        )

        metric_validation_engine.add_expectation(
            p_expectation = gxe.ExpectTableColumnsToMatchSet()
        )
        
        metric_validation_result: ExpectationSuiteValidationResult = metric_validation_engine.run()
        

        # Determine effective columns based on task parameters
        source_all_columns: List[str] = metric_validation_result["results"][0]["result"]["observed_value"]
        source_key_columns: Optional[List[str]] = [_column for _column in source_all_columns if _column.strip().lower().endswith("_key")]
        source_nonkey_columns: Optional[List[str]] = [_column for _column in source_all_columns if not _column.strip().lower().endswith("_key")]
        
        task_parameter_columns: Optional[List[str]] = p_task_parameter.get("columns") if p_task_parameter else None
        task_parameter_key_columns: Optional[bool] = p_task_parameter.get("include_key_columns") if p_task_parameter else None


        if not task_parameter_columns:

            if task_parameter_key_columns is True:
                task_effective_columns = source_key_columns

            elif task_parameter_key_columns is False:
                task_effective_columns = source_nonkey_columns

            else:
                task_effective_columns = source_all_columns

        else:
            task_effective_columns = (
                task_parameter_columns + source_key_columns
                    if task_parameter_key_columns else task_parameter_columns
            )

        # Remove duplicate columns        
        task_effective_columns: List[str] = list(set(task_effective_columns))


        # Initiate validation
        main_validation_engine: SqlExpectation = SqlExpectation(
            p_dbtype = p_src_config["src_dbtype"],
            p_dbname = p_src_config["src_dbname"],
            p_schema = p_src_config["src_schema"],
            p_table  = p_src_config["src_table"],
            p_query  = p_src_config["src_query"]
        )

        # Set data asset expectations
        for _column in task_effective_columns:
            main_validation_engine.add_expectation(
                p_expectation = gxe.ExpectColumnValuesToNotBeNull(column = _column)
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
                        "column": _column,
                        "observed_count": _result["result"]["element_count"],
                        "null_count": _result["result"]["unexpected_count"]
                    }
                } for _column, _result in zip(task_effective_columns, validation_result_object["results"])
            ]
        }

        return validation_result_output