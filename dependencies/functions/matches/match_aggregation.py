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


class MatchAggregation(IDiagnose):
        

    @classmethod
    def __prepare_df(cls, p_dbtype: str, p_dbname: str, p_schema: Optional[str], p_table: str, p_query: Optional[str]) -> pd.DataFrame:

        """Prepares a dataframe by querying a database table."""

        f_database: IDatabase = FDatabase(p_dbtype)

        return DfUtil.read_sql(
            p_query = f_database.prepare_read_query(p_schema, p_table, p_query),
            p_engine = f_database.make_connection(p_dbname).engine
        )


    @classmethod
    def __aggregate_df(cls, p_df: pd.DataFrame, p_group_columns: List[str], p_agg_column: str, p_agg_method: str) -> pd.DataFrame:

        """Aggregates a DataFrame based on specified group-by columns and aggregation method."""

        agg_df: pd.DataFrame = p_df.groupby(p_group_columns, dropna = True).agg(
            agg_value = (p_agg_column, p_agg_method)
        )

        return agg_df.reset_index()


    @classmethod
    def evaluate(cls, p_task_name: str, p_src_config: dict, p_tgt_config: dict, p_task_parameter: dict) -> dict:


        # Load parsed inputs
        inp_src_dbtype      : str = p_src_config["src_dbtype"]
        inp_src_dbname      : str = p_src_config["src_dbname"]
        inp_src_schema      : Optional[str] = p_src_config["src_schema"]
        inp_src_table       : str = p_src_config["src_table"]
        inp_src_table_query : Optional[str] = p_src_config["src_query"]

        inp_tgt_dbtype      : str = p_tgt_config["tgt_dbtype"]
        inp_tgt_dbname      : str = p_tgt_config["tgt_dbname"]
        inp_tgt_schema      : Optional[str] = p_tgt_config["tgt_schema"]
        inp_tgt_table       : str = p_tgt_config["tgt_table"]
        inp_tgt_table_query : Optional[str] = p_tgt_config["tgt_query"]
        
        inp_src_group_columns: List[str] = p_task_parameter["src_group_columns"]
        inp_src_agg_column: str = p_task_parameter["src_agg_column"]
        inp_src_agg_method: str = p_task_parameter["src_agg_method"]

        inp_tgt_group_columns: List[str] = p_task_parameter["tgt_group_columns"]
        inp_tgt_agg_column: str = p_task_parameter["tgt_agg_column"]
        inp_tgt_agg_method: str = p_task_parameter["tgt_agg_method"]


        # Load source and target data
        src_df: pd.DataFrame = cls.__prepare_df(inp_src_dbtype, inp_src_dbname, inp_src_schema, inp_src_table, inp_src_table_query)
        tgt_df: pd.DataFrame = cls.__prepare_df(inp_tgt_dbtype, inp_tgt_dbname, inp_tgt_schema, inp_tgt_table, inp_tgt_table_query)


        # Aggregate data
        src_agg_df: pd.DataFrame = cls.__aggregate_df(src_df, inp_src_group_columns, inp_src_agg_column, inp_src_agg_method)
        tgt_agg_df: pd.DataFrame = cls.__aggregate_df(tgt_df, inp_tgt_group_columns, inp_tgt_agg_column, inp_tgt_agg_method)


        # Merge source and target data on group columns
        joined_df: pd.DataFrame = src_agg_df.merge(
            tgt_agg_df, left_on = inp_src_group_columns, right_on = inp_tgt_group_columns, how = "left", suffixes = ("_src", "_tgt")
        ).convert_dtypes()


        # Identify mismatches
        mismatch_df: pd.DataFrame = joined_df[
            (joined_df["agg_value_src"] != joined_df["agg_value_tgt"]) |
            (joined_df["agg_value_src"].isna() != joined_df["agg_value_tgt"].isna())
        ]

        if not mismatch_df.empty:
            logger.info("Aggregated join dataframe:"); DfUtil.print(joined_df.head(5))
            logger.info("Aggregated mismatch dataframe:"); DfUtil.print(mismatch_df.head(5))


        # Initiate validation
        validation_engine: DfExpectation = DfExpectation(
            p_name = f"{inp_src_table}-{inp_tgt_table}",
            p_df   = mismatch_df
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
                        "observed_source_count": src_df.shape[0],
                        "aggregated_source_count": src_agg_df.shape[0],
                        "observed_target_count": tgt_df.shape[0],
                        "aggregated_target_count": tgt_agg_df.shape[0],
                        "observed_join_count": joined_df.shape[0],
                        "mismatch_count": _result["result"]["observed_value"]
                    }
                } for _result in validation_result_object["results"]
            ]
        }

        return validation_result_output
    