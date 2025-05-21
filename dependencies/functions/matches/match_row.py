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


class MatchRow(IDiagnose):
    

    @classmethod
    def __prepare_df(cls, p_dbtype: str, p_dbname: str, p_schema: Optional[str], p_table: str, p_query: Optional[str], p_primary_cols: List[str]) -> pd.DataFrame:

        """Prepares a dataframe by querying a database table."""

        f_database: IDatabase = FDatabase(p_dbtype)

        f_df: pd.DataFrame = DfUtil.read_sql(
            p_query = f_database.prepare_read_query(p_schema, p_table, p_query),
            p_engine = f_database.make_connection(p_dbname).engine
        )

        # Validate DataFrame
        DfUtil.find_duplicate_records(p_df = f_df, p_subset = p_primary_cols, raise_exception = True)

        return f_df


    @classmethod
    def __match_unmatched_records(cls, p_df: pd.DataFrame, p_columns_to_be_compared: List[str]) -> bool:

        """Filters records where source and target columns have mismatched values."""
        
        # Initialize the filter condition as True (to start with no filtering)
        filter_condition = False

        for column in p_columns_to_be_compared:

            # Construct the condition for each column and method
            condition = p_df[f"{column}_src"] != p_df[f"{column}_tgt"]

            # Combine the conditions using logical OR
            filter_condition |= condition

        return filter_condition


    @classmethod
    def __fetch_unmatched_records(cls, p_df: pd.DataFrame, p_join_columns: List[str], p_columns_to_be_compared: List[str]) -> pd.DataFrame:

        """Displays the top 5 unmatched records by comparing source and target columns."""

        # Initialize list to store columns with mismatches
        mismatched_columns: List[str] = []
        mismatched_src_tgt_columns: List[str] = p_join_columns.copy()

        # Check each column pair for mismatches
        for comp_column in sorted(
            list(filter(lambda column: column not in p_join_columns, p_columns_to_be_compared))
        ):

            src_col: str = f"{comp_column}_src"
            tgt_col: str = f"{comp_column}_tgt"
            
            # Check if there is at least one mismatch in the column pair
            has_mismatch = (p_df[src_col] != p_df[tgt_col]).any()

            if has_mismatch:
                mismatched_columns.append(comp_column)
                mismatched_src_tgt_columns.extend([src_col, tgt_col])


        logger.info(f"Mismatched columns: {mismatched_columns}")

        return p_df[mismatched_src_tgt_columns]


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
        
        inp_join_columns    : List[str] = p_task_parameter["join_columns"]


        # Load source and target data
        src_df: pd.DataFrame = cls.__prepare_df(inp_src_dbtype, inp_src_dbname, inp_src_schema, inp_src_table, inp_src_table_query, inp_join_columns)
        tgt_df: pd.DataFrame = cls.__prepare_df(inp_tgt_dbtype, inp_tgt_dbname, inp_tgt_schema, inp_tgt_table, inp_tgt_table_query, inp_join_columns)

        DfUtil.have_same_columns(p_df1 = src_df, p_df2 = tgt_df, raise_exception = True)


        # Merge source and target data on group columns
        joined_df: pd.DataFrame = src_df.merge(
            tgt_df, on = inp_join_columns, how = "outer", suffixes = ("_src", "_tgt")
        ).convert_dtypes().replace({pd.NaT: None, None: pd.NA})

        joined_str_df = joined_df.astype(str)
                

        # Identify mismatches
        filter_condition: bool = cls.__match_unmatched_records(
            p_df = joined_str_df,
            p_columns_to_be_compared = [column for column in src_df.columns if column not in inp_join_columns]
        )

        mismatch_df: pd.DataFrame = joined_str_df[filter_condition]

        logger.info(f"Mismatch count: {mismatch_df.shape[0]}")

        if not mismatch_df.empty:

            # Find mismatched column
            mismatch_column_df: pd.DataFrame = cls.__fetch_unmatched_records(mismatch_df, inp_join_columns, src_df.columns)
            
            logger.info("Joined dataframe:"); DfUtil.print(DfUtil.sort_columns(joined_str_df, inp_join_columns).head(5))
            logger.info("Mismatch dataframe:"); DfUtil.print(mismatch_column_df.head(5))


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
                        "observed_target_count": tgt_df.shape[0],
                        "observed_join_count": joined_df.shape[0],
                        "mismatch_count": _result["result"]["observed_value"]
                    }
                } for _result in validation_result_object["results"]
            ]
        }

        return validation_result_output
    