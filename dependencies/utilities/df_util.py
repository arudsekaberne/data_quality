#####################################################
# Packages                                          #
#####################################################

import logging
import pandas as pd
from functools import wraps
from sqlalchemy import text
from tabulate import tabulate
from collections import namedtuple
from sqlalchemy.engine.base import Engine
from typing import Any, Callable, List, Optional


#####################################################
# Class                                             #
#####################################################

logger = logging.getLogger(__name__)


class DfUtil:

    """A utility class for handling data operations with Pandas DataFrames."""

    def __manage_connection(func: Callable) -> Callable:

        """Decorator to manage database connections."""

        @wraps(func)
        def _wrapper(*args, **kwargs) -> Any:

            # Extract engine from function arguments
            p_engine = kwargs.get("p_engine") or args[1]

            with p_engine.connect() as connection, connection.begin():
                result = func(*args, **kwargs)
                
            return result
        
        return _wrapper


    @staticmethod
    @__manage_connection
    def read_sql(p_query: str, p_engine: Engine, p_dtype: type = None) -> pd.DataFrame:

        """Executes an SQL SELECT query and returns the results as a Pandas DataFrame."""

        logger.debug(f"Passed query: {p_query}")
        
        return pd.read_sql_query(sql = text(p_query), con = p_engine, dtype = p_dtype)
    

    @staticmethod
    @__manage_connection
    def insert_df_to_sql(p_df: pd.DataFrame, p_schema: str, p_table: str, p_engine: Engine, p_if_exists: str = "append", p_dtype: dict = None) -> None:

        """Writes a Pandas DataFrame to a SQL database table."""
        
        p_df.to_sql(p_table, schema = p_schema, con = p_engine, if_exists = p_if_exists, method = "multi", index = False, dtype = p_dtype)
        

    @staticmethod
    def have_same_columns(p_df1: pd.DataFrame, p_df2: pd.DataFrame, raise_exception: bool = False) -> bool:
        
        """Check if two DataFrames have the exact same columns (only names and not order)."""

        sorted_df1_columns: List[str] = sorted(p_df1.columns.to_list())
        sorted_df2_columns: List[str] = sorted(p_df2.columns.to_list())

        same_columns: bool = sorted_df1_columns == sorted_df2_columns

        if not same_columns and raise_exception:

            raise Exception(
                f"Column mismatch detected! Expected: {sorted_df1_columns} Found: {sorted_df2_columns}"
            )
        
        return same_columns
    

    @staticmethod
    def find_null_records(p_df: pd.DataFrame, p_subset: Optional[List[str]] = None, raise_exception: bool = False) -> namedtuple:
    
        """Check if DataFrame has null (NaN) values in the specified columns."""

        # Filter rows where any of the specified columns have NaN values
        null_df: pd.DataFrame = p_df[p_df[p_subset if p_subset else p_df.columns].isna().any(axis=1)]

        if not null_df.empty and raise_exception:
            raise Exception(
                f"Null records detected! Found: {null_df.shape[0]} records"
            )

        # Named tuple to store the result
        NullInfo = namedtuple("NullInfo", ["df", "has_null"])

        return NullInfo(df = null_df, has_null = not null_df.empty)
    

    @staticmethod
    def find_duplicate_records(p_df: pd.DataFrame, p_subset: Optional[List[str]] = None, raise_exception: bool = False) -> namedtuple:
        
        """Check if DataFrame has duplicate rescords."""

        duplicate_df: pd.DataFrame = p_df[p_df.duplicated(subset = p_subset)]

        if not duplicate_df.empty and raise_exception:

            raise Exception(
                f"Duplicate records detected! Found: {duplicate_df.shape[0]} records"
            )
        
        DuplicateInfo = namedtuple("DuplicateInfo", ["df", "has_duplicate"])

        return DuplicateInfo(df = duplicate_df, has_duplicate = not duplicate_df.empty)


    @staticmethod
    def print(p_df: pd.DataFrame) -> None:
        
        """
        Pretty print a pandas DataFrame using the tabulate library.
        """

        # Apply repr() to every element so that pd.NA, None, NaN show as literal.
        safe_df = p_df.applymap(repr)

        logger.info(f"\n{tabulate(safe_df, headers='keys', tablefmt='psql', showindex=False)}")


    @staticmethod
    def sort_columns(p_df: pd.DataFrame, p_grains: Optional[List[str]] = None) -> pd.DataFrame:

        """Returns a DataFrame with columns reordered: primary columns first, others sorted alphabetically."""

        primary_columns: Optional[List[str]] = p_grains.copy() if p_grains else []
        
        sorted_columns: List[str] = sorted(list(filter(lambda col: col not in primary_columns, p_df.columns)))

        if p_grains:
            sorted_columns = primary_columns + sorted_columns

        return p_df[sorted_columns]