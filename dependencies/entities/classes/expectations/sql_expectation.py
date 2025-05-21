#####################################################
# Packages                                          #
#####################################################

import logging
from typing import Optional
import great_expectations as gx
from great_expectations.exceptions import DataContextError
from dependencies.entities.factories.f_database import FDatabase
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from dependencies.entities.interfaces.i_expectation import IExpectation
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource, TableAsset
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult


#####################################################
# Class                                             #
#####################################################

logger = logging.getLogger(__name__)


class SqlExpectation(IExpectation):

    """
    SqlExpectation initializes a database connection,
        sets up data sources, defines validation expectations, and runs the validation process.
    """
        

    def __init__(self, p_dbtype: str, p_dbname: str, p_schema: Optional[str], p_table: str, p_query: Optional[str]) -> None:

        # Great Expectations component names
        self.data_source_name       = f"{p_dbtype}.{p_dbname}".lower()
        self.data_asset_name        = f"{p_schema + '.' + p_table if p_schema else p_table}".lower()
        self.batch_definition_name  = f"{self.data_asset_name}_batch"        
        self.expectation_suite_name = f"{self.data_asset_name}_expectations"
        self.validation_name        = f"{self.data_asset_name}_validation"

        # Get database connection string
        self.db_instance_conn_str = self._initialize_database(p_dbtype, p_dbname)
        
        # Add or update SQL data source
        self.data_source = self._setup_data_source()

        # Create a Table or Query Asset
        self.data_asset = self._setup_data_asset(p_schema, p_table, p_query)

        # Define batch for processing
        self.batch_definition: BatchDefinition = self._define_batch()

        # Define expectations for processing
        self.expectation_suite: ExpectationSuite = self._setup_expectation_suite()

        # Define validation
        self.validation_definition: ValidationDefinition = self._setup_validation()


    def _initialize_database(self, _dbtype: str, _dbname: str) -> str:

        """Initializes and retrieves the database connection string."""
        
        return FDatabase(_dbtype).make_connection(_dbname).connection_string

    
    def _setup_data_source(self) -> SQLDatasource:
        
        """Adds or updates an SQL data source in Great Expectations."""

        return self.context.data_sources.add_or_update_sql(
            name = self.data_source_name,
            connection_string = self.db_instance_conn_str
        )
        
        
    def _setup_data_asset(self, _schema: str, _table: str, _query: str) -> TableAsset:

        """Sets up a Table or Query asset in the data source."""

        if _query:
            return self.data_source.add_query_asset(name = self.data_asset_name, query = _query)
        else:
            return self.data_source.add_table_asset(name = self.data_asset_name, table_name = f"{_schema + '.' + _table if _schema else _table}")


    def _define_batch(self) -> BatchDefinition:

        """Defines a batch for processing the data asset."""

        return self.data_asset.add_batch_definition_whole_table(
            name = self.batch_definition_name
        )


    def _setup_expectation_suite(self) -> ExpectationSuite:

        """Creates or updates an expectation suite for validation."""

        try:
            self.context.suites.delete(name = self.expectation_suite_name)
            logging.debug(f"Deleted existing expectation suite: {self.expectation_suite_name}")

        except DataContextError:
            logging.debug(f"No existing expectation suite named {self.expectation_suite_name} found. Creating new one.")
            
        return self.context.suites.add(
            gx.ExpectationSuite(name = self.expectation_suite_name)
        )


    def _setup_validation(self) -> ValidationDefinition:

        """Defines a validation definition for validating the data asset."""

        try:
           self.context.validation_definitions.delete(name = self.validation_name)
           logging.debug(f"Deleted existing validation: {self.validation_name}")
            
        except DataContextError:
            logging.debug(f"No existing validation named {self.validation_name} found. Creating new one.")

        return self.context.validation_definitions.add(
            gx.ValidationDefinition(
                name = self.validation_name,
                data = self.batch_definition,
                suite = self.expectation_suite
            )
        )
    

    def run(self) -> ExpectationSuiteValidationResult:

        """Runs the validation process and returns validation result."""

        validation_result: ExpectationSuiteValidationResult = self.validation_definition.run()

        for validation_info in validation_result["results"]:

            self.raise_exception(
                p_exception_info = validation_info["exception_info"]
            )

        return validation_result