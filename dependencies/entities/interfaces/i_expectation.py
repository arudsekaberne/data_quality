#####################################################
# Packages                                          #
#####################################################

from typing import Any
import great_expectations as gx
from abc import ABC, abstractmethod
from great_expectations.expectations.expectation import Expectation
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.exceptions.exceptions import GreatExpectationsError
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource, TableAsset
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult
from great_expectations.data_context.data_context.ephemeral_data_context import EphemeralDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig, InMemoryStoreBackendDefaults, ProgressBarsConfig
)

#####################################################
# Class                                             #
#####################################################

class IExpectation(ABC):



    # Create the config
    config = DataContextConfig(
        progress_bars = ProgressBarsConfig(globally = False),
        store_backend_defaults = InMemoryStoreBackendDefaults()
    )

    # Initialize the context
    context: EphemeralDataContext = gx.get_context(project_config = config)


    @abstractmethod
    def _setup_data_source(self, *args, **kwargs) -> SQLDatasource: ...

    @abstractmethod
    def _setup_data_asset(self, *args, **kwargs) -> TableAsset: ...

    @abstractmethod
    def _define_batch(self, *args, **kwargs) -> BatchDefinition: ...

    @abstractmethod
    def _setup_expectation_suite(self, *args, **kwargs) -> ExpectationSuite: ...

    @abstractmethod
    def run(self) -> ExpectationSuiteValidationResult: ...

    def add_expectation(self, p_expectation: Expectation) -> None:

        """Adds an expectation to the expectation suite."""

        self.expectation_suite.add_expectation(expectation = p_expectation)


    def raise_exception(self, p_exception_info: Any):

        """
        Recursively checks an exception details dictionary for raised exceptions.
            If an exception is found, it raises a formatted Exception with details.
        """
    
        if isinstance(p_exception_info, dict):    
            for key, value in p_exception_info.items():
                if key == "raised_exception" and value is True:
                    
                    # Extract the exception message and traceback if available
                    exception_message: str = p_exception_info["exception_message"]
                    exception_traceback: str = p_exception_info["exception_traceback"]
                    raise GreatExpectationsError(f"{exception_message}\nTraceback: {exception_traceback}")
                
                else:
                    
                    # Recursively check nested dictionaries
                    self.raise_exception(value)