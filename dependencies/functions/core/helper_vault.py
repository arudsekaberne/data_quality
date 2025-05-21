#####################################################
# Packages                                          #
#####################################################

import time
import logging
import argparse
from functools import wraps
from typing import Any, Optional
from argparse import RawTextHelpFormatter
from sqlalchemy.exc import OperationalError
from dependencies.utilities.env_util import EnvUtil



#####################################################
# Main Class                                        #
#####################################################


class HelperVault:

    @staticmethod
    def parse_arguments() -> argparse.Namespace:
        
        # Define input arguments
        parser: argparse.ArgumentParser = argparse.ArgumentParser(
            formatter_class = RawTextHelpFormatter,
            description = "MGDB Data Quality Job Runner"
        )

        parser.add_argument(
            "--job_id", type = int, required = True,
            help = (
                "Specifies the unique identifier for the job configuration.\n"
                "This ID is used to retrieve relevant job-level and task-level settings from the configuration tables.\n\n"
                "To view available jobs and associated tasks, execute the following SQL queries:\n"
                "  SELECT * FROM public.data_quality_job_config ORDER BY job_id;\n"
                "  SELECT * FROM public.v_data_quality_task_config ORDER BY job_id, task_id;\n\n"
                "Example:\n"
                "  $ python main.py --job_id 101"
            )
        )

        if EnvUtil.enable_auto():
            parser.add_argument(
                "--auto", action = "store_true", required = False,
                help = (
                    "Indicates that the job is being triggered automatically as part of a scheduled Airflow DAG run.\n"
                    "If omitted, the job will attempt to resume from the last known failure point, typically at the validation stage.\n\n"
                    "Example:\n"
                    "  $ python main.py --job_id 101 --auto"
                )
            )

        parser.add_argument(
            "--debug", action = "store_true", required = False,
            help = (
                "Enables debug-level logging for detailed diagnostic output.\n"
                "Use this option during development or troubleshooting to trace program execution and internal states.\n\n"
                "Example:\n"
                "  $ python main.py --job_id 101 --debug"
            )
        )

        # Get input argument
        args: argparse.Namespace = parser.parse_args()

        return args


    @staticmethod
    def retry_connection_error(retry_seconds: int = 60, max_retries: Optional[int] = 3) -> Any:

        """
        Retries a function on OperationalError only.
            If max_retries is None, it will retry indefinitely.
            If max_retries is a number, it will retry up to that number and giveup.
        """
        
        def decorator(func):
            
            @wraps(func)
            def wrapper(*args, **kwargs):
                
                retry_counter = 1

                while True:
                    
                    try:
                        return func(*args, **kwargs)
                    
                    except (ConnectionError, OperationalError) as error:
                        
                        logging.warning(
                            f"[Retry {retry_counter}] Failed to execute {func.__name__} due to ConnectionError/OperationalError error. "
                            f"Retrying in {retry_seconds} second{'s' if retry_seconds > 1 else ''}. "
                            f"Error: {error}"
                        )
                        
                        time.sleep(retry_seconds)
                        
                        retry_counter += 1

                    # Stop retrying after max_retries attempts
                    if max_retries is not None and retry_counter > max_retries:
                        
                        error_message: str = f"Max retries reached for {func.__name__}. Giving up."

                        logging.critical(error_message)
                        
                        raise Exception(error_message)
            
            return wrapper
        
        return decorator