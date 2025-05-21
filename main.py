#####################################################
# Environment Setup                                 #
#####################################################

import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


#####################################################
# Packages                                          #
#####################################################

import signal
import logging
import argparse
import warnings
from types import FrameType
from typing import List, Optional
from dependencies.utilities.env_util import EnvUtil
from dependencies.functions.core.helper_job import HelperJob
from dependencies.functions.core.helper_task import HelperTask
from dependencies.functions.core.helper_alert import HelperAlert
from dependencies.functions.core.helper_vault import HelperVault
from dependencies.functions.core.config_reader import ConfigReader
from dependencies.functions.core.log_auditor_job import LogAuditorJob
from great_expectations.exceptions import GreatExpectationsValidationError
from dependencies.entities.models.process_enum import JobStatusEnum, TaskStatusEnum
from great_expectations.datasource.fluent.sql_datasource import GxDatasourceWarning
from dependencies.entities.models.config_core_model import JobConfigModel, TaskConfigModel


#####################################################
# Default Configs                                   #
#####################################################

JOB_ID: int = 1002
JOB_DEBUG: bool = False
JOB_BATCH_ID: Optional[str] = None


#####################################################
# Pre - Execution                                   #
#####################################################

# Get input argument
args: argparse.Namespace = (
    argparse.Namespace(job_id = JOB_ID, debug = JOB_DEBUG)
        if EnvUtil.is_dev() else HelperVault.parse_arguments()
)

arg_job_scheduled: bool = args.auto if EnvUtil.enable_auto() else False


# Initiate logging
logging.basicConfig(
    level = logging.DEBUG if args.debug else logging.INFO,
    format = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
)


# Suppress Great expectation logs below WARNING
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("great_expectations").setLevel(logging.WARNING)
warnings.filterwarnings("ignore", category = GxDatasourceWarning)


#####################################################
# Helper Function                                   #
#####################################################

def __get_job_batch_id() -> Optional[str]:

    """
    Retrieves the current job batch ID from the global scope.
    """

    return globals().get("JOB_BATCH_ID")


@HelperVault.retry_connection_error()
def __update_job_termination(p_closing_status: JobStatusEnum, p_error: Exception) -> None:

    """
    Attempts to update the job log with a termination status and error details
    """

    # Log termination error
    logging.error(p_error)


    # Proceed only if a job batch ID is present in the global context
    if __get_job_batch_id():

        # Attempt to log the job termination details
        LogAuditorJob.update_log(
            job_status = p_closing_status,
            job_exception_type = p_error.__class__.__name__,
            job_exception_message = str(p_error).replace("'", "''")
        )

    sys.exit(1)


def __handle_job_termination(signum: int, frame: FrameType) -> None:

    """
    Gracefully terminates a job by updating its status to 'STOPPED' in the job log.
    """

    # Determine the signal name for clarity
    signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"

    # Construct an error message with job ID and signal details
    error_message: str = (
        f"Job Batch ID '{JOB_BATCH_ID}' is being forcibly terminated. "
        f"Received termination signal: {signal_name} (Signal Number: {signum})."
    )

    logging.critical(error_message)
    
    # If a job batch ID is present, update its status to 'STOPPED' in the job log
    if __get_job_batch_id():
        
        __update_job_termination(JobStatusEnum.STOPPED, SystemExit(error_message))


#####################################################
# Main Function                                     #
#####################################################

def main() -> None:
        
    try:

        logging.info(f"Input arguments: {args}")

        # Validate job configuration
        job_config: JobConfigModel = ConfigReader.get_job_config(args.job_id)


        # Validate task configuration
        task_configs: List[TaskConfigModel] = ConfigReader.get_task_configs(job_config.job_id)


        # Initialize job logger
        logging.info("Batch Setup:")
        global JOB_BATCH_ID; JOB_BATCH_ID = LogAuditorJob.initialize(p_job_config = job_config)


        # Log trigger status
        LogAuditorJob.insert_log(p_job_scheduled = arg_job_scheduled)


        # Registering termination signals
        signal.signal(signal.SIGINT, __handle_job_termination)
        signal.signal(signal.SIGTERM, __handle_job_termination)


        # Check job activeness
        if not job_config.is_active:

            LogAuditorJob.update_log(job_status = JobStatusEnum.IN_ACTIVE)
            return


        # Check previous active job runs
        HelperJob.validate_previous_jobs(job_config.job_id, JOB_BATCH_ID, job_config.job_wait_minute)


        # Restart from failure (Only for manual run)
        starting_task_id: int = HelperTask.get_starting_task_id(job_config.job_id, JOB_BATCH_ID, arg_job_scheduled, job_config.is_restart)
        logging.info(f"Job starting task id: {starting_task_id}")


        # Task Validation
        LogAuditorJob.update_log(job_status = JobStatusEnum.IN_PROGRESS); logging.info("***\n")
        
        logging.info("Data Quality Checks:")

        for task_index, task_config in enumerate(task_configs, start = 1):
                        
            if task_config.task_id >= starting_task_id:

                try:
                    HelperTask.diagnose(JOB_BATCH_ID, task_config)
                    logging.info("---" if task_index != len(task_configs) else "***\n")

                except GreatExpectationsValidationError as gx_error:
                    logging.error(gx_error)
                    logging.info("***\n")
                    LogAuditorJob.update_log(fail_fast = True)
                    break


        # Update validation status
        job_validation_status: TaskStatusEnum = HelperTask.get_validation_status(JOB_BATCH_ID)
        LogAuditorJob.update_log(validation_status = job_validation_status)
        

        # Mark job as 'COMPLETED'
        LogAuditorJob.update_log(job_status = JobStatusEnum.COMPLETED)

    
    except TimeoutError as error:
        __update_job_termination(JobStatusEnum.TIMEOUT, error)


    except Exception as error:
        __update_job_termination(JobStatusEnum.ERROR, error)

            
    finally:

        if __get_job_batch_id():

            # Attempt to send an email notification and get the results
            job_log_model, notification_info = HelperAlert.send_email_notification(job_config, JOB_BATCH_ID)

            if notification_info.status != TaskStatusEnum.SUCCESS:

                # Trigger a Teams notification for un-successful job status
                # HelperAlert.send_teams_notification(job_config, job_log_model, notification_info)
                pass


#####################################################
# Main Execution                                    #
#####################################################

if __name__ == "__main__":

    main()