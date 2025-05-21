#####################################################
# Packages                                          #
#####################################################

import json
import logging
import pandas as pd
from jinja2 import Template
from dependencies import assets
from collections import namedtuple
import importlib.resources as pkg_resources
from dependencies.utilities.env_util import EnvUtil
from dependencies.functions.core.helper_job import HelperJob
from dependencies.functions.core.helper_task import HelperTask
from dependencies.entities.models.log_model import JobLogModel
from typing import Dict, Final, Optional, Tuple, TextIO, Union
from dependencies.functions.core.helper_vault import HelperVault
from dependencies.utilities.alert_util import OutlookAlert, TeamsAlert
from dependencies.entities.models.config_core_model import JobConfigModel
from dependencies.entities.models.process_enum import JobStatusEnum, TaskStatusEnum


#####################################################
# Main Class                                        #
#####################################################

logger = logging.getLogger(__name__)


class HelperAlert:

    @staticmethod
    def __get_notification_message(p_job_status: JobStatusEnum, p_validation_status: TaskStatusEnum) -> namedtuple:

        STATUS_EMOJI_CONFIG: Final[Dict[Union[JobStatusEnum, TaskStatusEnum], str]] = {
            TaskStatusEnum.SUCCESS  : "✅",
            TaskStatusEnum.FAILURE  : "❌",
            TaskStatusEnum.WARNING  : "⚠️",
            TaskStatusEnum.SKIPPED  : "⏭️",            
            JobStatusEnum.ERROR     : "🛑",
            JobStatusEnum.STOPPED   : "⛔",
            JobStatusEnum.TIMEOUT   : "⏱️",
            JobStatusEnum.IN_ACTIVE : "💤"
        }

        notification_status: Union[JobStatusEnum, TaskStatusEnum] = (
            p_validation_status
                if p_job_status == JobStatusEnum.COMPLETED
                    else p_job_status
        )

        NotifyInfo = namedtuple("NotifyInfo", ["status", "emoji"])

        return NotifyInfo(status = notification_status, emoji = STATUS_EMOJI_CONFIG[notification_status])
    
        
    @staticmethod
    @HelperVault.retry_connection_error()
    def send_email_notification(p_job_config: JobConfigModel, p_job_batch_id: str) -> Tuple[JobLogModel, namedtuple]:

        """
        Sends a job completion notification via Outlook email using a pre-defined HTML template.
        """

        def __style_task_status(p_task_status: str):

            """
            Apply HTML styling to task status for use in the email template.
            """

            task_status_enum: TaskStatusEnum = TaskStatusEnum(p_task_status)
            
            # Mapping from task status to styled HTML span
            task_status_style_map: Final[dict] = {
                TaskStatusEnum.SUCCESS: f"<span class='status-success'>{TaskStatusEnum.SUCCESS}</span>",
                TaskStatusEnum.FAILURE: f"<span class='status-failure'>{TaskStatusEnum.FAILURE}</span>",
                TaskStatusEnum.SKIPPED: f"<span class='status-skipped'>{TaskStatusEnum.SKIPPED}</span>",
                TaskStatusEnum.WARNING: f"<span class='status-warning'>{TaskStatusEnum.WARNING}</span>",
            }
            
            return task_status_style_map[task_status_enum]
        

        # Define column renaming mapping for readability in the email
        SELECTED_COLUMNS_CONFIG: Final[Dict[str, str]] = {
            "task_id": "Task ID",
            "task_name": "Task Name",
            "task_rule": "Task Rule",
            "task_status_cl": "Task Status",
            "time_taken": "Time Taken"
        }

        # Parse job and task logs
        p_job_log_model: JobLogModel = HelperJob.parse_log(p_job_batch_id)
        task_log_df: Optional[pd.DataFrame] = HelperTask.parse_log(p_job_log_model.batch_id)


        # Create a working copy of the task DataFrame and apply styling
        task_work_df: pd.DataFrame = task_log_df.copy()
        task_work_df["task_status_cl"] = task_work_df["task_status"].apply(__style_task_status)
        
        
        # Select and rename relevant columns
        task_selected_df: pd.DataFrame = task_work_df[SELECTED_COLUMNS_CONFIG.keys()]
        task_renamed_df: pd.DataFrame = task_selected_df.rename(columns = SELECTED_COLUMNS_CONFIG)
        
        
        # Convert the styled DataFrame to HTML for embedding into the email
        task_table_html: str = task_renamed_df.to_html(index = False, escape = False)


        # Load the HTML template from the assets
        with pkg_resources.open_text(assets, "job_status_email_template.html", encoding="utf-8") as html_obj:
            email_html_object: TextIO = html_obj.read()


        # Render the email template with actual values
        rendered_html_template = Template(email_html_object).render(
            ph_job_id = p_job_log_model.job_id,
            ph_job_name = p_job_log_model.job_name,
            ph_job_status = p_job_log_model.job_status,
            ph_job_batch_id = p_job_log_model.batch_id,
            ph_batch_date = p_job_log_model.batch_date,
            ph_batch_seq = p_job_log_model.batch_seq,
            ph_batch_type = p_job_log_model.batch_type,
            ph_alert_channel = p_job_config.alert_channel,
            ph_validation_status = p_job_log_model.validation_status,
            ph_dw_created_ts = p_job_log_model.dw_created_ts.strftime("%Y-%m-%d %I:%M:%S %p %Z"),
            ph_dw_updated_ts = p_job_log_model.dw_updated_ts.strftime("%Y-%m-%d %I:%M:%S %p %Z") if p_job_log_model.dw_updated_ts else None,
            ph_time_taken = p_job_log_model.time_taken,
            ph_is_active = p_job_config.is_active,
            ph_task_table_html = None if task_log_df.empty else task_table_html,
            ph_is_restart = p_job_log_model.is_restart,
            ph_fail_fast = p_job_log_model.fail_fast
        )

        notification_info: namedtuple = HelperAlert.__get_notification_message(
            p_job_status = p_job_log_model.job_status,
            p_validation_status = p_job_log_model.validation_status
        )

        # Send the rendered email using Outlook
        OutlookAlert.send(
            recipients = p_job_config.email_to,
            cc_recipients = p_job_config.email_cc,
            subject = f"{'TEST' if EnvUtil.is_dev() else 'LIVE'} [Data Quality] {notification_info.emoji} {p_job_config.job_name} - {notification_info.status} | Batch ID: {p_job_log_model.batch_id}",
            body = rendered_html_template,
            is_html = True
        )

        # TODO: Remove the below statement in production
        # print(rendered_html_template)

        logger.info("Email notification sent.")

        return (p_job_log_model, notification_info)
    

    @staticmethod
    @HelperVault.retry_connection_error()
    def send_teams_notification(p_job_config: JobConfigModel, p_job_log_model: JobLogModel, notification_info: namedtuple) -> None:
    
        # Load the JSON template from the assets
        with pkg_resources.open_text(assets, "job_status_teams_template.json", encoding="utf-8") as json_obj:
            teams_json_object: TextIO = json_obj.read()

        # Render the message template with actual values
        rendered_json_template = Template(teams_json_object).render(
            ph_batch_id = p_job_log_model.batch_id,
            ph_env = "TEST" if EnvUtil.is_dev() else "LIVE",
            ph_notification_emoji = notification_info.emoji,
            ph_job_name = p_job_config.job_name,
            ph_notification_status = notification_info.status,
            ph_job_owners = " / ".join(map(lambda email_address: email_address.split("@")[0], p_job_config.email_to)),
            ph_job_tags = " ".join([
                tag for tag in [
                    "#JobRestart" if p_job_log_model.is_restart else "#JobStart",
                    "#JobFailFast" if p_job_log_model.fail_fast else None,
                    "#JobInternalError" if p_job_log_model.job_status == JobStatusEnum.ERROR else None,
                    "#JobTimeOut" if p_job_log_model.job_status == JobStatusEnum.TIMEOUT else None,
                    "#JobTerminated" if p_job_log_model.job_status == JobStatusEnum.STOPPED else None,
                ] if tag
            ])
        )

        TeamsAlert.send(p_alert_channel = p_job_config.alert_channel, p_payload = json.loads(rendered_json_template))

        logger.info("Teams notification sent.")