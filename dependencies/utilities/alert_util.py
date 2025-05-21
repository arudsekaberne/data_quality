#####################################################
# Packages                                          #
#####################################################

import json
import smtplib
import requests
from typing import Optional, List
from email.mime.text import MIMEText
from requests.models import Response
from email.mime.multipart import MIMEMultipart
from dependencies.utilities.cred_util import CredUtil
from dependencies.functions.core.config_validator import ConfigValidator


#####################################################
# Class                                             #
#####################################################


class OutlookAlert:
    
    """
    A utility class for sending email alerts using Outlook SMTP settings.
    """

    @staticmethod
    def send(recipients: List[str], subject: str, body: str, cc_recipients: Optional[List[str]] = None, is_html: bool = False) -> None:
        
        """
        Send an email using SMTP credentials from CredUtil.
        """

        # Retrieve SMTP credentials
        smtp_credentials = CredUtil.get_smtp_credential()
        smtp_port = smtp_credentials["smtp_port"]
        smtp_server = smtp_credentials["smtp_address"]
        smtp_username = smtp_credentials["sender_login"]
        smtp_password = smtp_credentials["sender_password"]


        # Initialize the email message
        email_message = MIMEMultipart()
        email_message["Subject"] = subject
        email_message["From"] = smtp_username
        email_message["To"] = ", ".join(ConfigValidator.validate_email(recipients))
        email_message["Cc"] = ", ".join(ConfigValidator.validate_email(cc_recipients)) if cc_recipients else ""


        # Attach the email body
        email_message.attach(MIMEText(body, "html" if is_html else "plain"))


        # Connect to the SMTP server and send the email
        with smtplib.SMTP(smtp_server, smtp_port) as smtp_connection:

            smtp_connection.starttls()
            smtp_connection.login(smtp_username, smtp_password)

            # Combine TO and CC recipients for sending
            all_recipients = recipients + (cc_recipients or [])
            smtp_connection.sendmail(smtp_username, all_recipients, email_message.as_string())


class TeamsAlert:

    """
    A utility class for sending alerts to a Microsoft Teams channel.
    """

    @staticmethod
    def send(p_alert_channel: str, p_payload: dict) -> None:

        """
        Sends a POST request to the Teams webhook.
        """

        # Get webhook URL from env
        alert_channel_webhook: str = CredUtil.getenv(p_alert_channel)

        # Send POST request with JSON payload
        response: Response = requests.post(
            alert_channel_webhook, data = json.dumps(p_payload), headers = {"Content-Type": "application/json"}
        )

        # Raise error if request fails
        response.raise_for_status()