#####################################################
# Packages                                          #
#####################################################

import requests
from dependencies.utilities.js_util import JsUtil
from dependencies.utilities.cred_util import CredUtil
from dependencies.entities.interfaces.i_request import IRequestAuth
from dependencies.entities.models.process_enum import ApiAuthKeyEnum


#####################################################
# Classes                                           #
#####################################################

class BearerTokenAuth(IRequestAuth):

    @staticmethod
    def get_config(auth_key: ApiAuthKeyEnum) -> dict:

        response = requests.post(
            url = CredUtil.getenv(f"API_AUTH_URL_{auth_key}"),
            json = {
                "emailId": CredUtil.getenv(f"API_AUTH_EMAIL_ID_{auth_key}"),
                "requestToken": CredUtil.getenv(f"API_AUTH_REQUEST_TOKEN_{auth_key}")
            }
        )
        
        response.raise_for_status()

        response_type: str = response.headers.get("Content-Type")

        if "text/plain" in response_type:
            access_token: str = response.text.strip()

        elif "application/json" in response_type:
            
            access_token: str = JsUtil.drill_down_dict(
                p_object = response.json(),
                p_nested_keys = CredUtil.getenv(f"API_AUTH_RESPONSE_PATH_{auth_key}").split(".")
            )

        return {
            "headers": {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
        }