#####################################################
# Packages                                          #
#####################################################

from requests.auth import HTTPBasicAuth
from dependencies.utilities.cred_util import CredUtil
from dependencies.entities.interfaces.i_request import IRequestAuth
from dependencies.entities.models.process_enum import ApiAuthKeyEnum


#####################################################
# Classes                                           #
#####################################################

class BasicAuth(IRequestAuth):

    @staticmethod
    def get_config(auth_key: ApiAuthKeyEnum) -> dict:

        return {
            "auth": HTTPBasicAuth(
                username = CredUtil.getenv(f"API_AUTH_USERNAME_{auth_key}"),
                password = CredUtil.getenv(f"API_AUTH_PASSWORD_{auth_key}")
            ),
            "headers": {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
        }