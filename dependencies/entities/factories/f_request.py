#####################################################
# Packages                                          #
#####################################################

from typing import Dict, Final
from dependencies.entities.interfaces.i_request import IRequestAuth
from dependencies.entities.models.process_enum import ApiAuthKeyEnum
from dependencies.entities.classes.requests.basic_auth import BasicAuth
from dependencies.entities.classes.requests.bearer_token_auth import BearerTokenAuth
from dependencies.entities.classes.requests.bearer_client_auth import BearerClientAuth

#####################################################
# Class                                             #
#####################################################

class FApiAuth:

    # Class Private Variables
    __AUTH_INSTANCE: Final[Dict[ApiAuthKeyEnum, IRequestAuth]] = {
        ApiAuthKeyEnum.SAPSF: BasicAuth,
        ApiAuthKeyEnum.SFDC: BearerClientAuth,
        ApiAuthKeyEnum.DEX_TC: BearerTokenAuth
    }

    @classmethod
    def get_auth_instance(cls, p_auth_key: ApiAuthKeyEnum) -> IRequestAuth:
        
        if p_auth_key not in cls.__AUTH_INSTANCE.keys():
            raise ValueError(f"Unsupported auth key detected: {p_auth_key}, supported auth: {', '.join(cls.__AUTH_INSTANCE)}.")

        auth_instance: IRequestAuth = cls.__AUTH_INSTANCE[p_auth_key]

        return auth_instance