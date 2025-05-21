#####################################################
# Packages                                          #
#####################################################

from typing import Optional
from abc import ABC, abstractmethod


#####################################################
# Class                                             #
#####################################################

class IRequestAuth(ABC):
    
    @staticmethod
    @abstractmethod
    def get_config(auth_key: str) -> dict: ...