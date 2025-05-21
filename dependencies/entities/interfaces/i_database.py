#####################################################
# Packages                                          #
#####################################################

from typing import Optional, Union
from abc import ABC, abstractmethod
from sqlalchemy.engine.base import Engine


#####################################################
# Class                                             #
#####################################################

class IDatabase(ABC):
    
    @abstractmethod
    def __init__(self, p_username: str, p_password: str, p_hostname: str, p_port: Optional[int]) -> None: ...

    @abstractmethod
    def connection_string(self, p_dbname: str) -> str: ...

    @abstractmethod
    def create_engine(self, p_connection_string: str) -> Engine: ...

    @abstractmethod
    def table_identifier(self, p_schema: Union[str, Optional[str]], p_table: str) -> str: ...

    @abstractmethod
    def select_query(self, p_table_identifier: str) -> str: ...