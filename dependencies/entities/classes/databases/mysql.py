#####################################################
# Packages                                          #
#####################################################

from urllib.parse import quote
from typing import Final, Optional
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from dependencies.entities.interfaces.i_database import IDatabase


#####################################################
# Classes                                           #
#####################################################

class Mysql(IDatabase):


    def __init__(self, p_username: str, p_password: str, p_hostname: str, p_port: Optional[int] = None) -> None:
                
        # Private Variables
        self.__hostname: Final[str] = p_hostname
        self.__username: Final[str] = quote(p_username)
        self.__password: Final[str] = quote(p_password)
        self.__port    : Final[int] = p_port
        

    def connection_string(self, p_dbname: str) -> str:

        return f"mysql+pymysql://{self.__username}:{self.__password}@{self.__hostname}{f':{self.__port}' if self.__port else ''}/{p_dbname}"
    

    def create_engine(self, p_connection_string: str) -> Engine:
        
        return create_engine(p_connection_string)
    

    def table_identifier(self, p_schema: Optional[str], p_table: str) -> str:
        
        return p_table
    

    def select_query(self, p_table_identifier: str) -> str:

        return f"SELECT * FROM {p_table_identifier};"