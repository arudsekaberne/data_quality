#####################################################
# Packages                                          #
#####################################################

import logging
from sqlalchemy import text
from collections import namedtuple
from sqlalchemy.orm import sessionmaker
from typing import Dict, Final, Optional
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from dependencies.utilities.cred_util import CredUtil
from dependencies.entities.classes.databases.mysql import Mysql
from dependencies.entities.interfaces.i_database import IDatabase
from dependencies.entities.classes.databases.postgre import Postgre


#####################################################
# Class                                             #
#####################################################

logger = logging.getLogger(__name__)


class FDatabase:

    # Class Private Variables
    __DBINSTANCE: Final[Dict[str, IDatabase]] = {
        "MYSQL": Mysql,
        "POSTGRE": Postgre
    }


    def __init__(self, p_dbtype: str) -> None:

        dbtype: str = p_dbtype.strip().upper()
        dbcred: dict = CredUtil().get_db_credential(dbtype)
        
        if dbtype not in self.__DBINSTANCE.keys():
            raise ValueError(f"Unsupported database type detected: {dbtype}, supported databases: {', '.join(self.__DBINSTANCE)}.")
        
        self.db_instance: IDatabase = self.__DBINSTANCE[dbtype](
            p_username = dbcred["username"],
            p_password = dbcred["password"],
            p_hostname = dbcred["hostname"],
            p_port     = dbcred["port"]
        )
    

    def make_connection(self, p_dbname: str) -> namedtuple:

        """
        Establishes a database connection and returns a named tuple containing 
            the database engine and connection string.
        """

        db_connection_str: str = self.db_instance.connection_string(p_dbname)
        db_engine: Engine = self.db_instance.create_engine(db_connection_str)
        DbConnection = namedtuple("DbConnection", ["engine", "connection_string"])

        return DbConnection(engine = db_engine, connection_string = db_connection_str)
        

    def prepare_read_query(self, p_schema: Optional[str], p_table: str, p_query: Optional[str]) -> str:

        """
        Prepares a SQL read query based on the provided table and optional query.
        """

        _read_query: str = (
            p_query
                if p_query
                    else self.db_instance.select_query(
                        self.db_instance.table_identifier(p_schema, p_table)
                    )
        )

        return _read_query
    

    def execute_query(self, p_dbname: str, p_query: str) -> None:

        """
        Executes a given SQL query on the specified database.
        """
        
        Session: sessionmaker = sessionmaker(
            bind = self.make_connection(p_dbname).engine
        )

        with Session() as session:

            try:

                logging.debug(f"Passed query: {p_query}")
                
                session.execute(text(p_query))
                
                session.commit()

            except SQLAlchemyError:

                session.rollback()

                raise