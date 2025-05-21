#####################################################
# Packages                                          #
#####################################################

import re
import pytz
from datetime import datetime
from typing import Any, List, Optional, Union


#####################################################
# Main Class                                        #
#####################################################


class ConfigValidator:

    """A class to handle reading and validating job configurations from a database."""
    

    @staticmethod
    def to_lowercase(p_value: Any) -> Any:
        return p_value.lower() if p_value and isinstance(p_value, str) else p_value
    
    
    @staticmethod
    def to_uppercase(p_value: Any) -> Any:
        return p_value.upper() if p_value and isinstance(p_value, str) else p_value
        

    @staticmethod
    def convert_utc_to_ist(p_value: Optional[datetime]) -> Optional[datetime]:

        return (
            p_value.astimezone(pytz.timezone("Asia/Kolkata"))
                if p_value and isinstance(p_value, datetime) else p_value
        )
    

    @staticmethod
    def validate_email(p_value: Union[str, List[str]]) -> List[str]:
            
        """Validates emails against the 'altimetrik.com' domain."""
    
        def _validate_single_email(_email: str) -> str:

            pattern: str = r"@(altimetrik.com)$"
            email_cleaned: str = _email.strip()

            if not re.search(pattern, email_cleaned, re.IGNORECASE):
                raise ValueError(f"Invalid email: '{email_cleaned}'. Must belong to 'altimetrik.com' domain.")
            
            return email_cleaned
        

        if not p_value:
            return p_value
        
        elif isinstance(p_value, str):
            return [_validate_single_email(email) for email in p_value.split(",")]
        
        elif isinstance(p_value, list):
            return [_validate_single_email(email) for email in p_value]
        
        else:
            raise TypeError("Expected a string or a list of strings.")
    

    @staticmethod
    def validate_schema(p_dbtype: str, p_schema: Optional[str]) -> None:

        _mandatory_schema_dbtypes: List[str] = ["POSTGRE"]

        if (p_dbtype in _mandatory_schema_dbtypes and p_schema is None):
            raise ValueError(
                f"Schema is required for database type '{p_dbtype}'. "
                 "Please provide 'schema' when using this database type."
            )
        
        if (p_dbtype not in _mandatory_schema_dbtypes and p_schema is not None):
            raise ValueError(
                f"Schema is not required for database type '{p_dbtype}'. "
                 "Please don't provide 'schema' when using this database type."
            )
        

    @staticmethod
    def validate_table_used_in_query(p_schema: Optional[str], p_table: str, p_sql_query: str) -> None:

        """Checks if the given table is used in the SQL query only in FROM and JOIN clauses."""

        table_identifier: str = f"{p_schema + '.' if p_schema else ''}{p_table}"

        pattern = fr"\bFROM\s+{table_identifier}\b|\bJOIN\s+{table_identifier}\b"

        matches = re.findall(pattern, p_sql_query, re.IGNORECASE)

        if not bool(matches):
            raise ValueError(f"The table '{table_identifier}' is not used correctly in the SQL query '{p_sql_query}', please check.")