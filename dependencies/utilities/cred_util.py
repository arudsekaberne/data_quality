#####################################################
# Packages                                          #
#####################################################

import os
from dotenv import load_dotenv
from typing import Optional, Literal
from dependencies.utilities.env_util import EnvUtil


#####################################################
# Class                                             #
#####################################################


# Load .env variables once when the module is imported
load_dotenv()


class CredUtil:

    # Class Private Variables
    __ENV: Literal["DEV", "PROD"] = "DEV" if EnvUtil.is_dev() else "PROD" 


    @classmethod
    def getenv(cls, p_key: str, raise_expection: bool = True) -> Optional[str]:

        value: Optional[str] = os.getenv(p_key, None)

        if not value and raise_expection:
            raise Exception(f"Environment value can't be empty for {p_key}: {value}, please check `.env` or `.bashrc` file.")

        return value
    
    @classmethod
    def get_db_credential(cls, p_dbtype: str) -> dict:
        
        return {
            "username": cls.getenv(f"{p_dbtype}_USER_{cls.__ENV}"),
            "password": cls.getenv(f"{p_dbtype}_PASS_{cls.__ENV}"),
            "hostname": cls.getenv(f"{p_dbtype}_HOST_{cls.__ENV}"),
            "port"    : cls.getenv(f"{p_dbtype}_PORT_{cls.__ENV}", raise_expection = False)
        }
    
    @classmethod
    def get_smtp_credential(cls) -> dict:
        
        return {
            "smtp_port"      : cls.getenv("SMTP_PORT"),
            "smtp_address"   : cls.getenv("SMTP_ADDRESS"),
            "sender_login"   : cls.getenv("SMTP_SENDER_LOGIN"),
            "sender_password": cls.getenv("SMTP_SENDER_PASSWORD"),
        }