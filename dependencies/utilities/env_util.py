#####################################################
# Packages                                          #
#####################################################

import os
import hashlib
import platform
import subprocess
from typing import List


#####################################################
# Class                                             #
#####################################################


class EnvUtil:
    
    """
    Utility class for environment detection.
    """

    # Class Private Variables
    __PRODUCTION_DETAILS: dict = {
        "56705b95ba85bf0e16eb7564e6101478d94494a3c8e4fb142348b4e1864edbc7": { # Airflow Container
            "enable_auto": True
        },
        "fc4e8607035d68f5f11599690e256cbb59d64093982e745ae36592fd0d5f0f0d": { # AWS EC2 Instance (megadashboard-jump-server)
            "enable_auto": False
        }
    }


    @classmethod
    def __get_hashed_machine_id(cls) -> str:

        """
        Retrieves and returns a SHA-256 hash of the machine's unique identifier (UUID), based on the operating system.
        """
        
        _machine_id: str = None
        _system_name: str = platform.system().lower()
        
        if _system_name == "windows":
            
            # Windows: Get the UUID from WMIC
            output: bytes = subprocess.check_output("wmic csproduct get uuid")
            decoded_output_split: List[str] = output.decode().split("\n")
            
            _machine_id = decoded_output_split[1].strip() if len(decoded_output_split) >= 2 else None

        elif _system_name == "linux":
            
            # Linux: Read from /etc/machine-id or /var/lib/dbus/machine-id
            for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"]:
                if os.path.exists(path):
                    with open(path, "r") as fin:
                        _machine_id = fin.read().strip()
        
        elif _system_name == "darwin":
            
            # MacOS: Use ioreg to get the hardware UUID
            output: bytes = subprocess.check_output("ioreg -rd1 -c IOPlatformExpertDevice | grep IOPlatformUUID")
            decoded_output_split: List[str] = output.decode().split('"')

            _machine_id = decoded_output_split[-2] if len(decoded_output_split) >= 2 else None
        
        else:

            raise NotImplementedError(f"Machine ID retrieval is not implemented for OS: {_system_name}")


        if not _machine_id:
            raise Exception("Critical error: Machine ID could not be retrieved. Ensure system compatibility.")

        return hashlib.sha256(_machine_id.encode()).hexdigest()
        

    @classmethod
    def is_dev(cls) -> bool:
        
        """
        Checks if the current environment is production based on the masked machine id.
        """
        
        return cls.__get_hashed_machine_id() not in cls.__PRODUCTION_DETAILS.keys()
    

    @classmethod
    def enable_auto(cls) -> bool:

        """
        Determines whether the '--auto' argument parser should be created based on the machine id.
        """

        return False if cls.is_dev() else cls.__PRODUCTION_DETAILS[cls.__get_hashed_machine_id()]["enable_auto"]