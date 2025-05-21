#####################################################
# Packages                                          #
#####################################################

import pytz
from datetime import datetime


#####################################################
# Main Class                                        #
#####################################################


class DtUtil:
    

    @staticmethod
    def get_current_ist_datetime() -> datetime:

        """Returns the current date and time in Indian Standard Time (IST)."""

        return datetime.now(pytz.timezone("Asia/Kolkata"))