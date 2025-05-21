#####################################################
# Packages                                          #
#####################################################

from abc import ABC, abstractmethod


#####################################################
# Class                                             #
#####################################################

class IDiagnose(ABC):
    
    @classmethod
    @abstractmethod
    def evaluate(
        cls, p_task_name: str, p_src_config: dict, p_tgt_config: dict, p_rule_parameter: dict
    ) -> dict: ...