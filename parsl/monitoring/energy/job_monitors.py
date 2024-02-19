import logging

from parsl.monitoring.energy.base import JobEnergyMonitor

logger = logging.getLogger(__name__)

def SlurmJobEnergyMonitor(JobEnergyMonitor):
    """JobEnergyMonitor provides a interface to access job monitoring information
    usually provided by the cluster scheduler (i.e. Slurm or PBS)"""

    def __init__(self, debug: bool = True):
        self.debug = debug

    @abstractmethod
    def start(self, frequency: int) -> None:
        pass

    @abstractmethod
    def add(self, jobid: str) -> None:
        pass