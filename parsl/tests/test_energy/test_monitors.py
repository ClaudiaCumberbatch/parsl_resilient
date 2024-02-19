import logging
import os
import pytest
import time

from parsl.monitoring.energy.node_monitors import *

logger = logging.getLogger(__name__)

@pytest.mark.local
def test_rapl_monitor():
    monitor = RaplCPUNodeEnergyMonitor(debug=True)
    result = monitor.report()
    assert result

    time.sleep(2)

    new_result = monitor.report()
    assert new_result.start_time == result.end_time
    assert new_result.total_energy > 0
    
    import json
    assert json.dumps(new_result.dict()["devices"])
    
    

if __name__ == "__main__":
    test_energy_collection()