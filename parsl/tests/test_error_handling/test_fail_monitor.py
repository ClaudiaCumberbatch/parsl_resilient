import logging
import os
import parsl
import pytest

from parsl.app.app import python_app

logger = logging.getLogger(__name__)

@python_app
def always_fail():
    raise ValueError("This ValueError should propagate to the app caller in fut.result()")

@python_app
def exhaust_memory():
    memory_hog = []
    while True:
        memory_hog.append('A' * 4096 * 4096)  # Append 16MB of 'A's in each iteration

@pytest.mark.local
def test_simple():

    import sqlalchemy
    from sqlalchemy import text
    from parsl.tests.configs.htex_local_energy import fresh_config

    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    logger.info("loading parsl")
    parsl.load(fresh_config())

    logger.info("invoking and waiting for result")
    with pytest.raises(BaseException):
        fut = exhaust_memory()
        fut.result()

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()
    parsl.clear()
