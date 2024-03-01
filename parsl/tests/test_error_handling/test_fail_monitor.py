import logging
import os
import parsl
import pytest

from parsl.app.app import python_app

logger = logging.getLogger(__name__)

@python_app
def always_fail():
    raise ValueError("This ValueError should propagate to the app caller in fut.result()")

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
    with pytest.raises(ValueError):
        fut = always_fail()
        fut.result()

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()
    parsl.clear()

    logger.info("checking database content")
    engine = sqlalchemy.create_engine("sqlite:///runinfo/monitoring.db")
    with engine.begin() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM resource"))
        (c, ) = result.first()
        # assert c >= 1

        result = connection.execute(text("SELECT COUNT(*) FROM energy"))
        (c, ) = result.first()
        # assert c >= 1

    logger.info("all done")
