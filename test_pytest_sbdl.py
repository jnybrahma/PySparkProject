import pytest

from lib.Utils import get_spark_session
from lib.ConfigLoader import get_config
##Note: PyTest file name should start with test_ or end with _test.py

@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")

##Note: Test function name must start from test_....

def test_blank_test(spark):
    print(spark.version)
    assert spark.version == "3.4.4"

def test_get_config():
    conf_local = get_config("LOCAL")
    conf_qa = get_config("QA")
    assert conf_local["kafka.topic"] == "sbdl_kafka_cloud"
    assert conf_qa["hive.database"] == "sbdl_db_qa"



