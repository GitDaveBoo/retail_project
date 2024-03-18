import pytest
from lib.Utils import get_pyspark_session

@pytest.fixture
def spark():    
    """Hello here we are creating spark session which can be re-used and then upon use the resources are freed"""
    spark_session = get_pyspark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_results(spark):
    "gives the expected result"
    results_schema = "state string, count int"
    return spark.read \
           .format('csv') \
           .schema(results_schema) \
           .load("data/test_result/state_aggregate.csv")