from pyspark.sql import SparkSession
from pytest import fixture

# instantiates a single Spark session for testing using conftest.py
SPARK = (
    SparkSession
    .builder
    .master("local[*]")
    #.master("local[*]")
    .appName("localTests")
    .getOrCreate()
)

@fixture
def spark():
    return SPARK.getActiveSession()