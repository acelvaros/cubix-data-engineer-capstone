from pyspark.sql import DataFrame, SparkSession
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

@fixture
def some_df() -> DataFrame:
    return SPARK.createDataFrame(
        [("some_data",)],
        schema=["some_column",]
    )