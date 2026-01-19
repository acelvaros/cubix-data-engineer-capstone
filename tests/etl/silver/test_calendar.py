from datetime import datetime

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.calendar import get_calendar

def test_get_calendar(spark):
    """
    Positive test that the function get_calendar returns the expected DataFrame
    """

    test_data = spark.createDataFrame(
        [
            # contents should only be syntactically correct here
            ("2017-01-01", 7, "Sunday", "January", "1", "1", "52", "1", "2017", "2016", "1", "1", "7", "1", "201701", "extra_value"), #noqa: E501 #to suppress too long line
            ("2017-01-01", 7, "Sunday", "January", "1", "1", "52", "1", "2017", "2016", "1", "1", "7", "1", "201701", "extra_value"), #noqa: E501
        ],
        schema=[
            "Date",
            "DayNumberOfWeek",
            "DayName",
            "MonthName",
            "MonthNumberOfYear",
            "DayNumberOfYear",
            "WeekNumberOfYear",
            "CalendarQuarter",
            "CalendarYear",
            "FiscalYear",
            "FiscalSemester",
            "FiscalQuarter",
            "FinMonthNumberOfYear",
            "DayNumberOfMonth",
            "MonthID",
            "extra_col"
        ]
    )

    results = get_calendar(test_data)

    excpected_schema = st.StructType(
        [
            st.StructField("Date", st.DateType(), True),
            st.StructField("DayNumberOfWeek", st.IntegerType(), True),
            st.StructField("DayName",  st.StringType(), True),
            st.StructField("MonthName",  st.StringType(), True),
            st.StructField("MonthNumberOfYear", st.IntegerType(), True),
            st.StructField("DayNumberOfYear", st.IntegerType(), True),
            st.StructField("WeekNumberOfYear", st.IntegerType(), True),
            st.StructField("CalendarQuarter", st.IntegerType(), True),
            st.StructField("CalendarYear", st.IntegerType(), True),
            st.StructField("FiscalYear", st.IntegerType(), True),
            st.StructField("FiscalSemester", st.IntegerType(), True),
            st.StructField("FiscalQuarter", st.IntegerType(), True),
            st.StructField("FinMonthNumberOfYear", st.IntegerType(), True),
            st.StructField("DayNumberOfMonth", st.IntegerType(), True),
            st.StructField("MonthID", st.IntegerType(), True)
        ]
    )

    excpected = spark.createDataFrame(
        [
            (
                datetime(2017, 1,1),
                7,
                "Sunday",
                "January",
                1,
                1,
                52,
                1,
                2017,
                2016,
                1,
                1,
                7,
                1,
                201701
            )
        ],
        schema=excpected_schema
    )

    spark_testing.assertDataFrameEqual(result, excpected)