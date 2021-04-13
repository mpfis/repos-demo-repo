# Databricks notebook source
# MAGIC %md
# MAGIC ### Author: Miklos Christine

# COMMAND ----------

# DBTITLE 1,Build Unit Tests for Spark
import unittest, sys
from pyspark.sql.functions import col, lower, udf
from pyspark.sql.types import StringType

def flight_date(yr, mon):
    return "{0}-{1:02d}".format(yr, mon)
  
flight_date_udf = udf(flight_date, StringType())

class PysparkAirlineUDFTests(unittest.TestCase):
  
  df_test = spark.createDataFrame([[2017, 12, "UA", 28, "SFO", "SEA"], 
                                   [2017, 1, "UA", 211, "SEA", "LAX"]], 
                                  ["year", "month", "airline", "flight_num", "src", "dest"])
  src_airports = {"sfo", "sea"}
  dest_airports = {"sea", "lax"}
  
  #def flight_date(yr, mon):
  #  return "{0}-{1:02d}".format(yr, mon)
  
  # flight_date_udf = udf(flight_date, StringType())
  
  def test_normalize_airlines(self):
    src_str = set([x.src for x in self.df_test.select(lower(col("src")).alias("src")).collect()])
    dest_str = set([x.dest for x in self.df_test.select(lower(col("dest")).alias("dest")).collect()])
    self.assertEqual(src_str, self.src_airports)
    self.assertEqual(dest_str, self.dest_airports)

  def test_valid_flight_nums(self):
    flight_nums = [x.flight_num for x in self.df_test.select(col("flight_num")).collect()]

    self.assertTrue(isinstance(flight_nums[0], (int)))
    self.assertFalse(isinstance(flight_nums[1], (float)))

  def test_date_udf(self):
    dates = [x.date for x in self.df_test.select(flight_date_udf(col("year"), col("month")).alias("date")).collect()]
    self.assertEqual(dates[0], "2017-12")
    self.assertEqual(dates[1], "2017-01")


# COMMAND ----------

# DBTITLE 1,Run Unit Tests
suite1 = unittest.TestLoader().loadTestsFromTestCase(PysparkAirlineUDFTests)
suites = unittest.TestSuite([suite1])

unittest.TextTestRunner(stream=sys.stdout, verbosity=2).run(suites)

# COMMAND ----------


