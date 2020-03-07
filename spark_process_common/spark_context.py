"""
This script contains the base classes for the SparkContexts
used in the various locations for the transform scripts.
These classes help decouple the spark context.
"""
from abc import ABCMeta, abstractmethod
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


class SparkContextBase(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_spark_context(self, app_name):
        pass

    @abstractmethod
    def get_spark_sql_context(self, app_name):
        pass


class TestSparkContext(SparkContextBase):
    """
    Encapsulates the default context object for Unit Test runs.
    It will be provided by the caller through the test framework.
    """

    def __init__(self, spark_context):
        self.sc = spark_context
        self.sqlContext = SQLContext(self.sc)

    def get_spark_context(self, app_name):
        return self.sc

    def get_spark_sql_context(self, app_name):
        self.sqlContext = SQLContext(self.sc)
        return self.sqlContext


class HDISparkContext(SparkContextBase):
    """
    Encapsulate the default context object for HDInsight clusters.
    """

    def __init__(self):
        self.conf = None
        self.sc = None
        self.sqlContext = None

    def get_spark_context(self, app_name):
        if self.sc is None:
            self.conf = SparkConf().setAppName(app_name)
            self.sc = SparkContext(conf=self.conf)

        return self.sc

    def get_spark_sql_context(self, app_name):
        if self.sc is None:
            self.get_spark_context(app_name)

        self.sqlContext = SQLContext(self.sc)
        return self.sqlContext


class JupyterSparkContext(SparkContextBase):
    """
    Encapsulate the spark context for Jupyter Notebook Development.
    """

    def __init__(self, spark_context):
        self.sc = spark_context
        self.sqlContext = SQLContext(self.sc)

    def get_spark_context(self, app_name):
        return self.sc

    def get_spark_sql_context(self, app_name):
        self.sqlContext = SQLContext(self.sc)
        return self.sqlContext


class SparkStructuredContext(SparkContextBase):
    """
    Use `pyspark.sql.SparkSession` as the main entry point for working with structured data.

    This class is only needed for backward compatibility.
    Can be removed after existing transformation jobs are refactored.
    """

    def __init__(self, spark_session):
        self.spark = spark_session
        self.sc = self.spark.sparkContext
        self.sqlContext = self.spark

    def get_spark_context(self, app_name):
        return self.sc

    def get_spark_sql_context(self, app_name):
        return self.spark
