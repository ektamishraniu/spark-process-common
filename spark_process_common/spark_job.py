"""
Process construct used to perform basic schema-driven validations
and to add some standard meta data values to be used downstream
"""

import argparse
import json
import sys
from abc import ABCMeta

from py4j.protocol import Py4JJavaError
from pyspark import SparkConf, SparkContext, SparkFiles
from pyspark.sql import DataFrame, SparkSession

from spark_process_common import logging
from spark_process_common.job_config import JobConfig


class BaseSparkJob(metaclass=ABCMeta):
    """
    Base class for Process.  This provides the controller pattern
    structure to be sure that the extract and save functions
    are properly called.

    This class also provides the implementation for loading
    the json configuration.
    """

    def __init__(
            self,
            spark_config: dict = None,
            json_config: dict = None,
            spark: SparkSession = None,
            config: dict = None,
            logger: logging.Logger = None):
        """
        Default constructor.

        Parameters:

        spark_config - contains the configuration settings in dictionary form
        to pass to the SparkSession builder.

            example:
            {
                "spark.sql.hive.convertMetastoreOrc": "true",
                "spark.sql.files.ignoreMissingFiles": "true",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.hive.verifyPartitionPath": "false",
                "spark.sql.orc.filterPushdown": "true",
                "spark.sql.sources.partitionOverwriteMode": "DYNAMIC"
            }

        json_config - contains the config_bucket and config_path parameters
        for development and testing usage that will be collected from the
        spark-submit arguments in cluster mode.

            example:
            {
                "config_bucket": "/vagrant/mill_profitability_ingest/job-config/",
                "config_path": "extract_pbm_as400_urbcrb_invoices.json"
            }
        """
        self._spark_conf, self._spark, self._config = None, None, None
        self.logger = logger or logging.getLogger()

        if json_config:
            config = self.json_to_dict(json_config=json_config)

        self.set_config(config=config)
        self.set_spark(spark=spark, spark_config=spark_config)

    def __enter__(self):
        """
        Function called when entering a with block.

        Not building the SparkSession her so that in dev
        we can skip the with statement which causes the closing
        of the metastore connection, requiring a restart of the
        jupyter kernel after each execution.
        """
        return self

    def __exit__(self, type, value, traceback):
        """
        Function called when exiting a with block.

        Ensure the spark session is stopped when exiting a with
        statement
        """
        self._spark.stop()

    def get_spark(self):
        """
        Helper function to retrieve the generated SparkSession.
        """
        return self._spark

    def get_spark_context(self) -> SparkContext:
        """
        Helper function to retrieve the SparkContext from the generated SparkSession.
        """
        return self._spark.sparkContext

    def add_dependencies(self):
        dependencies = self.get_dependencies()
        if dependencies:
            sc = self.get_spark_context()
            for file_path in dependencies:
                try:
                    sc.addPyFile(file_path)
                # This is necessary for testing locally, as Spark can't access s3://
                # s3a:// can be used locally, but it may become obsolete in EMR
                # This is safer for testing, until Spark can connect to s3:// locally
                # Override the dependencies (if necessary) before instantiating the object
                except Py4JJavaError as e:
                    if 'java.io.IOException: No FileSystem for scheme: s3' in str(e):
                        pass
                    else:
                        raise Py4JJavaError

            sys.path.insert(0, SparkFiles.getRootDirectory())

    def get_dependencies(self):
        return self.get_config().get('dependencies')

    def set_spark(self, spark_config: dict, spark: SparkSession = None):
        """
        Helper function to create SparkSession after instantiation.
        """
        # Prevents re-assigning spark session after instantiation
        if not self._spark:
            # Allows instantiation / override for testing or sub-processing
            if spark:
                self._spark = spark
            else:
                self.set_spark_conf(spark_config)
                # Build the SparkSession here
                self._spark = (
                    SparkSession.builder.appName(self._config.get('app_name'))
                        .config(conf=self._spark_conf)
                        .enableHiveSupport()
                        .getOrCreate()
                )
                # Add any dependent py files to the sparkContext
                self.add_dependencies()

    def set_spark_conf(self, spark_config: dict):
        spark_conf = SparkConf()
        for key, value in spark_config.items():
            spark_conf.set(key, value)
        self._spark_conf = spark_conf

    def get_config(self):
        """
        Helper function to retrieve a copy of the JSON Configuration.
        """
        return self._config.copy()

    def set_config(self, config: dict = None):
        """
        Helper function to update / override configuration for testing.
        """
        # Allows instantiation / override for testing or sub-processing
        if config:
            self._config = config
        else:
            parser = self.create_parser()
            args = parser.parse_args()
            config_bucket = args.__dict__.pop('config_bucket')
            config_path = args.__dict__.pop('config_path')

            # Configuration
            job_config = JobConfig(config_bucket, config_path)
            self._config = job_config.get()
            for name, value in vars(args).items():
                self._config[name] = value

        # Allows us to override dependencies for testing
        if self._spark:
            self.add_dependencies()

    @classmethod
    def json_to_dict(cls, json_config: dict) -> dict:
        with open(json_config.get("config_bucket") + json_config.get("config_path")) as f:
            config = json.loads(f.read())

        return config

    def execute(self):
        """
        Entry point for the Process.  This function controls the
        ordering of transformation and save process.
        """
        raise NotImplementedError

    def read_source(self, source: dict) -> DataFrame:
        """
        Read and return source as a Dataframe using the options passed in the
        'source' dictionary.
        """
        spark = self.get_spark()
        path = source['path']
        file_format = source.get('format')

        if file_format == 'csv':
            read_options = source.get('read_options')
            if not read_options:
                read_options = {
                    "sep": '|',
                    "header": 'true',
                    "escape": '"',
                    "multiLine": 'true',
                    "timestampFormat": 'yyyy-MM-dd HH:mm:ss',
                    "dateFormat": 'yyyy-MM-dd'
                }
            return spark.read.csv(path=path, **read_options)
        else:
            return spark.read.orc(path=path)

    def create_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            'config_bucket',
            help='Used by boto to specify S3 bucket to json config files.',
            default='wrktdtransformationprocessproddtl001',
            type=str)
        parser.add_argument(
            'config_path',
            help='Used by boto to specify S3 path to json config file.',
            type=str)
        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser):
        pass

