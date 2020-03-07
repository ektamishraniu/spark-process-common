"""
Process construct used to perform basic schema-driven validations
and to add some standard meta data values to be used downstream
"""

import argparse
import json
import sys

from abc import ABCMeta, abstractmethod
from pyspark import SparkConf, SparkContext, SparkFiles
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from py4j.protocol import Py4JJavaError

from spark_process_common.job_config import JobConfig
from spark_process_common.transforms import hash_columns

# Conveys a value was expected, but not found
MISSING_DESC = 'Not Specified'
MISSING_CODE = 'N/S'

# Conveys a value was not available, but not necessarily expected
NOT_APPLICABLE_DESC = 'Not Applicable'
NOT_APPLICABLE_CODE = 'N/A'

# Conveys an ID value that is expected, but not found
# Dimensions will contain a '~' row, so that consumers can always inner join from Facts
MISSING_STRING_ID = '~'
MISSING_NUMERIC_ID = -1

# Conveys a default or expected numeric value, so that NULLs don't pollute calculations.
MISSING_NUMBER = 0


class BaseProcess(metaclass=ABCMeta):
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
        config: dict = None
        ):
        """
        Default constructor.

        Parameters:

        **spark_config - contains the configuration settings in dictionary form
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

        **json_config - contains the config_bucket and config_path parameters
        for development and testing usage that will be collected from the
        spark-submit arguments in cluster mode.

            example:
            {
                "config_bucket": "/vagrant/mill_profitability_ingest/job-config/",
                "config_path": "extract_pbm_as400_urbcrb_invoices.json"
            }

        **spark - an already configured SparkSession, from another object instance or a test class, for example.

        **config - a dictionary of expected or optional configuration items, such as those already read and dumped
        from a json file
        """
        self.__extract_dttm = None
        self.__spark = None
        self.__config = None

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
        self.__spark.stop()

    def get_spark(self):
        """
        Helper function to retrieve the generated SparkSession.
        """
        return self.__spark

    def get_spark_context(self) -> SparkContext:
        """
        Helper function to retrieve the SparkContext from the generated SparkSession.
        """
        return self.__spark.sparkContext

    def add_dependencies(self):

        dependencies = self.get_config().get('dependencies')

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

    def set_spark(self, spark_config: dict, spark: SparkSession = None):
        """
        Helper function to create SparkSession after instantiation.
        """
        # Prevents re-assigning spark session after instantiation
        if not self.__spark:

            # Allows instantiation / override for testing or sub-processing
            if spark:
                self.__spark = spark

            else:
                # SparkSession Configuration
                spark_conf = SparkConf()
                for key, value in spark_config.items():
                    spark_conf.set(key, value)
                self.__spark_conf = spark_conf

                # Build the SparkSession here
                self.__spark = (
                    SparkSession.builder.appName(self.__config.get('app_name'))
                    .config(conf=self.__spark_conf)
                    .enableHiveSupport()
                    .getOrCreate()
                )

                # Add any dependent py files to the sparkContext
                self.add_dependencies()

    def get_config(self):
        """
        Helper function to retrieve a copy of the JSON Configuration.
        """
        return self.__config.copy()

    def set_config(self, config: dict = None):
        """
        Helper function to update / override configuration for testing.
        """
        # Allows instantiation / override for testing or sub-processing
        if config:
            self.__config = config

        else:
            parser = self.create_parser()
            args = parser.parse_args()
            config_bucket = args.__dict__.pop('config_bucket')
            config_path = args.__dict__.pop('config_path')

            # Configuration
            job_config = JobConfig(config_bucket, config_path)
            self.__config = job_config.get()
            for name, value in vars(args).items():
                self.__config[name] = value

        # Allows us to override dependencies for testing
        if self.__spark:
            self.add_dependencies()

    @classmethod
    def json_to_dict(cls, json_config: dict) -> dict:
        with open(json_config.get("config_bucket") + json_config.get("config_path")) as f:
            config = json.loads(f.read())

        return config

    @abstractmethod
    def transform(self, config: dict) -> DataFrame:
        # Config requirements may be different among child class implementations
        raise NotImplementedError

    @staticmethod
    def save(data_frame: DataFrame, config: dict):
        """
        Write csv output to be loaded into Redshift
        """
        target_path = config['target_path']
        target_format = config.get('target_format')

        if target_format == 'orc':
            data_frame.write.orc(path=target_path, mode='overwrite')
        # csv is the default
        else:
            write_options = config.get('write_options')
            if not write_options:
                write_options = {
                    "sep": '|',
                    "header": 'true',
                    "escape": '"',
                    "quoteAll": 'true',
                    "timestampFormat": 'yyyy-MM-dd HH:mm:ss',
                    "dateFormat": 'yyyy-MM-dd',
                    "mode": 'overwrite'
                }

            data_frame.write.csv(path=target_path, **write_options)

    def execute(self):
        """
        Entry point for the Process.  This function controls the
        ordering of transformation and save process.
        """
        config = self.get_config()

        self.create_views(sources=config['sources'])
        df = self.transform(config=config)
        df = self.add_meta_data_and_primary_key(data_frame=df)

        self.save(data_frame=df, config=config)

    def read_source(self, source: dict) -> DataFrame:
        """
        Read and return source as a Dataframe using the options passed in the
        'source' dictionary.
        """
        spark = self.get_spark()
        path = source['path']
        file_format = source.get('format')

        if file_format == 'csv':
            read_options =  source.get('read_options')
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

    def create_views(self, sources: dict):
        """
        Read and expose each source as a spark table/view.

        Purpose:
        Abstract the read from processing and allow each
        source to be accessed via spark.sql or spark native api's.

        Example:
        sources = {
            source_name_1: {"path": "s3://bucket/path", "create_view": true, "view_type": "global"},
            source_name_2: {"path": "s3://bucket/path", "create_view": true, "view_type": "temp"}
        }
        """
        spark = self.get_spark()
        for key, val in sources.items():
            if val.get('create_view'):
                df = spark.read.orc(val['path'])
                if val.get("view_type") == 'global':
                    df.createOrReplaceGlobalTempView(key)
                else:
                    df.createOrReplaceTempView(key)

    def add_meta_data_and_primary_key(self, data_frame: DataFrame) -> DataFrame:
        """
        Add standardized meta data and primary key columns
        """
        config = self.get_config()
        key_columns = config['key_columns']
        primary_key = config['target_name'] + '_id'
        app_name = config['app_name']

        non_key_columns = [i for i in data_frame.columns if i not in key_columns]

        df = (
            data_frame.withColumn(primary_key, F.expr(hash_columns(key_columns)))
            .withColumn("iptmeta_process_name", F.lit(app_name))
            .withColumn("iptmeta_mod_dttm", F.current_timestamp())
            .withColumn("iptmeta_diff_md5", F.expr(hash_columns(non_key_columns)))
        )

        # Prepare column order for output
        column_list = [primary_key] + key_columns
        column_list.append('iptmeta_process_name')
        column_list.append('iptmeta_mod_dttm')
        column_list.append('iptmeta_diff_md5')
        column_list = column_list + non_key_columns

        df = df.select(*column_list)

        return df

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
