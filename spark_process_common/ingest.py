"""
Ingest construct used to perform basic schema-driven validations
and to add some standard meta data values to be used downstream
"""

import argparse
import boto3
import glob
import json
import numpy as np
import pandas as pd
import re

from abc import ABCMeta, abstractmethod
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_process_common.job_config import JobConfig
from spark_process_common.transforms import normalize_columns, snek

class BaseIngest(metaclass=ABCMeta):
    """
    Base class for ingest.  This provides the controller pattern
    structure to be sure that the extract and save functions
    are properly called.

    This class also provides the implementation for loading
    the json configuration.
    """

    def __init__(self, spark_config: dict, json_config: dict=None):
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
        self.__extract_dttm = None

        if json_config:
            with open(json_config.get("config_bucket") + json_config.get("config_path")) as f:
                self.__config = json.loads(f.read())
        else:
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
                         
            args = parser.parse_args()
            config_bucket = args.config_bucket
            config_path = args.config_path

            # Configuration
            job_config = JobConfig(config_bucket, config_path)
            self.__config = job_config.get()

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

    def get_config(self):
        """
        Helper function to retrieve a copy of the JSON Configuration.
        """
        return self.__config.copy()

    @staticmethod
    def __reorder_columns_for_partitioning(column_list: list, partitioned_by: list) -> list():
        """
        Helper function to reorder partition columns to be last (required by Spark / Hive).
        """
        _column_list = column_list.copy()
        _partitioned_by = partitioned_by.copy()
        non_partition_columns = [i for i in _column_list if i not in _partitioned_by]
        _column_list = non_partition_columns + _partitioned_by

        return _column_list

    @staticmethod
    def __validate_primary_key(data_frame: DataFrame, key_columns: list, data_source: str):
        """
        Function to validate that source data conforms to the proper uniquness, as defined
        by the list of keys in the job-config json.
        """
        if key_columns:

            df = data_frame.groupBy(*key_columns).count().alias('count')
            if not df.where('count > 1').rdd.isEmpty():
                df = df.where('count > 1')
                strData = str(df.limit(5).collect())
                cntDups = df.count()
                errMessage = "Source violates configured primary key. "
                if cntDups == 1:
                    errMessage = "{} There is {} duplicate row".format(errMessage, str(cntDups))
                else:
                    errMessage = "{} There are {} duplicate rows".format(errMessage, str(cntDups))
                    
                errMessage = "{} in {}. ".format(errMessage, data_source)
                errMessage = "{} Key Columns: {} ".format(errMessage, str(key_columns))
                errMessage = "{} Duplicate Data: {}".format(errMessage, strData)
                
                   
                raise IngestPrimaryKeyViolationError(errMessage)
  

    def __stage_source(self, hive_db: str, hive_table: str, partitioned_by: list, column_list: list, data_frame: DataFrame):
        """
        Function called to save the ingested Dataframe into Hive stage table -> 'SOURCE' partition
        """
        spark = self.get_spark()
        _column_list = column_list.copy()
        _partitioned_by = []
        if partitioned_by:
            _partitioned_by = partitioned_by.copy()

        _partitioned_by.append('iptmeta_record_origin')
        _column_list.append('iptmeta_record_origin')
        _column_list = [snek(c) for c in _column_list]

        data_frame = normalize_columns(data_frame)
        data_frame = data_frame.withColumn('iptmeta_record_origin', F.lit('SOURCE'))
        
        source_view_name = "{}_{}_src_temp".format(hive_db, hive_table)
        data_frame.createOrReplaceTempView(source_view_name)
        
        columns = ", ".join([i for i in _column_list])
        partitions = ", ".join(_partitioned_by)

        dml = (
            "TRUNCATE TABLE stage.{}_{}"
            .format(hive_db, hive_table)
        )
        spark.sql(dml)

        # drop all partitions
        dml = "ALTER TABLE stage.{}_{} ".format(hive_db, hive_table)
        dml = "{} DROP IF EXISTS PARTITION (iptmeta_record_origin='TARGET')".format(dml)
        spark.sql(dml)

        dml = "ALTER TABLE stage.{}_{} ".format(hive_db, hive_table)
        dml = "{} DROP IF EXISTS PARTITION (iptmeta_record_origin='SOURCE')".format(dml)
        spark.sql(dml)

        dml = "INSERT OVERWRITE TABLE stage.{}_{}".format(hive_db, hive_table)
        dml = "{} PARTITION({})".format(dml, partitions)
        dml = "{} SELECT {} FROM {}".format(dml, columns, source_view_name) 
        
        spark.sql(dml)

    def __stage_target(self, hive_db: str, hive_table: str, key_columns: list, partitioned_by: list, column_list: list):
        """
        Function to stage target rows for keys not found in the source. Prepares stage for insert overwrite.
        """
        spark = self.get_spark()
        
        joins = ""
        joins = " AND ".join(["tgt.{} = src.{}".format(i,i) for i in key_columns])
        _partitioned_by = []
        filter_partitions = []
        if partitioned_by:
            _partitioned_by = partitioned_by.copy()

            partition_columns = ", ".join(["tgt.{}".format(i) for i in _partitioned_by])

            # get the list of partition values from the target table that have matching primary keys
            dml = "SELECT DISTINCT {} ".format(partition_columns)
            dml = "{} FROM {}.{} as tgt".format(dml, hive_db, hive_table)
            dml = "{} INNER JOIN stage.{}_{} as src ON {} ".format(dml, hive_db, hive_table, joins)

            part_list_tgt = spark.sql(dml)
        
            # get list of partitions from staging table        
            dml = (
                "SELECT DISTINCT {} FROM stage.{}_{} as tgt"
                .format(partition_columns, hive_db, hive_table)
            )        
       
            part_list_src = spark.sql(dml)
        
            # final list of partition columns that we will select from 
            part_list = (
                part_list_src
                .union(part_list_tgt)
                .distinct()  
            )
            part_columns = part_list.columns 
        
            filter_partitions = []
            for row in part_list.collect():
                row_filter = " AND ".join(["tgt.{} = '{}'".format(part_columns[x],y) for x,y in enumerate(row)])
                filter_partitions.append("({})".format(row_filter))
    
            filter_partitions =  " OR ".join(filter_partitions)     

        columns = ", ".join(["tgt.{}".format(i) for i in column_list])
        _partitioned_by.append('iptmeta_record_origin')
        partitions = ", ".join(_partitioned_by)
        
        dml = "INSERT OVERWRITE TABLE stage.{}_{}".format(hive_db, hive_table)
        dml = "{} PARTITION({})".format(dml, partitions)
        dml = "{} SELECT {}, 'TARGET' as iptmeta_record_origin".format(dml, columns)
        dml = "{} FROM {}.{} as tgt".format(dml, hive_db, hive_table)
        dml = "{} LEFT OUTER JOIN stage.{}_{} as src ON {} WHERE src.{} IS null".format(dml, hive_db, hive_table, joins, key_columns[0])
        if filter_partitions:
            dml = "{} AND ({})".format(dml, filter_partitions)
        
        spark.sql(dml)

    def __write(self, hive_db: str, hive_table: str, key_columns: list, partitioned_by: list, 
                column_list: list, partition_overwrite: bool, force_truncate: bool):
        """
        Function to perform the final write to target from stage
        """
        spark = self.get_spark()

        # Table should be completely overwritten if no primary key exists
        # Insert Overwrite may not overwrite all partitions
        if (partitioned_by and not key_columns and not partition_overwrite) or force_truncate:
            dml = (
                "TRUNCATE TABLE {}.{}"
                .format(hive_db, hive_table)
            )
            spark.sql(dml)

        if hive_db == 'master_data_sources':
            column_list.append('iptmeta_record_origin')

        columns = [snek(c).replace('__', '_') for c in column_list]
        columns = ", ".join([i for i in columns])

        if partitioned_by:         
            partitions = ", ".join(partitioned_by)
            dml = (
                "INSERT OVERWRITE TABLE {}.{} PARTITION({}) SELECT {} FROM stage.{}_{}"
                .format(hive_db, hive_table, partitions, columns, hive_db, hive_table)
            )
        else:
            dml = (
                "INSERT OVERWRITE TABLE {}.{} SELECT {} FROM stage.{}_{}"
                .format(hive_db, hive_table, columns, hive_db, hive_table)
            )            

        spark.sql(dml)

    def __archive(self, archive_path: str, data_frame: DataFrame=None):
        """
        Function to saves input files into a timestamped archive directory for troubleshooting or re-processing 
        """
        archive_dttm = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        archive_path = "{}/{}".format(archive_path, archive_dttm)

        data_frame = normalize_columns(data_frame)
        data_frame.write.orc(path=archive_path, mode='append')

    def pre_processing(self, data_frame: DataFrame) -> DataFrame:
        """
        Function called to perform any necessary pre processing.
        Meant to be overriden when necessary.
        """        
        return data_frame

    @staticmethod
    def add_meta_data_columns(data_frame: DataFrame) -> DataFrame:
        """
        Append extracted DataFrames with some common meta data fields
        
        iptmeta_extract_dttm:       A single timestamp to help with troubleshooting
        iptmeta_corrupt_record:     to be used / populated as needed during validation
        iptmeta_record_origin:      An identifier to partition stage on, used to distinquish 
                                    between new rows inbound compared to target rows to be merged
        """ 

        if 'iptmeta_corrupt_record' not in data_frame.columns:
            data_frame = data_frame.withColumn('iptmeta_corrupt_record', F.lit(None).cast(T.StringType()))

        data_frame = data_frame.withColumn('iptmeta_extract_dttm', F.current_timestamp())

        return data_frame

    @abstractmethod
    def extract(self, config: dict) -> DataFrame:
        # Config requirements may be different among child class implementations 
        raise NotImplementedError

    def execute(self):
        """
        Entry point for the Ingest process.  This function controls the
        ordering of extract and save process.

        If key_columns exist, then data will be merged.  Target records will be updated
        if a match on primary key is found.  Otherwise, new records will be inserted.
        
        If key_columns do not exist, the target data will be truncated and overwritten.
        
        If key_columns do not exist, partitioned_by columns exist, and non_key_partition_overwrite, then
        only the partitions that exist in the source will be overwritten.  
        The entire target table will not be truncated in this case.

        If force_truncate is true, the target data will be truncated and overwritten.
        """

        config = self.get_config()

        hive_db = config["hive_db"]
        hive_table = config["hive_table"]

        key_columns = config.get("key_columns")
        partitioned_by = config.get("partitioned_by")
        archive_path = config['archive_path']
        force_truncate = config.get('force_truncate')

        read_options = config.get("read_options")
        path = config.get("path")  

        if read_options or path:
            data_source = "data source: {} {}".format(path, str(read_options))
        else:
            data_source = "sqoop.{}_{} table".format(hive_db, hive_table)

        # non_key_partition_overwrite argument will prevent the target table from being truncated.
        non_key_partition_overwrite = config.get("non_key_partition_overwrite")
        
        if key_columns or (not partitioned_by):
            non_key_partition_overwrite = False

        df = self.extract(config)

        # Return with no error if the sheet is optional and any Excel file does not contain it
        sheet_optional = config.get('sheet_optional')
        if sheet_optional:
            if not df:
                return

        if df.count() == 0:
            # Return with no error if empty data frame is an acceptable outcome
            success_if_no_records = config.get('success_if_no_records')
            if success_if_no_records:
                    return
            # Otherwise throw error
            else:
                errMessage = "No records exist in {}.".format(data_source)
                raise IngestNoDataError(errMessage)

        # Perform any pre-processing that may be necessary
        df = self.pre_processing(data_frame=df)

        # Add meta data fields to source output
        df = self.add_meta_data_columns(df)

        column_list = df.columns
        if partitioned_by:
            column_list = self.__reorder_columns_for_partitioning(
                column_list=column_list, 
                partitioned_by=partitioned_by
            )

            df.repartition(*partitioned_by)
        else:
            df.coalesce(10)

        # Insert Overwrite into stage.(hive_db)_(hive_table) from source -> SOURCE Partition
        self.__stage_source(
            hive_db=hive_db,
            hive_table=hive_table,
            partitioned_by=partitioned_by,
            column_list=column_list,
            data_frame=df
        )

        if key_columns:
            self.__validate_primary_key(data_frame=df, key_columns=key_columns, data_source=data_source)

        if key_columns and not force_truncate:
            # Insert Overwrite into stage.(hive_db)_(hive_table) from source -> Target Partition
            self.__stage_target(
                hive_db=hive_db,
                hive_table=hive_table,
                key_columns=key_columns,
                partitioned_by=partitioned_by,
                column_list=column_list
                )

        self.__write(
            hive_db=hive_db,
            hive_table=hive_table,
            key_columns=key_columns,
            partitioned_by=partitioned_by,
            column_list=column_list,
            partition_overwrite=non_key_partition_overwrite,            
            force_truncate=force_truncate
        )
        
        # Write orc file to s3 to retain archive of ingest #
        self.__archive(
            archive_path=archive_path,
            data_frame=df
        )


class IngestCSV(BaseIngest):
    """
    Implementation of the BaseIngest class that provides for ingesting
    CSV files.

    The schema utilized by this class follows the PySpark StructType schema.
    """

    def extract(self, config: dict) -> DataFrame:
        """
        Entry point for extract logic.

        Apply schema and add meta data.
        """
        path = config['source_path']
        read_options = config.get('read_options')
        schema = config.get('schema', dict())

        _read_options = read_options.copy()

        if schema:
            # StructType.fromJson method looks for 'fields' key ..
            schema_struct = T.StructType.fromJson(schema) 
            schema_struct.add(T.StructField('iptmeta_corrupt_record', T.StringType(), True))
            _read_options['schema'] = schema_struct
        else:
            _read_options['inferSchema'] = 'true'

        df = self.get_spark().read.csv(path, **_read_options)
        #df.cache()

        return df    

class IngestFixedLengthFile(BaseIngest):
    """
    Implementation of the BaseIngest class that provides for ingesting
    fixed length files.

    sample schema json:
        "schema": {
            "fields": [
                {"name": "Record_Type", "type": "string", "nullable": true, "metadata": "", "start": 1, "length": 3},
                {"name": "Currency_Code_From", "type": "string", "nullable": true, "metadata": "", "start": 4, "length": 3},
                {"name": "Currency_Code_To", "type": "string", "nullable": true, "metadata": "", "start": 7, "length": 3},
                {"name": "Effective_Date", "type": "string", "nullable": true, "metadata": "", "start": 10, "length": 8},
                {"name": "Conversion_Rate_Multiplier", "type": "string", "nullable": true, "metadata": "", "start": 18, "length": 15},
                {"name": "Conversion_Rate_Divisor", "type": "string", "nullable": true, "metadata": "", "start": 33, "length": 15},
                {"name": "Extract_Date", "type": "string", "nullable": true, "metadata": "", "start": 48, "length": 8},
                {"name": "Extract_Time", "type": "string", "nullable": true, "metadata": "", "start": 56, "length": 6}
            ]
        }
    """
    @abstractmethod
    def filter_df(self, data_frame: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def process_columns(self, data_frame: DataFrame) -> DataFrame:
        raise NotImplementedError

    def extract(self, config: dict) -> DataFrame:
        """
        Entry point for extract logic.

        Use schema provided to create the dataframe utilizing
        field names and column starts and lengths.  Also adds meta data.
        """
        path = config['source_path']
        schema = config['schema']

        if not schema:
            raise IngestSchemaError()

        df = self.get_spark().read.text(path)
        df = self.filter_df(df)

        for field in schema["fields"]:
            df = df.withColumn(field["name"], F.col("value").substr(field["start"], field["length"]).cast(field["type"]))

        df = self.process_columns(df)
        #df.cache()

        return df 

class IngestExcelFile(BaseIngest):
    """
    """

    def __get_pd_datatype(self, field_type: str) -> np.dtype:
        """
        Function to map Spark SQL types to Numpy types
        """        
        return {
                'string': str,
                'timestamp': np.dtype('datetime64[ns]'),
                'double': np.float64,
                'float': np.float64,
                'integer': np.int64,
                'bigint': np.int64,
            }[field_type]

    def __create_pd_excel_schema(self, schema: dict) -> dict:
        """
        Function to generate Pandas schema from JSON Config schema
        """
        pd_schema = {}

        #Read json schema here and generate pandas schema
        for index, field in enumerate(schema["fields"]):
            pd_schema[index] = self.__get_pd_datatype(field["type"])

        return pd_schema

    @staticmethod
    def __convert_nan_to_null(data_frame: DataFrame) -> DataFrame:
        """
        Function to convert NaN string values to NULL
        """        
        data_frame = data_frame.select([
            F.when(F.isnan(c), None).otherwise(F.col(c)).alias(c) if t in ("double", "float", "integer", "bigint") else c
            for c, t in data_frame.dtypes
        ])

        data_frame = data_frame.select([
            F.when(F.col(c) == 'nan', None).otherwise(F.col(c)).alias(c) if t == "string" else c
            for c, t in data_frame.dtypes
        ])        

        return data_frame

    @staticmethod
    def __validate_type_conversions(data_frame: DataFrame) -> DataFrame:
        """
        Function to find and record value errors
        """
        validate_column_dtypes = False

        for c, t in data_frame.dtypes:
            if t in ("double", "float", "int", "bigint"):
                validate_column_dtypes = True

        if validate_column_dtypes:
            predicate = " or ".join(
                ['isnan({})'.format(c) for c, t in data_frame.dtypes if t in ("double", "float", "int", "bigint")]
            )
 
            iptmeta_corrupt_record = "case when {} then concat_ws('|', *) else NULL end".format(predicate)
            data_frame = data_frame.withColumn("iptmeta_corrupt_record", F.expr(iptmeta_corrupt_record))

        return data_frame

    @staticmethod
    def __validate_key_columns(data_frame: DataFrame, key_columns: list) -> DataFrame:
        """
        Function to find and record null key values
        """
        if key_columns:
            predicate = " or ".join(['isnull({})'.format(i) for i in key_columns])
            iptmeta_corrupt_record = "case when {} then concat_ws('|', *) else NULL end".format(predicate)

            data_frame = data_frame.withColumn("iptmeta_corrupt_record", F.expr(iptmeta_corrupt_record))
        
        return data_frame

    @staticmethod
    def __get_file_paths(path: str) -> list:
        """
        Gets a list of files based on the given path.  This method
        handles S3 file system paths and local file system paths.  Paths can be
        an absolute path to a single file or a globbed folder/file path.
        """
         # The list of Excel files we'll be importing into a dataframe
        file_paths = []

        # If the path is an S3 filesystem, then use Boto3, the AWS SDK
        if path.startswith('s3://') and (path.endswith('*.xlsx') or path.endswith('/')):
            # Remove everything after last forward slash
            path = path.rsplit('/', 1)[0] + '/';
            # Get the bucket name from the path
            bucket_name = re.search('s3://(.*?)/', path).group(1)
            # Get the folder prefix
            prefix = re.search('s3://' + bucket_name + '/(.*)', path).group(1)
            # Get the S3 bucket
            bucket = boto3.resource('s3').Bucket(bucket_name)
            # Build a list of file paths from S3 Bucket
            for obj in bucket.objects.filter(Prefix=prefix):
                # Skip if object path is the parent folder since we only want file paths
                if obj.key == prefix or not obj.key.endswith(".xlsx"):
                    continue

                file_paths.append("s3://{0}/{1}".format(bucket_name, obj.key))
        elif path.startswith('/') and "*" in path:
            # If path starts with a forward slash then assume we're running locally
            file_paths = glob.glob(path)
        else:
            # An absolute path to a file is given (S3 or local) so just append
            file_paths.append(path)

        return file_paths

    def extract(self, config: dict) -> DataFrame:
        """
        """
        read_options = config['read_options']
        schema = config.get('schema')
        _read_options = read_options.copy()

        self.IMPORT_NANS = [
            '', 'N/A', 'NULL', 'null', 'NaN', 'n/a', 'nan', '#N/A', '#N/A N/A', '#NA', '-1.#IND',
            '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', 'None', '(blank)', ' '
        ]
        _read_options["na_values"] = self.IMPORT_NANS
        
        if not schema or not schema.get('fields'):
            raise IngestSchemaError()
        else:
            schema_struct = T.StructType.fromJson(schema) 
            pd_schema = self.__create_pd_excel_schema(schema=schema)
            _read_options['dtype'] = pd_schema
        
        # replaces normalize_column_names logic, built-in 'names' attribute is meant for this
        field_names = [i["name"] for i in schema['fields']]
        _read_options["names"] = field_names
        
        # Set range on skip rows if defined in config
        if "skiprows" in _read_options:
            start_row= _read_options["skiprows"]["start"]
            stop_row = _read_options["skiprows"]["stop"]
    
            _read_options["skiprows"] = list(range(start_row, stop_row))

        file_paths = self.__get_file_paths(_read_options["io"])

        # Remove i/o read_option since we already have a list of excel files to read
        _read_options.pop("io", None)

        # If sheet_optional flag is set to True, return False if any Excel file does not contain the sheet
        sheet_optional = config.get('sheet_optional')
        if sheet_optional:
            sheet_name = read_options['sheet_name']
            for file in file_paths:
                xl = pd.ExcelFile(file)
                if sheet_name not in xl.sheet_names:
                    return False

        import_pd_df = pd.concat((
            pd.read_excel(file, **_read_options)
            for file in file_paths))
        
        df = self.get_spark().createDataFrame(import_pd_df, schema_struct)
        df = self.__validate_type_conversions(df)
        df = self.__convert_nan_to_null(df)        
        df = self.__validate_key_columns(df, config.get('key_columns'))
        #df.cache()

        return df


class IngestDbSource(BaseIngest):

    def extract(self, config: dict) -> DataFrame:
        """
        Function to read data in from sqoop.db in hive.
        """
        hive_db = config['hive_db']
        hive_table = config['hive_table']

        spark = self.get_spark()
        query = 'SELECT * FROM sqoop.{}_{}'.format(hive_db, hive_table)
        df = spark.sql(query)

        return df


class IngestError(Exception):
    """
    Base class for exceptions in this module.
    """
    pass

class IngestSchemaError(IngestError):
    """
    Exception raised for ingest schema errors.
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self):
        self.expression = "Schema, with list of fields, is required."
        self.message = "Schema, with list of fields, is required."

class IngestTypeError(IngestError):
    """Exception raised for ingest type errors.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """
    def __init__(self, ingest_type=None, target_table=None, key_columns=None):
        self.expression = 'ingest_type: {}, target_table: {}, key_columns: {}'.format(
                ingest_type,
                target_table,
                key_columns
            )

        self.message = "Valid ingest types are 'full' and 'incremental'. \
                        If 'incremental', source keys and target table must be supplied "

class IngestPrimaryKeyViolationError(IngestError):
    """Exception raised for ingest primary key violations.

    Source violates configured primary key

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """
    def __init__(self, errMessage):
        self.expression = errMessage
        self.message = errMessage
        
class IngestNoDataError(IngestError):
    """Exception raised when Sqoop table is empty

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """
    def __init__(self, errMessage):
        self.expression = errMessage
        self.message = errMessage
