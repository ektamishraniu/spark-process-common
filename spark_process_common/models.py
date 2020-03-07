from abc import ABCMeta, abstractmethod

from pyspark.sql import SparkSession, DataFrame, functions as F

from spark_process_common.transforms import hash_columns

class BaseTransform(metaclass=ABCMeta):

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def transform(self, *args, **kwargs):
        raise NotImplementedError

class ProcessTransform(BaseTransform):

    def transform(self, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    def add_meta_data_columns(
        data_frame: DataFrame, target_table_name: str, 
        key_columns: list(), process_name: str
    ) -> DataFrame:
        """
        Add meta data specific to the 'transform' pattern
        and a surrogate key from the hash of unique key values
        """
        target_key = "{} as {}_id".format(hash_columns(key_columns), target_table_name)
        non_key_columns = [i for i in data_frame.columns if i[:8] != 'iptmeta_' and i not in key_columns]

        iptmeta_insert_dttm = "current_timestamp() as iptmeta_insert_dttm"
        iptmeta_diff_md5 = "{} as iptmeta_diff_md5".format(hash_columns(non_key_columns))
        iptmeta_process_name = "'{}' as iptmeta_process_name".format(process_name)

        df_target_table = data_frame.selectExpr(
            target_key,
            key_columns,
            iptmeta_insert_dttm,
            iptmeta_process_name,
            iptmeta_diff_md5,
            non_key_columns
        )
        
        return df_target_table

    @staticmethod
    def save(data_frame: DataFrame, target_path: str, write_options: dict=None):
        """
        Write csv output to be loaded into Redshift
        """
        if not write_options:
            write_options = {
                "sep": '|',
                "header": 'true',
                "timestampFormat": 'yyyy-MM-dd HH:mm:ss',
                "mode": 'overwrite'
            }

        data_frame.write.csv(target_path, **write_options)
