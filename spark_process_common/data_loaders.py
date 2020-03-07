"""
This file contains the data loaders for
the spark processing files.  These classes
help decouple the transforms to support unit testing.
"""

from pyspark.sql import DataFrame


def format_s3_path(bucket: str, prefix: str) -> str:
    return 's3a://{0}/{1}'.format(bucket, prefix)


def write_to_s3(df: DataFrame, bucket: str, prefix: str, options: dict):
    s3_path = format_s3_path(bucket, prefix)
    df.write.parquet(s3_path, mode='overwrite', **options)


class OrcDataLoader(object):
    """
    This class loads the supplied data sources based on the
    paths provided when the class was instantiated.  This will
    cause all of the data frames to be loaded at when the first
    data source is loaded.
    Note: This class operates on ORC formatted files.

    sample input:
    source_paths = {"key": "path_to_orc_file"}
    """

    def __init__(self, source_paths):
        self.source_dfs = {}
        self.source_paths = source_paths

    def get_dfs(self, sqlContext):
        if not self.source_dfs:
            for key, path in self.source_paths.items():
                self.source_dfs[key] = sqlContext.read.format("orc").load(path)

        return self.source_dfs

    def get_df(self, df_key, sqlContext):
        return self.get_dfs(sqlContext)[df_key]


class CsvDataLoader(object):
    """
    This class loads the supplied data sources based on the
    paths provided when the class was instantiated.  This will
    cause all of the data frames to be loaded at when the first
    data source is loaded.
    Note: This class operates on CSV formatted files, but provides
    the ability to specify a delimiter.

    sample input without delimeter:
    source_paths = {"key1": {"path":"path_to_csv_file"},
                    "key2": {"path":"path_to_csv_file2"}}

    sample input with mixed delimiters:
    source_paths = {"key1": {"path":"path_to_csv_file"},
                    "key2": {"path":"path_to_csv_file2", "delim":"|"}}
    """

    def __init__(self, source_paths):
        self.source_dfs = {}
        self.source_paths = source_paths

    def get_dfs(self, sqlContext):
        if not self.source_dfs:
            for key, source_dict in self.source_paths.items():
                if "delim" in source_dict:
                    self.source_dfs[key] = sqlContext.read.format("csv") \
                        .option("header","true") \
                        .option("inferSchema","true") \
                        .option("delimiter", source_dict["delim"]) \
                        .load(source_dict["path"])
                else:
                    self.source_dfs[key] = sqlContext.read.format("csv") \
                        .option("header","true") \
                        .option("inferSchema","true") \
                        .load(source_dict["path"])

        return self.source_dfs

    def get_df(self, df_key, sqlContext):
        return self.get_dfs(sqlContext)[df_key]


class CompositeDataLoader(object):
    """
    This class allows the caller to mix the different types of
    Data Loaders, but there is a risk.  Since this class is just
    creating a dictionary of the combined DataFrames of all of the
    DataLoaders, the keys could overwrite one another.

    input is an array of DataLoader instances.
    [csvDataLoader, orcDataLoader]
    """

    def __init__(self, source_data_loaders):
        self.source_dfs = {}
        self.source_data_loaders = source_data_loaders

    def get_dfs(self, sqlContext):
        """
        Construct a flattened dictionary of all DataLoader keys
        with the corresponding get_df functions for the data_loaders.
        """
        if not self.source_dfs:
            for data_loader in self.source_data_loaders:
                for key in data_loader.get_dfs(sqlContext):
                    self.source_dfs[key] = data_loader.source_dfs[key]
        return self.source_dfs

    def get_df(self, df_key, sqlContext):
        return self.get_dfs(sqlContext)[df_key]
