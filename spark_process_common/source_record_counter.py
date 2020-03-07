from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

from spark_process_common import logging
from spark_process_common.transforms import filter_fiscal_year_and_month


class SourceRecordCounter:
    def __init__(self, spark, logger=None):
        self.spark = spark
        self.logger = logger or logging.getLogger()

    @property
    def empty_dataframe(self):
        empty_schema = StructType([])
        return self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), empty_schema)

    def get_record_count(self, sources: dict, source_name: str):
        source = sources[source_name]
        df = self._extract(source)
        record_count = df.count()
        self.logger.info(self.get_default_log_msg_for_source(source, source_name, record_count))
        return record_count

    def get_record_count_for_period(self, sources, source_name, fiscal_year, month_abbr):
        source = sources[source_name]
        date_format = source['date_format']
        date_col_1 = source['primary_filter_column']
        date_col_2 = source['secondary_filter_column']
        df = self._extract(source)
        df = filter_fiscal_year_and_month(df, fiscal_year, month_abbr, date_format, date_col_1, date_col_2)
        record_count = df.count()
        msg = self.get_default_log_msg_for_source(source, source_name, record_count)
        self.logger.info(msg + f" Year: {fiscal_year} Month: {month_abbr}")
        return record_count

    def get_record_count_with_filter(self, sources, source_name, query_filter):
        source = sources[source_name]
        df = self._extract(source)
        raw_count = df.count()
        self.logger.info("Before query filter: " + self.get_default_log_msg_for_source(source, source_name, raw_count))
        filtered_df = df.filter(query_filter)
        filtered_count = filtered_df.count()
        default_msg = self.get_default_log_msg_for_source(source, source_name, filtered_count)
        self.logger.info(default_msg + f" Query Filter: {query_filter}")
        return filtered_count

    @staticmethod
    def get_default_log_msg_for_source(source, source_name, record_count):
        location = source.get('path') or source.get('hive_table')
        return f"Source: {source_name} location: {location} count: {record_count}."

    def _extract(self, source: dict) -> DataFrame:
        if source.get('hive_table'):
            return self.spark.table(source.get('hive_table'))
        source_format = self._get_source_format(source)
        if source_format == 'csv':
            return self._extract_csv(source)
        else:
            return self._extract_or_empty(source, source_format)

    @staticmethod
    def _get_source_format(source):
        source_format = source.get('format', '').lower()
        if not source_format:
            source_format = source['path'].split('.')[-1].lower()
        return source_format

    def _extract_csv(self, source: dict) -> DataFrame:
        path = source['path']
        read_options = source.get('read_options')
        if read_options is None:
            read_options = {'header': True, 'inferSchema': True, }
        return self.spark.read.csv(path, **read_options)

    def _extract_or_empty(self, source, file_format):
        valid_file_formats = ['parquet', 'orc']
        if file_format not in valid_file_formats:
            raise ValueError("Unknown file format. Supported file formats are csv, orc, and parquet")
        extract_method = getattr(self.spark.read, file_format)
        try:
            return extract_method(source['path'])
        except AnalysisException as e:
            msg = f"Failed to read source file from location: {source['path']}.\nBuilding empty dataframe..."
            self.logger.error(str(e))
            self.logger.info(msg)
            return self.empty_dataframe