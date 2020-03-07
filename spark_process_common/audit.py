from datetime import datetime

from pyspark.sql import DataFrame, functions as F


class NullDataFrameAuditor:
    """
    Audit a DataFrame for records with null values in the specified audit_columns.
    If a null value is found in the audit_columns for any record, that column name
    is set as the value in the newly appended "audit_reason" column.

    Usage:
    my_df = spark.createDataFrame([
        {'pk': 1, 'key': 'one'},
        {'pk': 2, 'key': None},
        {'pk': None, 'key': 'three'}])
    audit_records = NullDataFrameAuditor.audit(df=my_df, audit_columns=['pk', 'key'])
    audit_records.show()
    +-----+----+------------+
    |  key|  pk|audit_reason|
    +-----+----+------------+
    | null|   2|         key|
    |three|null|          pk|
    +-----+----+------------+
    """

    @classmethod
    def audit(cls, df: DataFrame, audit_columns: list, filter_operator: str = 'or'):
        filtered_records = cls.filter(df, audit_columns, filter_operator)
        audit_records = cls.add_reason(filtered_records, audit_columns)
        return audit_records

    @classmethod
    def audit_and_write(cls, df: DataFrame, target_path: str, audit_columns: list, filter_operator: str = 'or'):
        audit_records = cls.audit(df, audit_columns, filter_operator)
        audit_path = cls.append_dttm_to_path(target_path)
        audit_records.coalesce(1).write.csv(path=audit_path, mode='overwrite', sep=',', header=True)
        return audit_records

    @staticmethod
    def subtract_audit_records(source: DataFrame, audited: DataFrame):
        return source.subtract(audited.select(source.columns))

    @staticmethod
    def add_reason(df: DataFrame, audit_columns: list):
        select_cols = [F.when(F.col(col).isNull(), col) for col in audit_columns]
        return df.select('*', F.coalesce(*select_cols).alias('audit_reason'))

    @staticmethod
    def append_dttm_to_path(path):
        return f"{path}/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"

    @staticmethod
    def filter(df: DataFrame, audit_columns: list, filter_operator: str):
        filter_clause = f" {filter_operator} ".join([f"{col} is NULL" for col in audit_columns])
        return df.filter(filter_clause)
