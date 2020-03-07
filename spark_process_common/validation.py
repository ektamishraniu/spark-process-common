"""
Various helper functions to analyze and compare an output DataFrame from one of our actual transformation jobs
to the corresponding expected output DataFrame for the same job. The functions contained in this module should be
generic enough to be used across all segments and repositories (Enterprise Sales, Mill Profitability, etc.).
"""
from collections import defaultdict

from pyspark.sql import functions as F


class DataFrameMissingColumnError(ValueError):
    pass


class DataFrameMissingStructFieldError(ValueError):
    pass


class DataFrameSchemaMismatchError(ValueError):
    pass


def agg_count_sum_column_values(df, col):
    return (
        df.groupBy(col)
        .agg(
            F.count(col).alias('num_records'),
            F.sum('amount').alias('sum_amount'))
        .sort('num_records', ascending=False)
    )


def extract_actual_and_expected(spark_session, actual_filepath, expected_filepath):
    read_options = {'header': True, 'inferSchema': True}
    actual_df = spark_session.read.csv(actual_filepath, **read_options)
    expected_df = spark_session.read.csv(expected_filepath, **read_options)
    return actual_df, expected_df


def compare_summaries(actual, expected):
    ac_desc = actual.describe('amount').withColumnRenamed('amount', 'actual_amount')
    ex_desc = expected.describe('amount').withColumnRenamed('amount', 'expected_amount')
    desc_diff = (
        ac_desc.join(ex_desc, 'summary', 'left_outer')
        .withColumn('amount_difference', ac_desc.actual_amount - ex_desc.expected_amount)
    )
    return desc_diff


def compare_frequent_values(actual, expected, grouping_cols):
    actual_freq = actual.freqItems(grouping_cols).collect()[0].asDict()
    expected_freq = expected.freqItems(grouping_cols).collect()[0].asDict()
    assert actual_freq.keys() == expected_freq.keys()
    missing_values = defaultdict(set)
    for name, values in expected_freq.items():
        missing_values[name] = set(values) - set(actual_freq[name])
    return missing_values


def count_distinct_values(df):
    unique_expr = [F.countDistinct(name).alias(name) for name in df.columns if name != 'amount']
    unique_counts = df.select(unique_expr).collect()[0].asDict()
    return unique_counts


def compute_relative_accuracy(actual, expected):
    """
    This is a somewhat naive approach and will likely develop into a more complete algorithm over time.
    """
    intersection = expected.intersect(actual)
    accuracy = (intersection.count() / expected.count()) * 100
    return accuracy


def create_md5_hash_keys(df, grouping_cols):
    return (
        df
        .withColumn('hash_key', F.md5(F.concat_ws('__', *grouping_cols)))
        .select(*grouping_cols, 'hash_key', 'amount')
        .sort(grouping_cols)
    )


def get_grouping_columns(df):
    unique_counts = count_distinct_values(df)
    grouping_cols = [key for key, val in unique_counts.items() if val > 1]
    return grouping_cols


def symmetric_difference(actual, expected, grouping_cols):
    expected_only = (
        expected
        .join(actual, grouping_cols, 'left_anti')
        .withColumn('appears_in', F.lit('expected_only'))
    )
    actual_only = (
        actual
        .join(expected, grouping_cols, 'left_anti')
        .withColumn('appears_in', F.lit('actual_only'))
    )
    return expected_only.union(actual_only)


def validate_presence_of_columns(df, pyspark_schema):
    """Verify that a Spark DataFrame contains all the columns in the specified Pyspark schema."""
    required_col_names = pyspark_schema.fieldNames()
    actual_col_names = df.columns

    missing_col_names = set(required_col_names) - set(actual_col_names)
    if missing_col_names:
        error_message = f"The {missing_col_names} columns are not included in the DataFrame with the following columns {actual_col_names}"
        raise DataFrameMissingColumnError(error_message)

    extra_col_names = set(actual_col_names) - set(required_col_names)
    if extra_col_names:
        error_message = f"The DataFrame contains extra columns that are not included in the required schema: {extra_col_names}"
        raise DataFrameMissingColumnError(error_message)


def validate_struct_fields(df, required_schema):
    """
    Verify that all columns in a Spark DataFrame have matching data types and parameters when compared
     to the required Pyspark schema.
    """
    all_struct_fields = df.schema
    missing_struct_fields = [x for x in required_schema if x not in all_struct_fields]
    error_message = f'The DataFrame is missing the following StructFields: {missing_struct_fields}'
    if missing_struct_fields:
        raise DataFrameMissingStructFieldError(error_message)
    non_matching_fields = set(all_struct_fields.fields()) - set(required_schema.fields())
    if non_matching_fields:
        error_message = f'The DataFrame contains StructFields that are not in the defined schema: {non_matching_fields}'
        raise DataFrameSchemaMismatchError(error_message)


def validate_schema(df, required_schema):
    """Combine individual validation functions to determine if the schema of a DataFrame is valid."""
    try:
        validate_presence_of_columns(df, required_schema)
        validate_struct_fields(df, required_schema)
        return True
    except ValueError:
        raise
