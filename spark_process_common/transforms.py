"""
This module contains common spark transformations performed
in the various pipelines.
"""
import calendar
import datetime
import functools
import re
import boto3
import os
from collections import OrderedDict

from pandas.tseries.holiday import USFederalHolidayCalendar

from pyspark.sql import DataFrame, Row, functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType
from fractions import Fraction


def build_col_expr(mapping: dict) -> list:
    """
    Build a list of column expressions from a dictionary mapping column names to valid
    spark column expressions.

    Allows composable select expressions that can be reused across multiple dataframes or transformation steps.
    The dictionary can also contain values of None, and string expressions that are
    valid in `DataFrame.selectExpr( ... )`.
    """
    expressions = []
    for name, expr in mapping.items():
        column_expr = expr if expr is not None else name
        if isinstance(column_expr, str):
            column_expr = F.expr(column_expr)
        expressions.append(column_expr.alias(name))
    return expressions


def calcfiscalyear(fiscalyear, fiscalmonth):
    """
    Create the Fiscal year format FY17 where oct, nov, dec advance to the next year from the four digit year.
    """
    if str(fiscalmonth).lower() == "oct" or str(fiscalmonth).lower() == "nov" or str(fiscalmonth).lower() == "dec":
        return "FY" + str(int(fiscalyear) + 1)[-2:]
    else:
        return "FY" + str(fiscalyear)[-2:]


def calcaccountingperiod(period_number):
    """
    Shift the period number to align with the normal month numbers 1 == Jan, 2 == Feb, etc.
    So that 12 == Sept and 1 == Oct.
    """
    return calendar.month_abbr[(period_number - 3 if (period_number - 3 > 0) else (period_number - 3) + 12)].upper()


def convertDecimalToFraction(mynum,denom = 16):
    """
    Convert decimal number to a fraction, denominator can be specified default is 16.
    For example 30.375 will be converted to 30 6/16
    """
    mynum_decimal = str(mynum-int(mynum))[1:]
    res = Fraction(mynum_decimal).limit_denominator()
    numer = int(res.numerator*(denom/res.denominator))
    if numer == 0:
        output = str(int(mynum))
    elif int(mynum) == 0:
        output = str(numer) + "/" + str(denom)
    else:
        output = str(int(mynum)) + " " +  str(numer) + "/" + str(denom)
    return output


def null_as_blank(x):
    """
    Replace null value with blank string.
    """
    return F.when(x.isNotNull(), x).otherwise(F.lit(''))


def unionAll(dfs):
    """
    Accepts a list of dataframes and combines them into one.
    """
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)


def snek(name: str) -> str:
    """
    Convert the given string to snake case so that it can be used for a clean,
    valid column name in Spark SQL. Remove leading and trailing spaces,
    convert other spaces to underscores, and remove anything that is not
    an alphanumeric, underscore, or dot.
    """
    re_camel_case = re.compile(r'([a-z0-9])([A-Z])')
    re_clean = re.compile(r'(?u)[^\w\s.]')
    cleaned = re_clean.sub('', name)
    cased = re_camel_case.sub(r'\1_\2', cleaned).strip().replace(' ', '_').lower()
    return cased


def normalize_columns(df):
    renamed = [snek(c) for c in df.columns]
    return df.toDF(*renamed)


def format_currency_conversion_rates(spark_session, currency_exchange_df, fiscalyear, fiscalmonth, day='01'):
    """
    Given a year, month, and day, retrieve that date's currency conversion rates
    from currency_exchange_df.  Currency exchange markets are closed on weekends
    and banking holidays, so if the given date falls on a weekend/holiday the date
    is set to the last non-holiday, non-weekend date.
    """
    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(
        start=f'{fiscalyear}-01-01', end=f'{fiscalyear}-12-31').to_pydatetime()

    date_string = f"{fiscalyear}-{datetime.datetime.strptime(fiscalmonth, '%b').month}-{day}"
    lookup_date = datetime.datetime.strptime(date_string, '%Y-%m-%d')

    # If the lookup date is on a weekend, rewind till last Friday.
    if lookup_date.weekday() == 5:
        lookup_date = lookup_date - datetime.timedelta(days=1)
    elif lookup_date.weekday() == 6:
        lookup_date = lookup_date - datetime.timedelta(days=2)

    if datetime.datetime.today() < lookup_date:
        raise ValueError(f"{lookup_date} hasn't occured yet. Given date must be {datetime.date.today()} or before.")

    if lookup_date not in holidays:
        df = (
            currency_exchange_df
            .where(F.col('effective_date') == lookup_date.strftime('%Y%m%d'))
            .where(F.col('currency_code_to') == 'USD')
        )
    else:
        if lookup_date.weekday() == 0: # If the holiday falls on a Monday, pull from last Friday.
            df = (
                currency_exchange_df
                .where(F.col('effective_date') == (lookup_date - datetime.timedelta(days=3)).strftime('%Y%m%d'))
                .where(F.col('currency_code_to') == 'USD')
            )
        else:  # Pull from the day before the holiday.
            df = (
                currency_exchange_df
                .where(F.col('effective_date') == (lookup_date - datetime.timedelta(days=1)).strftime('%Y%m%d'))
                .where(F.col('currency_code_to') == 'USD')
            )
    df_currencyconversionrates = (
        df.drop(
            'record_type', 'currency_code_to', 'effective_date',
            'conversion_rate_multiplier', 'extract_date', 'extract_time',
            'iptmeta_corrupt_record', 'iptmeta_extract_dttm')
        .withColumnRenamed('currency_code_from', 'Curr_Code')
        .withColumnRenamed('conversion_rate_divisor', 'Conversion_Rate')
    )

    if df_currencyconversionrates.rdd.isEmpty():
        raise ValueError(f"No currency conversion rates found for {lookup_date}.")

    usd_df = spark_session.createDataFrame([('USD', 1.0)], ("Curr_Code", "Conversion_Rate"))
    df_currencyconversionrates = df_currencyconversionrates.union(usd_df)

    return df_currencyconversionrates


def retrieve_hive_table(spark_session, hive_db, hive_table):
    hive_table_df = spark_session.table(f'{hive_db}.{hive_table}')
    return hive_table_df


def hash_columns(columns: list) -> str:
    """
    Formats a list of columns into a single string
    representating a call to pyspark.sql.functions.md5 to be passed to DataFrame.selectExpr()
    """
    _columns = columns.copy()
    _columns.sort()
    cast_columns = ','.join(["cast({} as string)".format(i) for i in _columns])
    md5_columns = "md5(concat_ws('~',{}))".format(cast_columns)
    return md5_columns


def coerce_to_schema(normalized_df: DataFrame, to_schema: StructType, aliases: dict) -> DataFrame:
    """
    Common transformation to ensure a DataFrame adheres to the correct schema for consistency across all
    Spark jobs. Handles column ordering, renaming, and type coercion.

    Currently using a `collections.OrderedDict` to build the mapping, but this can be changed to a native
    Python dictionary as soon as we update our EMR cluster and AWS Data Pipeline to use >= 3.6. Python 3.4 dicts
    do not preserve ordering, which is important in this case because the mapping gets translated to a select statement.

    :param normalized_df: DataFrame that represents the final output of a transformation but doesn't yet match the
    required schema exactly.
    :param to_schema: The defined `spark.sql.types.StructType` schema into which the input DataFrame should be
    converted.
    :param aliases: Dictionary mapping columns in the input DataFrame to the respective schema column names.
    :return: A new DataFrame that matches the required schema.
    """
    types = {f['name']: f['type'] for f in to_schema.jsonValue()['fields']}
    mapping = OrderedDict()
    for name in to_schema.names:
        if name not in aliases:
            selected = name
        else:
            selected = snek(aliases[name])
        mapping[name] = F.col(selected).cast(types[name])
    expr = build_col_expr(mapping)
    coerced = normalized_df.select(expr)
    return coerced


def add_fiscal_year_and_month_abbr(
        df, date_fmt: str='yyyy/MM/dd',
        filter_column_year: str='voucher_creation_date',
        filter_column_month: str='shipment_pickup_date') -> DataFrame:
    expr_mapping = {
        '_fiscal_year': (
            F.coalesce(
                F.year(F.add_months(
                    F.to_date(filter_column_year, date_fmt), 3)),
                F.year(F.add_months(
                    F.to_date(filter_column_month, date_fmt), 3))
            )
        ),
        '_month_abbr': (
            F.coalesce(
                F.upper(F.date_format(
                    F.to_date(filter_column_year, date_fmt), 'MMM')),
                F.upper(F.date_format(
                    F.to_date(filter_column_month, date_fmt), 'MMM'))
            )
        )
    }
    select_expr = build_col_expr(expr_mapping)
    transformed = df.select(F.expr('*'), *select_expr)
    return transformed


def filter_fiscal_year_and_month(
        df: DataFrame, fiscal_year: str, month_abbr: str,
        date_fmt: str='MM/dd/yyyy',
        filter_column_year: str='voucher_creation_date',
        filter_column_month: str='shipment_pickup_date') -> DataFrame:
    filtered = (
        df
        .withColumn(
            '_fiscal_year',
            F.coalesce(
                F.year(F.add_months(
                    F.to_date(filter_column_year, date_fmt), 3)),
                F.year(F.add_months(
                    F.to_date(filter_column_month, date_fmt), 3))
            ))
        .withColumn(
            '_month_abbr',
            F.coalesce(
                F.upper(F.date_format(
                    F.to_date(filter_column_year, date_fmt), 'MMM')),
                F.upper(F.date_format(
                    F.to_date(filter_column_month, date_fmt), 'MMM'))
            ))
        .where(F.col('_fiscal_year') == fiscal_year)
        .where(F.col('_month_abbr') == month_abbr)
    )  # yapf: disable
    return filtered


def cad_to_usd_rate(currency_exchange_rates: DataFrame, fiscal_year: str, month_abbr: str) -> float:
    """Currently returns latest exchange rate for the given month."""
    filtered = (
        currency_exchange_rates
        .where(F.col('currency_code_from') == 'CAD')
        .where(F.col('currency_code_to') == 'USD')
        .where(F.year(F.add_months(F.to_date(
            'effective_date', 'yyyyMMdd'), 3)) == fiscal_year)
        .where(F.upper(F.date_format(F.to_date(
            'effective_date', 'yyyyMMdd'), 'MMM')) == month_abbr)
        .sort('effective_date', ascending=False)
    )  # yapf: disable
    return filtered.first().conversion_rate_multiplier


def get_latest_exchange_rate(currency_exchange_rates: DataFrame) -> float:
    """Return the most recent currency exchange rate."""
    latest_rate = (
        currency_exchange_rates
        .where(F.col('currency_code_from') == 'CAD')
        .where(F.col('currency_code_to') == 'USD')
        .sort('effective_date', ascending=False)
    )  # yapf: disable
    return latest_rate.first().conversion_rate_multiplier


def rename_file(s3_bucket: str, prefix: str, target_key: str):
    """Rename the saved partitioned auto named file to desired format."""
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    for object_summary in bucket.objects.filter(Prefix=prefix):
        object_path = object_summary.key
        file_name = re.search('[^/]+$', object_path).group()
        if file_name.startswith('part'):
            source_key = f'{prefix}/{file_name}'
            copy_and_delete(s3_bucket, source_key, target_key)


def copy_and_delete(s3_bucket: str, source_key: str, target_key: str):
    copy_s3_object(s3_bucket, source_key, target_key)
    delete_s3_object(s3_bucket, source_key)


def copy_s3_object(s3_bucket: str, source_key: str, target_key: str):
    s3 = boto3.resource('s3')
    new_object = s3.Object(s3_bucket, target_key)
    new_object.copy_from(CopySource=f'{s3_bucket}/{source_key}')


def delete_s3_object(s3_bucket: str, source_key: str):
    s3 = boto3.resource('s3')
    previous_object = s3.Object(s3_bucket, source_key)
    previous_object.delete()
