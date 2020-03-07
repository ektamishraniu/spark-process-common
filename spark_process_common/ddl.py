"""
Base abstractions/components to generate the CREATE TABLE DDL statements for both Hive and Redshift, and to
convert the data model class to either a Pyspark schema (StructType class) or a JSON representation of the Spark schema.
The idea is to have one and only one place to define schemas and make changes (new columns, etc),
and then propagate those changes throughout our data pipelines via code generation and proper schema evolution patterns.
"""
import decimal
import logging
import json
import datetime as dt

import sqlalchemy as sa
from sqlalchemy import Column, exc
from pyhive.sqlalchemy_hive import HiveDialect, HiveTypeCompiler
from pyspark.sql.types import StructType
from sqlalchemy import util
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql.compiler import DDLCompiler
from sqlalchemy_redshift.dialect import RedshiftDDLCompiler

from .db import create_lazy_db_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


Base = declarative_base()


class HiveDDLCompiler(DDLCompiler):
    """
    Handles Hive-specific ``CREATE TABLE`` syntax.
    Users can specify the `comment`, `partition_by`, `stored_as`, and `location` properties per table.
    Table level properties can be set using the dialect specific syntax. For
    example, to specify a partition key you apply the following:
    >>> import sqlalchemy as sa
    >>> from sqlalchemy.schema import CreateTable
    >>> engine = sa.create_engine('hive://example')
    >>> metadata = sa.MetaData()
    >>> user = sa.Table(
    ...     'user',
    ...     metadata,
    ...     sa.Column('id', sa.Integer),
    ...     sa.Column('name', sa.String),
    ...     hive_partition_by='name',
    ... )
    >>> print(CreateTable(user).compile(engine))
    <BLANKLINE>
    CREATE TABLE "user" (
        id INT,
        name STRING,
    )
    PARTITIONED BY (name)
    <BLANKLINE>
    <BLANKLINE>
    """

    def visit_create_table(self, create):
        """
        Overrides the ``visit_create_table()`` method on the DDLCompiler base class to implement Hive-specific
        logic, such as creating external tables, removing foreign key and unique constraints (since those are not
        supported by Hive), and making sure the partition columns are not duplicated in the column definitions.
        """
        table = create.element
        preparer = self.preparer

        text = "\nCREATE "
        if table._prefixes:
            text += " ".join(table._prefixes) + " "
        text += "EXTERNAL TABLE " + preparer.format_table(table) + " "

        create_table_suffix = self.create_table_suffix(table)
        if create_table_suffix:
            text += create_table_suffix + " "

        text += "("

        separator = "\n"

        partition_keys = table.dialect_options['hive']._non_defaults.get('partition_by', [])
        partition_cols = []

        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(create_column, first_pk=False)
                if processed is not None:
                    if column.name in partition_keys:
                        partition_cols.append(processed)
                        continue
                    text += separator
                    separator = ", \n"
                    text += "\t" + processed
            except exc.CompileError as ce:
                util.raise_from_cause(
                    exc.CompileError(
                        util.u("(in table '%s', column '%s'): %s")
                        % (table.description, column.name, ce.args[0])
                    )
                )

        table.dialect_options['hive']._non_defaults['partition_by'] = partition_cols

        text += "\n)%s\n\n" % self.post_create_table(table)
        return text

    def post_create_table(self, table):
        """
        Add the logic to support table options specific to the Hive dialect. This allows us to
        specify the `comment`, `partition_by`, `stored_as`, and `location` properties per table in
        data model classes.
        """
        table_opts = []
        info = table.dialect_options['hive']

        comment = info.get('comment')
        if comment:
            table_opts.append(f" \nCOMMENT '{comment}'")

        partition_by = info.get('partition_by')
        if partition_by:
            if isinstance(partition_by, str):
                keys = [partition_by]
            else:
                keys = partition_by
            keys = [key.name if isinstance(key, Column) else key for key in keys]
            partition_by_string = ", ".join(keys)
            table_opts.append(f'PARTITIONED BY ({partition_by_string})')

        stored_as = info.get('stored_as')
        if stored_as:
            stored_as = stored_as.upper()
            if stored_as not in ('PARQUET', 'ORC', 'AVRO'):
                raise exc.CompileError(f'stored_as {stored_as} is invalid.')
            table_opts.append(f'STORED AS {stored_as}')

        location = info.get('location')
        if location:
            table_opts.append(f"LOCATION '{location}'")

        return ' \n'.join(table_opts)

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        colspec += " " + self.dialect.type_compiler.process(column.type)
        return colspec


class HiveDateTypeCompiler(HiveTypeCompiler):
    def visit_DATE(self, type_):
        return 'DATE'


def create_serializable_mapper(model_class):
    """Get data model columns."""
    mapper = sa.inspect(model_class)
    return mapper.columns


def create_table_ddl(table, engine):
    ddl = CreateTable(table).compile(engine)
    return ddl


def to_hive_ddl(model_class):
    """Generate Hive ``CREATE TABLE`` DDL for any defined model class."""
    HiveDialect.ddl_compiler = HiveDDLCompiler
    HiveDialect.type_compiler = HiveDateTypeCompiler
    engine = sa.create_engine('hive://localhost:10000/default', dialect=HiveDialect())
    ddl = CreateTable(model_class).compile(engine)
    return ddl


def to_redshift_ddl(model_class):
    """
    Generate Redshift ``CREATE TABLE`` DDL for any defined model class.

    Forces a conditional ``IF NOT EXISTS`` check by reassigning the ``visit_create_table()`` method
    to use the wrapped function instead of subclassing RedshiftDDLCompiler and then re-registering the dialect.
    """
    engine = create_lazy_db_engine('redshift')

    def _redshift_visit_create_table(compiler, create):
        table = create.element
        preparer = compiler.preparer

        text = "\nCREATE "
        if table._prefixes:
            text += " ".join(table._prefixes) + " "
        text += "TABLE IF NOT EXISTS " + preparer.format_table(table) + " "

        create_table_suffix = compiler.create_table_suffix(table)
        if create_table_suffix:
            text += create_table_suffix + " "

        text += "("

        separator = "\n"

        # if only one primary key, specify it along with the column
        first_pk = False
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = compiler.process(
                    create_column, first_pk=column.primary_key and not first_pk
                )
                if processed is not None:
                    text += separator
                    separator = ", \n"
                    text += "\t" + processed
                if column.primary_key:
                    first_pk = True
            except exc.CompileError as ce:
                util.raise_from_cause(
                    exc.CompileError(
                        util.u("(in table '%s', column '%s'): %s")
                        % (table.description, column.name, ce.args[0])
                    )
                )

        const = compiler.create_table_constraints(
            table,
            _include_foreign_key_constraints=create.include_foreign_key_constraints,  # noqa
        )
        if const:
            text += separator + "\t" + const

        text += "\n)%s\n\n" % compiler.post_create_table(table)
        return text

    RedshiftDDLCompiler.visit_create_table = _redshift_visit_create_table
    ddl = CreateTable(model_class).compile(engine)
    return ddl


def to_json_schema(model_class):
    """Generate the JSON representation of the schema for a defined model class."""
    type_mapping = {
        str: 'string',
        int: 'integer',
        bool: 'boolean',
        float: 'double',
        list: 'array',
        dict: 'struct',
        dt.date: 'date',
        dt.datetime: 'timestamp',
        decimal.Decimal: 'decimal',
    }

    mapper = sa.inspect(model_class)

    schema = {'fields': []}
    for name, col in mapper.columns.items():
        field = {
            'metadata': {},
            'name': name,
            'nullable': col.nullable,
            'type': type_mapping.get(col.type.python_type)
        }
        schema['fields'].append(field)

    return json.dumps(schema, indent=2)


def to_pyspark_schema(model_class):
    """Generate the Pyspark schema for a defined model class."""
    json_schema = to_json_schema(model_class)
    schema_dict = json.loads(json_schema)
    pyspark_schema = StructType.fromJson(schema_dict)
    return pyspark_schema
