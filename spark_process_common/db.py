"""
A collection of utility functions to be used in conjunction with data model classes for auto-generating dialect-specific
DDL, and building configuration objects to represent data sources using composition patterns.

Currently SQLAlchemy is being used to define schemas and generate DDL statements, not for any ORM functionality.
In the future this module may also include database migration tools based on the Alembic library.
"""
import sqlalchemy as sa


class UnsupportedDialectError(Exception):
    pass


def create_lazy_db_engine(dialect):
    lazy_connection_mapping = {
        'hive': 'hive://localhost:10000/default',
        'redshift': 'redshift+psycopg2://localhost:5439/default'
    }
    try:
        db_url = lazy_connection_mapping.get(dialect)
    except KeyError as e:
        raise UnsupportedDialectError from e
    return sa.create_engine(db_url)


def create_hive_engine(db_url=None):
    if db_url is None:
        db_url = 'hive://localhost:10000/default'
    return sa.create_engine(db_url)


def create_redshift_db_url(username, password, hostname, db_name, jdbc=False, lazy=True):
    if lazy:
        db_url = 'redshift+psycopg2://localhost:5439/default'
    elif jdbc:
        db_url = (
            'jdbc:postgresql://{hostname}:5432/{db_name}?'
            'user={username}&password={password}'.format(
                username=username, password=password, hostname=hostname, db_name=db_name
            )
        )
    else:
        db_url = "redshift_psycopg2://{username}:{password}@{hostname}:5439/{db_name}".format(
            username=username, password=password, hostname=hostname, db_name=db_name
        )
    return db_url


def create_redshift_engine(db_url=None):
    if db_url is None:
        db_url = 'redshift+psycopg2://localhost:5439/default'
    return sa.create_engine(db_url)


def create_postgres_db_url(username, password, hostname, db_name, jdbc=True):
    if jdbc:
        db_url = (
            'jdbc:postgresql://{hostname}:5432/{db_name}?'
            'user={username}&password={password}'.format(
                username=username, password=password, hostname=hostname, db_name=db_name
            )
        )
    else:
        db_url = 'postgresql://{username}:{password}@{hostname}:5432/{db_name}'.format(
            username=username, password=password, hostname=hostname, db_name=db_name
        )
    return db_url


def create_postgres_engine(db_url):
    return sa.create_engine(db_url)
