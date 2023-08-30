from enum import StrEnum


# https://docs.sqlalchemy.org/en/20/core/engines.html
class SqlAlchemyDialect(StrEnum):
    MSSQL = "mssql"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    MYSQL = "mysql"
    ORACLE = "oracle"
