from sqlalchemy import URL

from db_utils.url.enums import SqlAlchemyDialect


def create_url_from_args(
    dialect: SqlAlchemyDialect,
    username: str,
    password: str,
    host: str,
    database: str,
    port: int | None = None,
    query: dict | None = None,
) -> URL:
    if not query:
        query = {}
    dialect_defaults = {
        SqlAlchemyDialect.MSSQL: {
            "port": 1433,
            "query": dict(driver="ODBC Driver 17 for SQL Server"),
        },
        SqlAlchemyDialect.POSTGRESQL: {
            "port": 5432,
            # "query": None,
        },
        SqlAlchemyDialect.SQLITE: {
            "port": None,
            # "query": None,
        },
        SqlAlchemyDialect.MYSQL: {
            "port": 3306,
            # "query": None,
        },
        SqlAlchemyDialect.ORACLE: {
            "port": 1521,
            # "query": None,
        },
    }
    if not port:
        port = dialect_defaults[dialect]["port"]
    if not query:
        query = dialect_defaults[dialect]["query"]

    url_object = URL.create(
        drivername=dialect,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
        query=query,
    )
    return url_object
