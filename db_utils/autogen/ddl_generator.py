from sqlalchemy import Table
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateTable


def generate_table_ddl_string(table: Table, engine: Engine) -> str:
    """Generate DDL for a table."""
    return CreateTable(table).compile(engine).string
