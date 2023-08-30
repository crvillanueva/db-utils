from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy import create_engine, inspect

from db_utils.inspect.table.cli import app as inspect_table_app

from .enums import InspectDbEnum

app = typer.Typer()

app.add_typer(inspect_table_app, name="table")

# for table pretty output
console = Console()
table = Table(show_header=True, header_style="bold magenta")


@app.callback()
def callback():
    """
    Inspect database and tables.
    """


@app.command("database")
def inspect_database(
    ctx: typer.Context,
    db_object_type: InspectDbEnum,
    schema_name: Annotated[str, typer.Option(..., "--schema", "-s")],
):
    """
    Inspect database objects.
    """
    db_url = ctx.obj.db_url
    engine = create_engine(db_url)
    inspector = inspect(engine)

    if db_object_type.value == "schemas":
        for schema in inspector.get_schema_names():
            if not schema.startswith("db_") and schema.lower() not in [
                "sys",
                "information_schema",
                "guest",
            ]:
                print(schema)

    if db_object_type.value == "tables":
        if schema_name:
            for table in inspector.get_table_names(schema=schema_name):
                print(f"{schema_name}.{table}")
        else:
            for schema in inspector.get_schema_names():
                if schema.startswith("db_") or schema.lower() in [
                    "sys",
                    "information_schema",
                    "guest",
                ]:
                    continue
                for table in inspector.get_table_names(schema=schema):
                    print(f"{schema}.{table}")

    if db_object_type.value == "views":
        if schema_name:
            for view in inspector.get_view_names(schema=schema_name):
                print(f"{schema_name}.{view}")
        else:
            for schema in inspector.get_view_names():
                if schema.startswith("db_") or schema.lower() in [
                    "sys",
                    "information_schema",
                    "guest",
                ]:
                    continue
                for view in inspector.get_view_names(schema=schema):
                    print(f"{schema}.{view}")
