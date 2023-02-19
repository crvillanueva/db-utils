from dataclasses import dataclass
import json
import os
import subprocess
from enum import Enum
from typing import Dict, Optional, TypedDict

import pyperclip
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url
from sqlalchemy.engine.url import URL
from sqlparse import format as format_sql
from db_utils.config import output_options


class InspectEnum(str, Enum):
    tables = "tables"
    schemas = "schemas"
    views = "views"


app = typer.Typer()


@app.command()
def inspectdb(
    ctx: typer.Context,
    db_object_type: InspectEnum,
    schema_name: str = typer.Option(None, "--schema", "-s"),
):
    """
    Inspect database object.
    """
    db_url = ctx.obj.db_url
    engine = create_engine(db_url)
    inspector = inspect(engine)

    if db_object_type.value == "schemas":
        schema: str
        for schema in inspector.get_schema_names():
            if not schema.startswith("db_") and schema not in [
                "sys",
                "INFORMATION_SCHEMA",
                "guest",
            ]:
                print(schema)

    if db_object_type.value == "tables":
        if schema_name:
            for table in inspector.get_table_names(schema=schema_name):
                print(f"{schema_name}.{table}")
        else:
            for schema in inspector.get_schema_names():
                print(schema)
                for table in inspector.get_table_names(schema=schema):
                    print(f"{schema}.{table}")

    if db_object_type.value == "views":
        for view in inspector.get_view_names(schema=schema_name):
            print(view)


@app.command()
def inspecttable(
    table_name: str = typer.Argument(
        ..., help="Table name", autocompletion=autocomplete_tables
    ),
    schema_name: str = typer.Option(None, "--schema", "-s"),
    output: Optional[str] = typer.Option("tsv", "--output", "-o"),
):
    """
    Inspect database table.
    """
    db_url = state["db_url_string"]
    engine = create_engine(db_url)
    inspector = inspect(engine)
    if "." in table_name:
        schema_name, table_name = table_name.split(".")
    if not schema_name:
        typer_error_msg_to_stdout("Schema name is required")
    if output not in output_options:
        typer_error_msg_to_stdout(f"Output must be one of {''.join(output_options)}")
    column_data = []
    for column in inspector.get_columns(table_name, schema=schema_name):
        column_data.append(
            {
                "Name": column["name"],
                "Type": str(column["type"]),
                "Nullable": str(column["nullable"]),
                # "pk": column["primary_key"],
            }
        )
    if output == "tsv":
        max_len_name = max([len(column["Name"]) for column in column_data])
        for column in column_data:
            print(f"{column['Name']:<{max_len_name}}\t{column['Type']}")
    if output == "table":
        rich_table = Table(show_header=True, header_style="bold magenta")
        for key in column_data[0].keys():
            rich_table.add_column(key, style="dim", no_wrap=True)
        for row in column_data:
            rich_table.add_row(row["Name"], row["Type"], row["Nullable"])
        console.print(rich_table)
