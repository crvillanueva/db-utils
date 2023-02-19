from enum import Enum
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy import MetaData
from sqlalchemy import Table as SqlTable
from sqlalchemy import create_engine, inspect, select

from db_utils.cli_utils import typer_error_msg_to_stdout
from db_utils.config import output_options
from db_utils.utils import autocomplete_tables


class InspectEnum(str, Enum):
    tables = "tables"
    schemas = "schemas"
    views = "views"


class TableEnum(str, Enum):
    columns = "columns"
    sample = "sample"


app = typer.Typer()

# for table pretty output
console = Console()
table = Table(show_header=True, header_style="bold magenta")


@app.command("database")
def inspect_database(
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


@app.command("table")
def inspect_table(
    ctx: typer.Context,
    table_action_type: TableEnum,
    table_name: str = typer.Argument(
        ..., help="Table name", autocompletion=autocomplete_tables
    ),
    schema_name: str = typer.Option(None, "--schema", "-s"),
    output: Optional[str] = typer.Option("tsv", "--output", "-o"),
):
    """
    Inspect database table.
    """
    db_url = ctx.obj.db_url
    engine = create_engine(db_url)
    inspector = inspect(engine)
    if table_action_type == "sample":
        if "." in table_name:
            schema_name, table_name = table_name.split(".")
        if not schema_name:
            typer_error_msg_to_stdout("Schema name is required")
        metadata = MetaData(schema=schema_name)
        breakpoint()
        table = SqlTable(table_name, metadata, autoload_with=engine)
        with engine.connect() as conn:
            query = conn.execute(select(table).limit(10))
        if output == "tsv":
            for row in query:
                print("\t".join([str(x) for x in row]))
        if output == "table":
            results = query.all()
            results_as_dict = [row._asdict() for row in results]
            column_names = results_as_dict[0].keys()
            rich_table = Table(show_header=True, header_style="bold magenta")
            for key in column_names:
                rich_table.add_column(key, style="dim", no_wrap=False)
            for row in results_as_dict:
                results_to_str = [str(x) for x in row.values()]
                rich_table.add_row(*results_to_str)
            console.print(rich_table)

    if table_action_type == "columns":
        if "." in table_name:
            schema_name, table_name = table_name.split(".")
        if not schema_name:
            typer_error_msg_to_stdout("Schema name is required")
        if output not in output_options:
            typer_error_msg_to_stdout(
                f"Output must be one of {''.join(output_options)}"
            )
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
