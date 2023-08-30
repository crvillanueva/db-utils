import json
from dataclasses import asdict
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy import MetaData
from sqlalchemy import Table as SqlTable
from sqlalchemy import create_engine, inspect, select

from db_utils.cli_utils import typer_error_msg_to_stdout
from db_utils.enums import OutputFormat
from db_utils.inspect.table.main import get_table_schema_object
from db_utils.utils import autocomplete_tables

app = typer.Typer()

# for table pretty output
console = Console()
rich_table = Table(show_header=True, header_style="bold magenta")


@app.command("schema", help="Inspect table schema.")
def inspect_table_columns(
    ctx: typer.Context,
    table_name: Annotated[
        str, typer.Argument(..., help="Table name", autocompletion=autocomplete_tables)
    ],
    db_schema: Annotated[Optional[str], typer.Option(..., "--schema", "-s")] = None,
    extra: Annotated[bool, typer.Option(..., "--extra", "-e")] = False,
    output: Annotated[
        OutputFormat, typer.Option(..., "--output", "-o")
    ] = OutputFormat.TSV,
):
    """
    Inspect the schema for a table.
    """
    db_url = ctx.obj.db_url
    engine = create_engine(db_url)

    if "." in table_name:
        db_schema, table_name = table_name.split(".")
    if not db_schema:
        typer_error_msg_to_stdout("Schema name is required")

    inspector = inspect(engine)
    table_schema = get_table_schema_object(
        inspector, table_name=table_name, db_schema=db_schema, extra=extra
    )
    match output:
        case OutputFormat.TSV:
            max_len_name = max([len(column.name) for column in table_schema.columns])
            for column in table_schema.columns:
                print(f"{column.name:<{max_len_name}}\t{column.type}")
        case OutputFormat.TABLE:
            for column in table_schema.columns:
                rich_table.add_column(column.name, style="dim", no_wrap=True)
            for column in table_schema.columns:
                rich_table.add_row(column.name, column.type, str(column.nullable))
            console.print(rich_table)
        case OutputFormat.JSON:
            json_str = json.dumps(asdict(table_schema))
            print(json_str)
        case _:
            raise ValueError(f"Output format {output} not supported.")


@app.command("sample", help="Get sample rows from table.")
def table_sample(
    ctx: typer.Context,
    table_name: Annotated[
        str, typer.Argument(..., help="Table name", autocompletion=autocomplete_tables)
    ],
    output: Annotated[
        OutputFormat, typer.Option(..., "--output", "-o")
    ] = OutputFormat.TSV,
    schema_name: Annotated[Optional[str], typer.Option(..., "--schema", "-s")] = None,
    limit: Annotated[Optional[int], typer.Option(..., "--size", "-n")] = 5,
):
    db_url = ctx.obj.db_url
    engine = create_engine(db_url)
    if "." in table_name:
        schema_name, table_name = table_name.split(".")
    metadata = MetaData(schema=schema_name)
    table = SqlTable(table_name, metadata, autoload_with=engine)
    with engine.connect() as connection:
        results = connection.execute(select(table).limit(limit)).all()

    results_as_dict: list[dict] = [row._asdict() for row in results]
    column_names = results_as_dict[0].keys()
    match output:
        case OutputFormat.TSV:
            print("\t".join(column_names))
            for row in results:
                print("\t".join([str(x) for x in row]))
        case OutputFormat.TABLE:
            for key in column_names:
                rich_table.add_column(key, style="dim", no_wrap=False)
            for row in results_as_dict:
                results_to_str = [str(x) for x in row.values()]
                rich_table.add_row(*results_to_str)
            console.print(rich_table)
