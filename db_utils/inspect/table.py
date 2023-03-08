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

app = typer.Typer()

# for table pretty output
console = Console()
table = Table(show_header=True, header_style="bold magenta")


@app.command("columns", help="Inspect table columns.")
def inspect_table_columns(
    ctx: typer.Context,
    table_name: str = typer.Argument(
        ..., help="Table name", autocompletion=autocomplete_tables
    ),
    schema_name: str = typer.Option(None, "--schema", "-s"),
    output: Optional[str] = typer.Option("tsv", "--output", "-o"),
):
    """
    Inspect database table columns.
    """
    db_url = ctx.obj.db_url
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


@app.command("sample", help="Get sample rows from table.")
def table_sample(
    ctx: typer.Context,
    table_name: str = typer.Argument(
        ..., help="Table name", autocompletion=autocomplete_tables
    ),
    schema_name: str = typer.Option(None, "--schema", "-s"),
    output: Optional[str] = typer.Option("tsv", "--output", "-o"),
    limit: int = typer.Option(5, "--size", "-n"),
):
    db_url = ctx.obj.db_url
    engine = create_engine(db_url)
    inspector = inspect(engine)
    if "." in table_name:
        schema_name, table_name = table_name.split(".")
    if not schema_name:
        typer_error_msg_to_stdout("Schema name is required")
    metadata = MetaData(schema=schema_name)
    # breakpoint()
    table = SqlTable(table_name, metadata, autoload_with=engine)
    with engine.connect() as conn:
        query = conn.execute(select(table).limit(limit)).all()
    if output == "tsv":
        for row in query:
            print("\t".join([str(x) for x in row]))
    if output == "table":
        results = query.all()
        results_as_dict: list[dict] = [row._asdict() for row in results]
        column_names = results_as_dict[0].keys()
        rich_table = Table(show_header=True, header_style="bold magenta")
        for key in column_names:
            rich_table.add_column(key, style="dim", no_wrap=False)
        for row in results_as_dict:
            results_to_str = [str(x) for x in row.values()]
            rich_table.add_row(*results_to_str)
        console.print(rich_table)
