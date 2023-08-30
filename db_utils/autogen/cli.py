import os
import subprocess
from typing import List

import typer
from sqlalchemy import MetaData, Table, create_engine

from db_utils.config import db_url_default_key_name
from db_utils.utils import autocomplete_tables

from .ddl_generator import generate_table_ddl_string
from .pydantic_basemodel_generator import main as pydantic_models_autogen

app = typer.Typer()


@app.callback()
def callback():
    """
    Autogenerate models/tables from database.
    """


@app.command("models")
def autogen_database_models(
    schema: str = typer.Option(None, "--schema", "-s"),
    tables: list[str] = typer.Option(
        None,
        "--tables",
        "-t",
        autocompletion=autocomplete_tables,
        help="Tables to generate models for.",
    ),
    output: str = typer.Option(
        "model", "--output", "-o", help="Generate SQLAlchemy code as table or model"
    ),
    output_filename: str = typer.Option("autogen_models.py", "--output-file", "-f"),
):
    """
    Autogenerate models from database to file.
    """
    db_url = os.environ[db_url_default_key_name]

    cmd = [
        "sqlacodegen",
    ]
    if schema:
        cmd += ["--schema", schema]
        output_filename = f"{schema}_{output_filename}"
    # export db tables as tables and not models
    if output == "table":
        cmd += ["--generator", "tables"]
    if tables:
        for table in tables:
            cmd += ["--tables", table]
        # cmd += ["--tables"]
        # cmd += tables
    cmd += [db_url, "--outfile", output_filename]
    # with Progress(
    #     SpinnerColumn(),
    #     TextColumn("[bold blue]{task.description}"),
    #     transient=False,
    # ) as progress:
    #     progress.add_task(" ".join(cmd), total=100)
    # subprocess.run(cmd, check=True)
    print(" ".join(cmd))
    subprocess.run(cmd, check=True)


@app.command("pydantic")
def autogen_models_pydantic(
    schema: str = typer.Option(None, "--schema", "-s"),
    tables: List[str] = typer.Option(None, "--table", "-t"),
):
    """
    Create Pydantic models from SQLAlchemy models.
    """

    db_url = os.environ[db_url_default_key_name]
    engine = create_engine(db_url)
    pydantic_models_autogen(engine, schema, tables)


@app.command("ddl")
def autogen_ddl_for_table(
    table_names: List[str] = typer.Argument(
        ..., help="Table name", autocompletion=autocomplete_tables
    ),
):
    """Create DDL from table name"""

    db_url = os.environ[db_url_default_key_name]
    engine = create_engine(db_url)
    schema = None
    for table_name in table_names:
        if "." in table_name:
            schema, table_name = table_name.split(".")
        metadata = MetaData(schema=schema)
        table = Table(table_name, metadata, autoload_with=engine)
        ddl_string = generate_table_ddl_string(table, engine)
        print(ddl_string)
