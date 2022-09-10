import os
import subprocess
from enum import Enum
from typing import Optional

import pyperclip
import typer
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url
from sqlparse import format as format_sql

from .cli_utils import typer_error_msg_to_stdout
from .exceptions import NoDBUrlFoundException
from .sqlalchemy_utils import sqla_cli
from .url import get_db_url_value_from_env_file
from .utils import get_standard_db_url_from_sqla
from .viewgen.trigger_generator import inspect_related_tables

app = typer.Typer()

app.add_typer(sqla_cli.app, name="sqlalchemy")

state = {"DB_CONNECTION_URL": ""}


@app.callback()
def callback():
    """
    Database utilities with Python
    """
    db_url = None

    try:
        db_url = get_db_url_value_from_env_file()
    except FileNotFoundError:
        typer.secho("No .env file found", fg=typer.colors.YELLOW)
    except NoDBUrlFoundException:
        typer.secho("No DB_URL env var in file", fg=typer.colors.YELLOW)
    if db_url:
        os.environ["DB_CONNECTION_URL"] = db_url
        state["DB_CONNECTION_URL"] = db_url


@app.command()
def format(sql_query: Optional[str] = typer.Argument(None)):
    """
    Format given SQL query.
    """
    if sql_query is None:
        # Read from clipboard if not given as argument
        sql_query = pyperclip.paste()
        if not sql_query:
            typer_error_msg_to_stdout(
                "Not string given as argument and nothing in the clipboard"
            )
    try:
        formated_sql_string = format_sql(
            sql_query, reindent=False, reindent_aligned=True, keyword_case="upper"
        )
    except Exception as e:
        typer_error_msg_to_stdout(e)

    pyperclip.copy(formated_sql_string)
    typer.secho(formated_sql_string, bold=True)


@app.command()
def url(
    env_file: str = typer.Option(".env", "--env-file"),
    no_driver: bool = typer.Option(False, "--no-driver", "-d"),
):
    """
    Search the database URL from the given .env file.
    """

    try:
        db_url = os.environ["DB_CONNECTION_URL"]
    except KeyError:
        typer_error_msg_to_stdout("No DB_CONNECTION_URL env var in file")
    if no_driver:
        db_url = get_standard_db_url_from_sqla(
            sqla_url=db_url
        )  # remove driver from url

    typer.secho(f"URL: {db_url}")
    pyperclip.copy(db_url)
    typer.secho("DB_URL copied to clipboard", fg=typer.colors.GREEN, bold=True)


@app.command()
def connect(db_url: str = typer.Argument("")):
    """
    Connect to database given URL.
    """
    if not db_url:
        db_url = state["DB_CONNECTION_URL"]
    standard_url = get_standard_db_url_from_sqla(db_url)

    cmd = subprocess.run(["usql", standard_url])
    if cmd.check_returncode:
        typer_error_msg_to_stdout("Failed to connect to database")


class InspectEnum(str, Enum):
    tables = "tables"
    schemas = "schemas"
    views = "views"


@app.command()
def inspectdb(
    db_object_type: InspectEnum,
    schema_name: str = typer.Option(None, "--schema", "-s"),
):
    """
    Inspect database object.
    """
    db_url = state["DB_CONNECTION_URL"]
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
def viewgen(
    table: str = typer.Argument(None),
    schema: str = typer.Option(None, "--schema", "-s"),
):
    """
    Create Pydantic models from SQLAlchemy models.
    """
    inspect_related_tables(state["DB_CONNECTION_URL"], table, schema)
