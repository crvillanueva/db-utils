import json
import os
import subprocess
import time
from dataclasses import dataclass
from typing import Annotated, Dict, Optional, TypedDict

import pyperclip
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.engine.url import URL
from sqlparse import format as format_sql

from db_utils.enums import FormatKeyWordOption
from db_utils.url import cli as cli_url

from .autogen import cli as cli_autogen
from .cli_utils import typer_error_msg_to_stdout
from .config import db_metadata_filename, db_url_default_key_name
from .exceptions import NoDBUrlFoundException
from .inspect import cli as cli_inspect
from .run import cli as cli_run
from .utils import (
    get_db_conn_template_from_url,
    get_db_url_from_env_file,
    get_db_url_key_list_from_env_file,
    get_standard_db_url_from_sqla,
)
from .viewgen.trigger_generator import inspect_related_tables


class CLIState(TypedDict, total=False):
    db_url: URL
    db_url_string: str


@dataclass
class State:
    db_url: URL

    @property
    def db_url_string(self) -> str:
        return self.db_url.render_as_string(hide_password=False)


state: CLIState = {}

# SECTION: App definition
app = typer.Typer()

# app.add_typer(cli_autogen.app, name="autogen")
app.add_typer(cli_autogen.app, name="table")
app.add_typer(cli_inspect.app, name="inspect")
app.add_typer(cli_url.app, name="url")
app.add_typer(cli_run.app, name="run")

# for table pretty output
console = Console()
table = Table(show_header=True, header_style="bold magenta")


@app.callback()
def callback(
    ctx: typer.Context,
    silent: bool = typer.Option(False, "--silent", "-s"),
    db_url: Annotated[
        Optional[str], typer.Option(..., "--db-url", "-u", help="Database URL")
    ] = None,
    env_file: Annotated[
        str, typer.Option(..., ".env", help="Path to env-file.")
    ] = ".env",
    env_key_db_url: Annotated[
        str,
        typer.Option(
            ...,
            "--env-key-db-url",
            "-k",
            help="Key name of the database URL.",
            autocompletion=get_db_url_key_list_from_env_file,
        ),
    ] = db_url_default_key_name,
):
    """
    Database utilities with Python
    """
    db_url_obj = None
    if not db_url:
        try:
            db_url_obj = get_db_url_from_env_file(env_file, env_key_db_url)
        except FileNotFoundError:
            if not silent:
                typer.secho(
                    f"No env-file found in current directory {os.getcwd()}",
                    fg=typer.colors.YELLOW,
                )
        except NoDBUrlFoundException:
            if not silent:
                typer.secho(
                    f"No enviromental variable key {db_url_default_key_name!r} in env-file",
                    fg=typer.colors.YELLOW,
                )
    else:
        db_url_obj = make_url(db_url)
    if db_url_obj:
        state["db_url"] = db_url_obj
        db_url_str = db_url_obj.render_as_string(hide_password=False)
        os.environ[db_url_default_key_name] = db_url_str
        state["db_url_string"] = db_url_str
        ctx.obj = State(db_url=db_url_obj)


@app.command()
def format(
    sql_query: Optional[str] = typer.Argument(None),
    comma_first: Annotated[bool, typer.Option("--comma-first", "-c")] = False,
    keyword_case: FormatKeyWordOption = typer.Option(
        FormatKeyWordOption.UPPER, "--keyword-case", "-k"
    ),
    python_output: bool = typer.Option(False, "--python-output", "-p"),
):
    """
    Format SQL query and copy it to clipboard. If not argument is given use clipboard.
    """
    if sql_query is None:
        # read from clipboard if not given as argument
        sql_query = pyperclip.paste()
        if not sql_query:
            typer_error_msg_to_stdout(
                "Not string given as argument and nothing in the clipboard"
            )
    try:
        formated_sql_string = format_sql(
            sql_query,
            reindent=False,
            reindent_aligned=True,
            keyword_case=keyword_case.value,
            comma_first=comma_first,
            output_format="python" if python_output else None,
        )
    except Exception as e:
        typer_error_msg_to_stdout(e)

    pyperclip.copy(formated_sql_string)
    typer.secho(formated_sql_string, bold=True)


@app.command()
def url(
    no_driver: Annotated[bool, typer.Option(..., "--no-driver", "-d")] = False,
    template: Annotated[bool, typer.Option(..., "--template", "-t")] = False,
):
    """
    Show the database URL and copy it to clipboard.
    """
    try:
        db_url = state["db_url"]
    except KeyError:
        typer_error_msg_to_stdout(
            f"No '{db_url_default_key_name}' environmental variable in file or invalid URL"
        )
    if template:
        db_template = get_db_conn_template_from_url(db_url)
        print(db_template)
        pyperclip.copy(db_template)
        return
    # remove driver from url
    if not no_driver:
        db_url_str = f"{db_url.drivername}://{db_url.username}:{db_url.password}@{db_url.host}:{db_url.port}/{db_url.database}"
    else:
        db_url_str = str(db_url)
    typer.secho(f"URL: {db_url_str}")
    pyperclip.copy(db_url_str)
    typer.secho("Database URL copied to clipboard", fg=typer.colors.GREEN, bold=True)


@app.command()
def connect(db_url_string: str = typer.Argument("")):
    """
    Connect to interactive shell using usql.
    """
    if not db_url_string:
        try:
            db_url = state["db_url"]
        except KeyError:
            typer_error_msg_to_stdout(
                f"No '{db_url_default_key_name}' environmental variable in file or invalid URL"
            )
    else:
        db_url = make_url(db_url_string)
    standard_url = get_standard_db_url_from_sqla(db_url)

    cmd = subprocess.run(["usql", standard_url])
    if cmd.check_returncode:
        typer_error_msg_to_stdout("Failed to connect to database")


@app.command()
def time_query(query: str = typer.Argument(...)):
    """
    Time a query in seconds.
    """
    try:
        db_url = state["db_url"]
    except KeyError:
        typer_error_msg_to_stdout(
            f"No '{db_url_default_key_name}' environmental variable in file or invalid URL"
        )

    start_time = time.perf_counter()
    engine = create_engine(db_url)
    with engine.connect() as connection:
        print(f"Timing '{query}'...")
        connection.execute(text(query))
    end_time = time.perf_counter()
    typer.secho(
        f"Query took {end_time - start_time:0.4f} seconds", fg=typer.colors.GREEN
    )


def create_db_metadata_files(
    db_url: str, reflect_views: bool = False, schema: str | None = None
):
    """
    Create a file with the metadata of the database.
    """
    engine = create_engine(db_url)
    inspector = inspect(engine)
    metadata: Dict[str, list] = {}
    for db_schema in inspector.get_schema_names():
        if db_schema:
            if db_schema != schema:
                continue
        metadata[db_schema] = []
        tables = inspector.get_table_names(schema=db_schema)
        if reflect_views:
            tables.extend(inspector.get_view_names(schema=db_schema))
        for table_name in tables:
            metadata[db_schema].append(table_name)
    with open(".db_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)


@app.command()
def create_metatada(
    ctx: typer.Context,
    reflect_views: bool = typer.Option(False),
    schema: str = typer.Option(None),
):
    """
    Create a file with the metadata of the database.
    """
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        transient=False,
    ) as progress:
        progress.add_task("Creating metadata file...", total=100)
        create_db_metadata_files(ctx.obj.db_url_string, reflect_views, schema)
    typer.secho(
        f"Metadata file created at '{os.path.join(os.getcwd(), db_metadata_filename)}'",
        fg=typer.colors.GREEN,
    )


@app.command()
def viewgen(
    table: str = typer.Argument(None),
    schema: str = typer.Option(None, "--schema", "-s"),
):
    """
    Generate a view from a table that considers related tables given their foreign keys.
    """
    inspect_related_tables(state["db_url_string"], table, schema)
