import json
import os
import pathlib
import re
import subprocess
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, TypedDict

import pyperclip
import typer
from prompt_toolkit import prompt
from pyfzf.pyfzf import FzfPrompt
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax
from rich.table import Table
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.engine.url import URL
from sqlparse import format as format_sql

from .autogen import sqla_cli
from .cli_utils import typer_error_msg_to_stdout
from .config import db_metadata_filename, db_url_default_key_name
from .exceptions import NoDBUrlFoundException
from .inspect.main import app as inspect_app
from .url import get_db_url_key_list_from_env_file, get_db_url_value_from_env_file
from .utils import (
    get_standard_db_url_from_sqla,
    get_str_template_db_connection_from_url,
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
        return self.db_url.__to_string__(hide_password=False)


state: CLIState = {}

# SECTION: App definition
app = typer.Typer()

app.add_typer(sqla_cli.app, name="autogen")
app.add_typer(inspect_app, name="inspect")
app.add_typer(sqla_cli.app, name="table")

# for table pretty output
console = Console()
table = Table(show_header=True, header_style="bold magenta")


@app.callback()
def callback(
    ctx: typer.Context,
    env_file: str = typer.Option(".env", help="Path to env-file."),
    db_url_key: str = typer.Option(
        db_url_default_key_name,
        "--db-url-key",
        "-k",
        help="Key name of the database URL in the env-file.",
        autocompletion=get_db_url_key_list_from_env_file,
    ),
):
    """
    Database utilities with Python
    """
    db_url = None

    try:
        db_url = get_db_url_value_from_env_file(env_file, db_url_key)
    except FileNotFoundError:
        typer.secho(
            f"No env-file found in current directory {os.getcwd()}",
            fg=typer.colors.YELLOW,
        )
    except NoDBUrlFoundException:
        typer.secho(
            f"No enviromental variable key '{db_url_default_key_name}' in env-file",
            fg=typer.colors.YELLOW,
        )
    if db_url:
        state["db_url"] = db_url
        os.environ[db_url_default_key_name] = str(db_url)
        state["db_url_string"] = str(db_url)
        ctx.obj = State(db_url=db_url)




class KeyWordOptions(str, Enum):
    UPPER = "upper"
    LOWER = "lower"
    CAPITALIZE = "capitalize"


@app.command()
def format(
    sql_query: Optional[str] = typer.Argument(None),
    comma_first: bool = typer.Option(False, "--comma-first", "-c"),
    keyword_case: KeyWordOptions = typer.Option(
        KeyWordOptions.UPPER, "--keyword-case", "-k"
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
    no_driver: bool = typer.Option(False, "--no-driver", "-d"),
    template: bool = typer.Option(False, "--template", "-t"),
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
        db_template = get_str_template_db_connection_from_url(db_url)
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
            db_url_string = str(state["db_url"])
        except KeyError:
            typer_error_msg_to_stdout(
                f"No '{db_url_default_key_name}' environmental variable in file or invalid URL"
            )
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
    import time

    from sqlalchemy import text

    start_time = time.perf_counter()
    engine = create_engine(db_url)
    with engine.connect() as connection:
        print(f"Timing '{query}'...")
        connection.execute(text(query))
    end_time = time.perf_counter()
    typer.secho(
        f"Query took {end_time - start_time:0.4f} seconds", fg=typer.colors.GREEN
    )


def create_db_metadata_files(db_url: str, reflect_views: bool = False):
    """
    Create a file with the metadata of the database.
    """
    engine = create_engine(db_url)
    inspector = inspect(engine)
    metadata: Dict[str, list] = {}
    for schema in inspector.get_schema_names():
        metadata[schema] = []
        tables = inspector.get_table_names(schema=schema)
        if reflect_views:
            tables.extend(inspector.get_view_names(schema=schema))
        for table_name in tables:
            metadata[schema].append(table_name)
    with open(".db_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)


@app.command()
def create_metatada(ctx: typer.Context, reflect_views: bool = typer.Option(False)):
    """
    Create a file with the metadata of the database.
    """
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        transient=False,
    ) as progress:
        progress.add_task("Creating metadata file...", total=100)
        create_db_metadata_files(ctx.obj.db_url_string, reflect_views)
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


@app.command()
def run():
    """
    Run sql script from file.
    """
    db_url = state["db_url_string"]
    # select file from fzf prompt
    sql_scripts_directory = pathlib.Path("sql/scripts/")
    if not sql_scripts_directory.exists():
        typer_error_msg_to_stdout(f"Directory '{sql_scripts_directory}' does not exist")
    sql_files = [
        path.name for path in pathlib.Path(sql_scripts_directory).glob("*.sql")
    ]
    file = FzfPrompt().prompt(
        sql_files, "--prompt='Select SQL file: ' --reverse --height=50%"
    )
    if not file:
        typer_error_msg_to_stdout("No file selected")

    # obtain text query from file
    with open(sql_scripts_directory / file[0], "r") as f:
        query_str = f.read()

    # find parameters
    # query_params = re.findall(r"\s:(\w+)\b", query_str)

    # find parameters with regex for {} format
    query_params = re.findall(r"\s{(\w+)\b}", query_str)
    # if query params use python prompt toolkit to get input
    query_params_dict = {}
    if query_params:
        syntax = Syntax(query_str, "sql", theme="ansi_dark", line_numbers=True)
        console.print(syntax)

        for param in query_params:
            query_params_dict[param] = prompt(f"{param}: ")  # placeholder="value test")
        print()

        query_str = query_str.format(**query_params_dict)

    formated_sql_string = format_sql(
        query_str, reindent=False, reindent_aligned=True, keyword_case="upper"
    )

    # run query
    db_info_template = get_str_template_db_connection_from_url(state["db_url"], True)
    print("Running query: ")
    syntax = Syntax(formated_sql_string, "sql", theme="dracula")
    console.print(syntax)
    print()
    print("On database: ")
    print(db_info_template)

    try:
        engine = create_engine(db_url)
        results_dicts = []
        with engine.connect() as connection:
            results = connection.execute(text(query_str), **query_params_dict).all()
        for row in results:
            results_dicts.append(dict(row))
    except Exception as e:
        typer_error_msg_to_stdout(f"Database/Query error:\n\n {str(e)}")

    # print rich table
    rich_table = Table(show_header=True, header_style="bold magenta")
    for key in results_dicts[0].keys():
        rich_table.add_column(key, style="dim", no_wrap=True)
    for row in results_dicts:
        row_values = [str(row[key]) for key in row.keys()]
        rich_table.add_row(*row_values)
    console.print(rich_table)
