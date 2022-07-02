from typing import Optional

import pyperclip
import typer
from sqlparse import format as format_sql

from .cli_utils import typer_error_msg_to_stdout
from .exceptions import NoDBUrlFoundException
from .sqlalchemy_utils import sqla_cli
from .url import get_db_url_from_env_file
from .viewgen.trigger_generator import inspect_related_tables

app = typer.Typer()

app.add_typer(sqla_cli.app, name="sqlalchemy")


@app.callback()
def callback():
    """
    Database utilities with Python
    """


@app.command()
def url(env_file: str = typer.Option(".env", "--env-file")):
    """
    Search the database URL from the given .env file.
    """

    try:
        db_url = get_db_url_from_env_file(env_file)
    except FileNotFoundError as e:
        typer_error_msg_to_stdout(e)
    except NoDBUrlFoundException as e:
        typer_error_msg_to_stdout(e)

    typer.secho(f"URL: {db_url}")
    pyperclip.copy(db_url)

    typer.secho("DB_URL copied to clipboard", fg=typer.colors.GREEN, bold=True)


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
            sql_query, reindent=True, reindent_aligned=False, keyword_case="upper"
        )
    except Exception as e:
        typer_error_msg_to_stdout(e)

    pyperclip.copy(formated_sql_string)
    typer.secho(formated_sql_string, bold=True)


@app.command()
def viewgen(
    table: str = typer.Argument(None),
    schema: str = typer.Option(None, "--schema", "-s"),
):
    """
    Create Pydantic models from SQLAlchemy models.
    """
    try:
        db_url = get_db_url_from_env_file(None)
    except FileNotFoundError as e:
        typer_error_msg_to_stdout(e)
    except NoDBUrlFoundException as e:
        typer_error_msg_to_stdout(e)
    inspect_related_tables(db_url, table, schema)


# if __name__ == "__main__":
#     app()
