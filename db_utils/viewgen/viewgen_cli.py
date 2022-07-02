
from typing import List

import typer
from sqlalchemy import create_engine

from db_utils.exceptions import NoDBUrlFoundException
from db_utils.url import get_db_url_from_env_file

from .trigger_generator import inspect_related_tables

app = typer.Typer()

@app.callback()
def callback():
    """
    Database utilities for SQLAlchemy
    """


@app.command()
def viewgen(table: str = typer.Argument(None), schema: str = typer.Option(None, "--schema", "-s")):
    """
    Create Pydantic models from SQLAlchemy models.
    """
    try:
        db_url = get_db_url_from_env_file(None)
    except FileNotFoundError as e:
        typer.secho(str(e), fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)
    except NoDBUrlFoundException as e:
        typer.secho(str(e), fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)
    typer.prompt("Press enter to continue...", default=4, show_default=True)
    inspect_related_tables(db_url, table, schema)
