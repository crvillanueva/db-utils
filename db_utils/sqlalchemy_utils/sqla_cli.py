
from typing import List

import typer
from sqlalchemy import create_engine

from db_utils.exceptions import NoDBUrlFoundException
from db_utils.url import get_db_url_from_env_file

from .pydantic_basemodel_generator import main as pydantic_model_autogen

app = typer.Typer()

@app.callback()
def callback():
    """
    Database utilities for SQLAlchemy
    """


@app.command()
def pydantic_models_autogen(schema: str = typer.Option(None, "--schema", "-s"), tables: List[str] = typer.Option(None, "--table", "-t")):
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
    engine = create_engine(db_url)
    
    pydantic_model_autogen(engine, schema, tables)
