import os
import subprocess
from typing import List

import typer
from sqlalchemy import create_engine

from db_utils.exceptions import NoDBUrlFoundException
from db_utils.url import get_db_url_value_from_env_file

from .pydantic_basemodel_generator import main as pydantic_model_autogen

app = typer.Typer()


@app.callback()
def callback():
    """
    Database utilities for SQLAlchemy
    """


@app.command()
def autogen_models(
    schema: str = typer.Option(None, "--schema", "-s"),
    tables: bool = typer.Option(None, "--table", "-t"),
):
    """
    Autogenerate models from database to file.
    """
    db_url = os.environ["DB_CONNECTION_URL"]

    cmd = [
        "sqlacodegen",
    ]
    output_filename = "autogen_models.py"
    if schema:
        cmd += ["--schema", schema]
        output_filename = f"{schema}_{output_filename}"
    if tables:
        cmd += ["--generator", "tables"]
    cmd += [db_url, "--outfile", output_filename]

    print("Running command:", " ".join(cmd))

    subprocess.run(cmd)


@app.command()
def autogen_models_pydantic(
    schema: str = typer.Option(None, "--schema", "-s"),
    tables: List[str] = typer.Option(None, "--table", "-t"),
):
    """
    Create Pydantic models from SQLAlchemy models.
    """

    db_url = os.environ["DB_URL_CONNECTION"]

    engine = create_engine(db_url)

    pydantic_model_autogen(engine, schema, tables)
