from typing import Annotated, Optional

import pyperclip
import typer
from sqlalchemy import URL

from db_utils.cli_utils import typer_error_msg_to_stdout
from db_utils.config import db_url_default_key_name
from db_utils.url.enums import SqlAlchemyDialect
from db_utils.url.main import create_url_from_args
from db_utils.utils import get_db_conn_template_from_url

app = typer.Typer(help="Database URL utils")


@app.callback()
def callback():
    """
    Autogenerate models/tables from database.
    """


@app.command()
def show(
    ctx: typer.Context,
    no_driver: bool = typer.Option(False, "--no-driver", "-d"),
    template: bool = typer.Option(False, "--template", "-t"),
):
    """
    Shows the database URL and copy it to clipboard.
    """
    db_url: URL = ctx.obj.db_url
    if not db_url:
        typer_error_msg_to_stdout(
            f"No '{db_url_default_key_name}' environmental variable in file or invalid URL"
        )
        raise typer.Exit(code=1)
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
    typer.secho(db_url_str)
    pyperclip.copy(db_url_str)
    typer.secho("Database URL copied to clipboard", fg=typer.colors.GREEN, bold=True)


@app.command()
def make(
    dialect: Annotated[SqlAlchemyDialect, typer.Option(..., "--dialect", "-d")],
    database: Annotated[str, typer.Option(..., "--database", "-d")],
    username: Annotated[str, typer.Option(..., "--username", "-u")],
    password: Annotated[str, typer.Option(..., "--password", "-p")],
    host: Annotated[str, typer.Option(..., "--host", "-h")],
    port: Annotated[Optional[int], typer.Option(..., "--port", "-P")] = None,
):
    url = create_url_from_args(
        dialect,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    url_str = url.__to_string__(hide_password=False)
    typer.secho(url_str)
    pyperclip.copy(url_str)
