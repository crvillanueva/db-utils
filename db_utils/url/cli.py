from typing import Annotated, Optional

import pyperclip
import typer
from sqlalchemy import URL, make_url

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
    url_str: Optional[str] = typer.Argument(None),
    no_driver: bool = typer.Option(False, "--no-driver", "-d"),
    template: bool = typer.Option(False, "--template", "-t"),
):
    """Shows the database URL and copies it to clipboard."""

    if not url_str:
        db_url: URL = ctx.obj.db_url
    else:
        db_url = make_url(url_str)
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
    """Creates a database URL given parameters and copies it to clipboard."""

    url = create_url_from_args(
        dialect,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    url_str = url.render_as_string(hide_password=False)
    typer.secho(url_str)
    pyperclip.copy(url_str)
