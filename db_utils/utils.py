import json
import logging
import pathlib
import subprocess
import tempfile

import typer
from nltk.stem import PorterStemmer
from sqlalchemy import inspect
from sqlalchemy.engine import Connectable
from sqlalchemy.engine.url import URL


def get_stem_word(word: str):
    stemmer = PorterStemmer()
    return stemmer.stem(word, to_lowercase=False)


def get_standard_db_url_from_sqla(url: URL) -> str:
    if not url.drivername:
        return str(url)
    logging.debug(f"{url.get_backend_name()}://{url.username}:{url.password}@{url.host}:{url.port}/{url.database}")
    return f"{url.get_backend_name()}://{url.username}:{url.password}@{url.host}:{url.port}/{url.database}"


def get_str_template_db_connection_from_url(
    db_url: URL, password_hidden: bool | None = False
) -> str:
    """Get template connection string from SQLAlchemy URL object."""
    info_template = ""

    max_key_len = max([len(key) for key in db_url.translate_connect_args().keys()])
    for key, value in db_url.translate_connect_args().items():
        if key == "password" and password_hidden:
            continue
        line = f"{key:<{max_key_len}} = {value}\n"
        info_template += line
    return info_template


def get_fzf_selection_from_python_list(
    selection_list: list, fzf_options: str = "", multi_select: bool = False
) -> str:
    """Get selection from list using fzf."""
    with tempfile.NamedTemporaryFile("w", delete=True) as f:
        for i in selection_list:
            f.write(f"{i}\n")
        f.seek(0)
        list_cat = subprocess.run(["cat", f.name], stdout=subprocess.PIPE)
        # fzf
        fzf_cmd = ["fzf", "--height", "100%", "--layout", "reverse"]
        if multi_select:
            fzf_cmd.append("--multi")

        fzf_output = subprocess.run(
            fzf_cmd, input=list_cat.stdout, stdout=subprocess.PIPE
        )

    return fzf_output.stdout.decode("utf-8").rstrip()


def get_schemas_list(engine: Connectable) -> list:
    inspector = inspect(engine)
    excluded_schemas = ["information_schema", "pg_catalog", "sys", "guest"]
    return [
        schema
        for schema in inspector.get_schema_names()
        if schema not in excluded_schemas
    ]


def autocomplete_tables(ctx: typer.Context) -> list:
    if pathlib.Path(".db_metadata.json").exists():
        with open(".db_metadata.json", "r") as f:
            metadata = json.load(f)
        table_names = []
        for schema, table_list in metadata.items():
            for table in table_list:
                table_names.append(f"{schema}.{table}")
        return table_names
    return []


if __name__ == "__main__":
    print(get_stem_word("workflowitems"))
