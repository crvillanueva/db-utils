import json
import logging
import os
import pathlib
import re
import subprocess
import tempfile
from urllib.parse import quote_plus

import typer
from dotenv import dotenv_values, find_dotenv
from nltk.stem import PorterStemmer
from sqlalchemy import inspect
from sqlalchemy.engine import Connectable, make_url
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import ArgumentError

from .exceptions import NoDBUrlFoundException


def get_stem_word(word: str):
    stemmer = PorterStemmer()
    return stemmer.stem(word, to_lowercase=False)


def get_standard_db_url_from_sqla(url: URL) -> str:
    if not url.drivername:
        return str(url)
    password = str(url.password)
    if "@" in password or "#" in password:
        logging.debug("Encountered special character in password, encoding password")
        password = quote_plus(password)
    standard_url = f"{url.get_backend_name()}://{url.username}:{url.password}@{url.host}:{url.port}/{url.database}"
    logging.debug(standard_url)
    return standard_url


def get_db_conn_template_from_url(
    db_url: URL, password_hidden: bool | None = False
) -> str:
    """Get template connection string from SQLAlchemy URL object"""
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


def get_db_url_key_list_from_env_file() -> list:
    """Checks if .env file exists in current directory and returns all keys
    that start with DB_ and ends with _URL or _STR from regex expression.

    """
    if not os.path.exists(".env"):
        raise FileNotFoundError(
            f"'.env' file does not exists in the current directory: '{os.getcwd()}'"
        )
    dotenv_path = find_dotenv(filename=".env", usecwd=True)

    config = dotenv_values(dotenv_path=dotenv_path)

    # select all keys that start with DB_ and ends with _URL or _STR from regex expression
    db_url_keys = []
    for key, _ in config.items():
        if re.match(r"DB_.*_(URL|STR|STRING)", key):
            db_url_keys.append(key)
    if not db_url_keys:
        raise NoDBUrlFoundException("No database URLs found in '.env' file.")
    return db_url_keys


def get_db_url_from_env_file(
    dotenv_filename: str = ".env",
    db_url_key: str = "DB_CONNECTION_URL",
    interactive: bool = False,
) -> URL:
    """Get database URL from key DB_CONNECTION_URL in given .env file."""
    if not os.path.exists(dotenv_filename):
        raise FileNotFoundError(
            f"'{dotenv_filename}' file does not exists in the current directory: '{os.getcwd()}'"
        )
    dotenv_path = find_dotenv(filename=dotenv_filename, usecwd=True)

    config = dotenv_values(dotenv_path=dotenv_path)

    # select all keys that start with DB_ and ends with _URL or _STR from regex expression
    db_url_keys: list[str] = []
    for key, _ in config.items():
        if re.match(r"DB_.*_(URL|STR|STRING)", key):
            db_url_keys.append(key)
    if not db_url_keys:
        raise NoDBUrlFoundException(
            f"No database URLs found in '{dotenv_filename}' file."
        )
    if len(db_url_keys) == 1:
        db_url = config[db_url_keys[0]]
    else:
        if interactive:
            key_selection = get_fzf_selection_from_python_list(db_url_keys)
            db_url = config[key_selection]
        else:
            if db_url_key not in db_url_keys:
                raise NoDBUrlFoundException(
                    f"No database URL found in '{dotenv_filename}' file with key '{db_url_key}'"
                )
            db_url = config[db_url_key]

    if not db_url:
        raise NoDBUrlFoundException(
            f"No database URL found in '{dotenv_filename}' file with key '{db_url_key}'"
        )
    try:
        url = make_url(db_url)
    except ArgumentError:
        raise ValueError(f"Could not parse URL: '{db_url}'")
    return url


if __name__ == "__main__":
    print(get_stem_word("workflowitems"))
