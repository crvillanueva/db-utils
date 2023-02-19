import os
import re

import sqlalchemy
from dotenv import dotenv_values, find_dotenv
from sqlalchemy.engine import make_url
from sqlalchemy.engine.url import URL

from .exceptions import NoDBUrlFoundException
from .utils import get_fzf_selection_from_python_list


def get_db_url_key_list_from_env_file() -> list:
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
        raise NoDBUrlFoundException(f"No database URLs found in '.env' file.")
    return db_url_keys


def get_db_url_value_from_env_file(
    dotenv_filename: str = ".env",
    db_url_key: str = "DB_CONNECTION_URL",
    interactive: bool = False,
) -> URL | None:
    """Get database URL from key DB_CONNECTION_URL in given .env file."""
    if not os.path.exists(dotenv_filename):
        raise FileNotFoundError(
            f"'{dotenv_filename}' file does not exists in the current directory: '{os.getcwd()}'"
        )
    dotenv_path = find_dotenv(filename=dotenv_filename, usecwd=True)

    config = dotenv_values(dotenv_path=dotenv_path)

    # select all keys that start with DB_ and ends with _URL or _STR from regex expression
    db_url_keys = []
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
            if not db_url_key in db_url_keys:
                raise NoDBUrlFoundException(
                    f"No database URL found in '{dotenv_filename}' file with key '{db_url_key}'."
                )
            db_url = config[db_url_key]
    try:
        url = make_url(db_url)
    except sqlalchemy.exc.ArgumentError:
        raise ValueError(f"Could not parse URL: '{db_url}'")
    return url
