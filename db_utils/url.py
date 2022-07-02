import os
from typing import Optional

from dotenv import dotenv_values, find_dotenv

from .exceptions import NoDBUrlFoundException


def get_db_url_from_env_file(dotenv_filename: Optional[str] = None):
    if not dotenv_filename:
        dotenv_filename = ".env"

    if not os.path.exists(dotenv_filename):
        raise FileNotFoundError(
            f"'{dotenv_filename}' file does not exists in the current directory: '{os.getcwd()}'"
        )
    dotenv_path = find_dotenv(filename=dotenv_filename, usecwd=True)

    config = dotenv_values(dotenv_path=dotenv_path)

    try:
        db_url = config["DB_CONNECTION_URL"]
    except KeyError:
        raise NoDBUrlFoundException(
            f"No 'DB_CONNECTION_URL' key found in '{dotenv_filename}' file."
        )
    return db_url
