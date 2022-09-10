import os

from dotenv import dotenv_values, find_dotenv
from sqlalchemy.engine import make_url

from .exceptions import NoDBUrlFoundException


def get_db_url_value_from_env_file(dotenv_filename: str = ".env"):
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
    url = make_url(db_url)

    # connection db credentials template message

    db_info_template = f"""Connecting to database:
    
    DB_HOSTNAME = {url.host}
    DB_USERNAME = {url.username}
    DB_PASSWORD = {url.password}
    DB_NAME = {url.database}\n
    """
    print(db_info_template)

    return db_url
