from typing import Optional

from dotenv import dotenv_values, find_dotenv


def main(dotenv_filename: Optional[str] = None):
    if not dotenv_filename:
        dotenv_filename = ".env"
    dotenv_path = find_dotenv(filename=dotenv_filename, raise_error_if_not_found=True)

    config = dotenv_values(dotenv_path=dotenv_path)

    try:
        db_url = config["DB_CONNECTION_URL"]
    except KeyError:
        raise Exception("No 'DB_CONNECTION_URL' key found in '.env' file.")

    print("DB_CONNECTION_URL:", db_url)
    return db_url


# if __name__ == "__main__":
#     main()
