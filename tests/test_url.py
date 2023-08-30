from db_utils.utils import get_db_url_from_env_file


def test_get_db_url_value_from_env_file():
    url = get_db_url_from_env_file("tests/.env")
    # assert url == "postgresql://db-test:12345@localhost:5432/db-test"
