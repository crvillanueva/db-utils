import re
import pathlib
import sqlparse


def get_queries_from_sql_file() -> list[dict]:
    sql_file = pathlib.Path(__file__).parent / "data" / "queries.sql"
    with open(sql_file, "r") as f:
        statements = sqlparse.parse(f.read())
        queries = []
        for st in statements:
            query_info = dict()
            try:
                comments = [t for t in st.tokens if isinstance(t, sqlparse.sql.Comment)]
            except IndexError:
                raise ValueError("No comment found")
            name, _ = re.search(r"name: (\w+) :(\w+)", comments[0].value).groups()
            query_info["name"] = name
            # get :placeholders
            query = "".join(
                [t.value for t in st.tokens if not isinstance(t, sqlparse.sql.Comment)]
            ).strip()
            query_info["query"] = query
            placeholders = set(re.findall(r":(\w+)", query))
            if placeholders:
                query_info["placeholders"] = list(placeholders)
            queries.append(query_info)
    return queries


def test_query_parse():
    pass
