import json
import pathlib
import re
import subprocess
from pprint import pprint

import sqlparse
import typer
from pyfzf.pyfzf import FzfPrompt

from db_utils.run.datatable import TableApp as DataTable

app = typer.Typer()


def get_queries_from_sql_file(sql_file: pathlib.Path) -> list[dict]:
    with open(sql_file, "r") as f:
        statements = sqlparse.parse(f.read())
        queries = dict()
        for st in statements:
            try:
                comments = [t for t in st.tokens if isinstance(t, sqlparse.sql.Comment)]
            except IndexError:
                raise ValueError("No comment found")
            name, _ = re.search(r"name: (\w+) :(\w+)", comments[0].value).groups()
            queries[name] = dict()
            # get :placeholders
            query = "".join(
                [t.value for t in st.tokens if not isinstance(t, sqlparse.sql.Comment)]
            ).strip()
            queries[name]["query"] = query
            placeholders = set(re.findall(r":(\w+)", query))
            if placeholders:
                queries[name]["placeholders"] = list(placeholders)

    return queries


@app.command()
def query(
    ctx: typer.Context,
    sql_file: str = typer.Argument(),
):
    """
    Run SQL script from file and show results in a table.
    """
    db_url = ctx.obj.db_url

    # select file from fzf prompt
    queries_data = get_queries_from_sql_file(pathlib.Path(sql_file))
    names = queries_data.keys()
    query_name = FzfPrompt().prompt(
        names, "--prompt='Select SQL query: ' --reverse --height=50%"
    )[0]
    # obtain text query from file
    if db_url.drivername == "sqlite":
        if not pathlib.Path(db_url.database).exists():
            raise ValueError(f"Database file {db_url.database} does not exist")
        db_url_str = f"{db_url.drivername}://{db_url.database}"
    else:
        db_url_str = f"{db_url.drivername.split('+')[0]}://{db_url.username}:{db_url.password}@{db_url.host}:{db_url.port}/{db_url.database}"
    query_str = queries_data[query_name]["query"]
    # Read json results
    cmd_args = ["usql", "-c", query_str, db_url_str, "--json", "-q"]
    print(f"Running: {' '.join(cmd_args)}")
    run_results = subprocess.run(cmd_args, capture_output=True)
    if run_results.returncode != 0:
        raise ValueError(run_results.stderr.decode())
    query_results = json.loads(run_results.stdout.decode())

    datatable_app = DataTable(query_results)
    datatable_app.run()

    pprint(query_results)
