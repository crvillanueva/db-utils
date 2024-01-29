import json
import pathlib
import re
import subprocess
from urllib.parse import quote_plus

import sqlparse
import typer
from pyfzf.pyfzf import FzfPrompt
from rich import print, print_json

from db_utils.run.datatable import TableApp as DataTable
from db_utils.run.schemas import QueryData

app = typer.Typer()


def get_queries_from_sql_file(sql_file: pathlib.Path) -> list[QueryData]:
    queries: list[QueryData] = list()
    with open(sql_file, "r") as f:
        statements = sqlparse.parse(f.read())
        for st in statements:
            try:
                comments = [t for t in st.tokens if isinstance(t, sqlparse.sql.Comment)]
            except IndexError:
                raise ValueError("No comment found")
            comment_match = re.search(r"name: (\w+) :(\w+)", comments[0].value)
            if not comment_match:
                raise ValueError("No query name found")
            name, _ = comment_match.groups()
            query = "".join(
                [t.value for t in st.tokens if not isinstance(t, sqlparse.sql.Comment)]
            ).strip()
            placeholders = list(set(re.findall(r":(\w+)", query)))
            queries.append(QueryData(name=name, query=query, placeholders=placeholders))
    if not queries:
        raise ValueError("No queries found")
    return queries


@app.command()
def query(
    ctx: typer.Context,
    sql_file: str = typer.Argument(),
    output: str = typer.Option(
        "table", "--output", "-o", help="Output format: table, json"
    ),
):
    """
    Run SQL script from file and show results in a table.
    """
    db_url = ctx.obj.db_url

    # select file from fzf prompt
    queries_data = get_queries_from_sql_file(pathlib.Path(sql_file))
    queries_names = [q["name"] for q in queries_data]
    selected_query_name = str(
        FzfPrompt().prompt(
            queries_names, "--prompt='Select SQL query: ' --reverse --height=50%"
        )[0]
    )
    selected_query = [q for q in queries_data if q["name"] == selected_query_name][0]
    if not selected_query_name:
        print("[red]No query selected[/red]")
    # obtain text query from file
    if db_url.drivername == "sqlite":
        if not pathlib.Path(db_url.database).exists():
            raise ValueError(f"Database file {db_url.database} does not exist")
        db_url_str = f"{db_url.drivername}://{db_url.database}"
    else:
        db_url_str = f"{db_url.drivername.split('+')[0]}://{db_url.username}:{quote_plus(db_url.password)}@{db_url.host}:{db_url.port}/{db_url.database}"
    query_str = selected_query["query"]
    # Read json results
    cmd_args = ["usql", "-c", query_str, db_url_str, "--json", "-q"]
    print(f"Running: {' '.join(cmd_args)}")
    run_results = subprocess.run(cmd_args, capture_output=True)
    if run_results.returncode != 0:
        raise ValueError(run_results.stderr.decode())
    query_results = json.loads(run_results.stdout.decode())
    if not query_results:
        print("[red]No results[/red]")
        return
    if output == "json":
        print_json(data=query_results)
        return
    elif output == "table":
        datatable_app = DataTable(query_results)
        datatable_app.run()
    else:
        raise ValueError("Invalid output type")
