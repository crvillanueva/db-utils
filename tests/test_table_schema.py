from pprint import pprint

from sqlalchemy import Inspector

import pytest
from db_utils.inspect.table.main import get_table_schema_object

tables = ["ACTIVITYREPORT", "ACTIVITYREPORT_PERSON"]


@pytest.mark.parametrize("table", tables)
def test_table_schema_output(test_inspector: Inspector, table: str):
    table_schema = get_table_schema_object(
        test_inspector,
        table,
        "main",
    )
    pprint(table_schema)


@pytest.mark.parametrize("table", tables)
def test_table_schema_output_with_extra(test_inspector: Inspector, table: str):
    table_schema = get_table_schema_object(test_inspector, table, "main", True)
    pprint(table_schema)
