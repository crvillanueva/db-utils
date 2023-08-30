from dataclasses import dataclass
from typing import Optional

from sqlalchemy import Inspector


@dataclass
class TableColumn:
    name: str
    type: str
    nullable: bool
    is_pk: Optional[bool] = None
    is_fk: Optional[bool] = None


@dataclass
class FkReference:
    name: str
    columns: list[str]


@dataclass
class TableFk:
    columns: list[str]
    references: FkReference


@dataclass
class TablePk:
    name: Optional[str]
    columns: list[str]


@dataclass
class TableSchema:
    name: str
    columns: list[TableColumn]
    pk: TablePk
    fks: list[TableFk]


def get_table_schema_object(
    inspector: Inspector,
    table_name: str,
    db_schema: str | None = None,
    extra: bool = False,
) -> TableSchema:
    """Get the schema of a database table."""
    if "." in table_name:
        db_schema, table_name = table_name.split(".")
    # PK
    pk_object = inspector.get_pk_constraint(table_name, schema=db_schema)
    table_pk = TablePk(name=pk_object["name"], columns=pk_object["constrained_columns"])

    # FK
    table_fks: list[TableFk] = []
    for fk in inspector.get_foreign_keys(table_name, schema=db_schema):
        fk_ref = FkReference(name=fk["referred_table"], columns=fk["referred_columns"])
        table_fks.append(TableFk(columns=fk["constrained_columns"], references=fk_ref))

    # Columns
    table_columns: list[TableColumn] = []
    for column in inspector.get_columns(table_name, schema=db_schema):
        col = TableColumn(
            name=column["name"], type=str(column["type"]), nullable=column["nullable"]
        )
        if extra:
            col.is_pk = column["name"] in table_pk.columns
            col.is_fk = any(column["name"] in fk.columns for fk in table_fks)
        table_columns.append(col)

    table_schema = TableSchema(
        name=table_name,
        columns=table_columns,
        pk=table_pk,
        fks=table_fks,
    )

    return table_schema


if __name__ == "__main__":
    pass
