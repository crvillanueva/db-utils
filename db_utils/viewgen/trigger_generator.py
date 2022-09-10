from typing import Dict, List, Optional, Set

from jinja2 import FileSystemLoader
from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    MetaData,
    Table,
    create_engine,
    inspect,
    select,
    text,
)
from sqlalchemy.dialects import mssql
from sqlparse import format as format_sql

from db_utils.utils import get_stem_word


def inspect_related_tables(
    db_url: str, table_name: str, schema_name: Optional[str] = None
):

    engine = create_engine(db_url, future=True)

    metadata = MetaData(schema=schema_name)

    table_to_inspect = Table(table_name, metadata, autoload_with=engine)

    columns_to_select: List[Column] = []
    column: Column
    for column in table_to_inspect.columns:
        if column.foreign_keys:
            related_table = list(column.foreign_keys)[0].constraint.referred_table
            related_table_column_name = get_stem_word(related_table.name)
            related_table_column_name_input = input(
                f"Default name for related table '{related_table.name}': '{related_table_column_name}'. Change if wrong: "
            )
            if related_table_column_name_input:
                related_table_column_name = related_table_column_name_input
            column = getattr(related_table.c, related_table_column_name)
        columns_to_select.append(column)

    foreign_keys: Set[ForeignKeyConstraint] = table_to_inspect.foreign_key_constraints

    related_tables_by_columns: Dict[Column, Table] = {}
    for fk in foreign_keys:
        source_column = fk.columns[0]
        referred_table = fk.referred_table
        related_tables_by_columns[source_column] = referred_table

    # related_tables: List[Table] = [
    #     fk.referred_table for fk in foreign_keys
    # ]

    not_foreign_key_columns: List[Column] = [
        column for column in table_to_inspect.columns if not column.foreign_keys
    ]
    primary_key_column = [
        column for column in not_foreign_key_columns if column.primary_key == True
    ][0]

    related_tables_list = list(related_tables_by_columns.values())
    # related_tables_main_columns = get_related_tables_main_columns(related_tables_list)

    select_columns_query = select(columns_to_select)
    # select_columns_query = select(*not_foreign_key_columns, *related_tables_main_columns)

    # create joined query sqlalchemy element
    select_columns_query_joined = select_columns_query
    for source_column, related_table in related_tables_by_columns.items():
        is_left_join = False
        # column_from_original_table: Column = getattr(related_table.c, stem_word(related_table.name))
        if source_column.nullable:
            is_left_join = True
        select_columns_query_joined = select_columns_query_joined.join(
            related_table, isouter=is_left_join
        )

    query_text = str(select_columns_query_joined.compile(dialect=mssql.dialect()))

    # SECTION Create view
    view_name = f"IOV.GV_{table_to_inspect.name}2"
    sql_create_view = format_sql(
        f"CREATE VIEW {view_name} AS {query_text}",
        reindent=True,
        reindent_aligned=False,
        keyword_case="upper",
    )

    with engine.begin() as connection:
        connection.execute(text(sql_create_view))
        print(f"View {view_name} created")
    print(sql_create_view)
    # SECTION Create TRIGGER
    import pypika

    table_pypi = pypika.Table(table_to_inspect.name, schema=schema_name, alias="I")

    template_trigger_update = f"""
CREATE TRIGGER [IOV].[trg_{view_name}_U]
ON {view_name}
INSTEAD OF UPDATE
AS
BEGIN
    UPDATE I
    SET I.IdInfraestructuras=i1.IdInfraestructuras,
        I.IdInstrumentos=i2.IdInstrumentos,
        I.FechaInstalacion=II.FechaInstalacion,
        I.FechaDesinstalacion=II.FechaDesinstalacion,
        I.Comentario=II.Comentario
    # FROM hnv.InstalacionInstrumentos I
    INNER JOIN INSERTED II ON I.IdInstalacionInstrumentos = II.IdInstalacionInstrumentos
    INNER JOIN hnv.Infraestructuras i1 ON I.IdInfraestructuras = i1.IdInfraestructuras
    INNER JOIN hnv.Instrumentos i2 ON I.IdInstrumentos = i2.IdInstrumentos
END
    """

    template_trigger_insert = f"""
CREATE TRIGGER  [IOV].[trg_{view_name}_I]
ON {view_name}
INSTEAD OF INSERT
AS
BEGIN
	INSERT INTO {table_to_inspect.name} (IdInfraestructuras, IdInstrumentos, FechaInstalacion, FechaDesinstalacion, Comentario)
    SELECT i2.IdInfraestructuras ,
        i1.IdInstrumentos ,
        I.FechaInstalacion ,
        I.FechaDesinstalacion ,
        I.Comentario
    FROM INSERTED I
    INNER JOIN hnv.Instrumentos i1 ON I.Instrumento = i1.Instrumento
    INNER JOIN hnv.Infraestructuras i2 ON I.Infraestructura = i2.Infraestructura
END
"""

    template_trigger_delete = f"""
CREATE TRIGGER  [IOV].[trg_{view_name}_D]
ON {view_name}
INSTEAD OF DELETE
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION
            DELETE I FROM {table_to_inspect.name} I
            INNER JOIN
            DELETED D
            ON
            I.{primary_key_column.name} = D.{primary_key_column.name}
        COMMIT TRANSACTION
    END TRY
    BEGIN CATCH
	   Declare @Error Varchar(Max) = ERROR_MESSAGE()
	   PRINT
		  'Error ' + CONVERT(VARCHAR(50), ERROR_NUMBER()) +
		  ', Severity ' + CONVERT(VARCHAR(5), ERROR_SEVERITY()) +
		  ', State ' + CONVERT(VARCHAR(5), ERROR_STATE()) +
		  ', Line ' + CONVERT(VARCHAR(5), ERROR_LINE())

	   PRINT @Error
	   Raiserror(@Error, 16,1)

		IF XACT_STATE() <> 0
		BEGIN
			ROLLBACK TRANSACTION
		END
	END CATCH;
END
    """


def compile_sqlalchemy_object(sqlalchemy_object) -> str:
    return str(sqlalchemy_object.compile(dialect=mssql.dialect()))


def get_joined_table(table_to_join: Table, related_tables: List[Table]) -> Table:
    for related_table in related_tables:
        table_to_join = select(table_to_join).join(related_table)
    return table_to_join


def get_related_tables_main_columns(related_tables: List[Table]):
    main_columns_related_tables = []
    for table in related_tables:
        column = getattr(table.c, get_stem_word(table.name))
        main_columns_related_tables.append(column)
    return main_columns_related_tables


def create_triggers_str(
    table_name: str,
    schema_name: str,
    related_tables_names: List[str],
    columns_not_related_no_pk: List[str],
):
    from jinja2 import Environment, PackageLoader, select_autoescape

    env = Environment(
        loader=FileSystemLoader("templates"),
        # autoescape=select_autoescape()
    )
    template = env.get_template("template_trigger_update")

    view_name = f"IOV.GV_{table_name}"
    data = {
        "table_name": table_name,
        "view_name": view_name,
        "related_table_names": related_tables_names,
        "schema_table": schema_name,
        "columns_not_related_no_pk": columns_not_related_no_pk,
    }
    string_trigger_template = format_sql(
        template.render(**data),
        reindent=True,
        reindent_aligned=False,
        keyword_case="upper",
    )
    final_trigger_template = f"""
CREATE TRIGGER [IOV].[trg_{ table_name }_U]
ON { view_name }
INSTEAD OF UPDATE
AS
BEGIN
    \t{string_trigger_template}
END
    """

    print(final_trigger_template)


#     import pypika
#     from pypika import Query, Tables
#     table_pypi = pypika.Table(table_name, schema=schema_name, alias="I")
#     related_tables = Tables(*related_tables_names)


#     query_trigger = Query.update(table_pypi).set()
#     for table in related_tables:
#         query_trigger.join(table)

#     view_name = f"IOV.GV_{table_name}"
#     template_trigger_update = f"""
# CREATE TRIGGER [IOV].[trg_{view_name}_U]
# ON {view_name}
# INSTEAD OF UPDATE
# AS
# BEGIN
#     UPDATE I
#     SET I.IdInfraestructuras=i1.IdInfraestructuras,
#         I.IdInstrumentos=I.IdInstrumentos,
#         I.FechaInstalacion=II.FechaInstalacion,
#         I.FechaDesinstalacion=II.FechaDesinstalacion,
#         I.Comentario=II.Comentario
#     FROM {schema_name}.{table_name} I
#     INNER JOIN INSERTED II ON I.IdInstalacionInstrumentos = II.IdInstalacionInstrumentos
#     INNER JOIN hnv.Infraestructuras i1 ON I.IdInfraestructuras = i1.IdInfraestructuras
#     INNER JOIN hnv.Instrumentos i2 ON I.IdInstrumentos = i2.IdInstrumentos
# END
#     """

if __name__ == "__main__":
    create_triggers_str(
        "InstalacionInstrumentos",
        "hnv",
        ["Infraestructuras", "Instrumentos"],
        columns_not_related_no_pk=[
            "FechaInstalacion",
            "FechaDesintalacion",
            "Comentarios",
        ],
    )
    # create_triggers_str("InstalacionInstrumentos", "hnv", related_names=["Infraestructuras", "Instrumentos"])
    # inspect_related_tables(
    #     "mssql+pyodbc://adm_hidronvstg@hidronv-stg-dbs:sVQ4Wwwg3wRUJags@hidronv-stg-dbs.database.windows.net:1433/hidronv-stg-db?driver=ODBC+Driver+17+for+SQL+Server",
    #     schema_name="hnv",
    #     table_name="InstalacionInstrumentos",
    # table_name="GV_InstalacionIntrumentos",
    # schema_name="IOV",
    # )
