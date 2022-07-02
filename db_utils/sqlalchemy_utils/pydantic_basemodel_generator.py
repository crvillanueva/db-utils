import string
from typing import Container, List, Optional, Type

import stringcase
from pydantic import BaseConfig
from sqlalchemy import Column, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.properties import ColumnProperty

from db_utils.utils import get_stem_word


class OrmConfig(BaseConfig):
    orm_mode = True


def camelCase(st):
    output = "".join(x for x in st.title() if x.isalnum())
    return output[0].lower() + output[1:]


def sqlalchemy_model_to_pydantic_model(
    db_model: Type, *, config: Type = OrmConfig, exclude: Container[str] = []
):  # -> Type[BaseModel]
    table_name: str = db_model.__table__.name
    table_name_singular = get_stem_word(table_name)
    
    string_model_repr = (
        f"class {stringcase.capitalcase(table_name_singular)}In(BaseModelCustom):\n"
    )
    string_model_repr_in = (
        f"""class {stringcase.capitalcase(table_name_singular)}({stringcase.capitalcase(table_name_singular)}In):"""
    )

    mapper = inspect(db_model)
    fields = {}
    n = 0
    for attr in mapper.attrs:
        if isinstance(attr, ColumnProperty):
            if attr.columns:
                try:
                    column_name = attr.key
                    if (n == 0) and (column_name.lower().startswith("id")):
                        column_name = attr.key[:2]
                        continue
                    if column_name in exclude:
                        continue
                    column: Column = attr.columns[0]
                    python_type: Optional[type] = None
                    if hasattr(column.type, "impl"):
                        if hasattr(column.type.impl, "python_type"):
                            python_type = column.type.impl.python_type
                    elif hasattr(column.type, "python_type"):
                        python_type = column.type.python_type
                    assert python_type, f"Could not infer python_type for {column}"
                    default = None
                    if column.default is None and not column.nullable:
                        default = ...
                    fields[column_name] = (python_type, default)

                    string_model_repr += (
                        f"    {column_name}: Optional[{python_type.__name__}] = None\n"
                    )
                except NotImplementedError as e:
                    string_model_repr += (
                        f"    {column_name}: Optional[UNDEFINED] = None\n"
                    )
            n += 1
    string_model_repr += f"""
{string_model_repr_in}
    Id: Union[int, str]\n
"""
    print(string_model_repr)
    return string_model_repr
    
    # pydantic_model = create_model(
    #     db_model.__name__, __config__=config, **fields  # type: ignore
    # )
    # return pydantic_model



def main(engine, schema_name: Optional[str] = None, tables: Optional[List[str]] = None):
    
    metadata = MetaData()
    if not tables:
        metadata.reflect(engine, schema=schema_name)
    else:
        metadata.reflect(engine, schema=schema_name, only=tables)
    # custom mappings
    Base = automap_base(metadata=metadata)
    Base.prepare()

    file_template = f"""from datetime import datetime
from typing import Optional, Union

from pydantic import BaseModel


class BaseModelCustom(BaseModel):
    class Config:
        orm_mode = True
        allow_population_by_field_name = True
        use_enum_values = True
        
"""
    for model in Base.classes:
        file_template += sqlalchemy_model_to_pydantic_model(model)

    with open("schemas_autogen.py", "w") as f:
        f.write(file_template)
