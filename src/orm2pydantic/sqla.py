from typing import Container, Type

from pydantic import create_model, BaseConfig, Field
from pydantic.fields import FieldInfo

from sqlalchemy.inspection import inspect
from sqlalchemy.orm import ColumnProperty, RelationshipProperty, Mapper
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.sql.schema import Column, ColumnDefault

from .utils import resolve_dotted_path


__all__ = [
    'field_from_column',
    'from_sqla'
]


FieldDef = tuple[type, FieldInfo]


_local_namespace = {}


class OrmConfig(BaseConfig):
    orm_mode = True


def field_from_column(column: Column) -> FieldDef:
    try:
        field_type = column.type.impl.python_type
    except AttributeError:
        try:
            field_type = column.type.python_type
        except AttributeError:
            raise AssertionError(f"Could not infer Python type for {column.key}")
    default = ... if column.default is None and not column.nullable else column.default
    if isinstance(default, ColumnDefault):
        if default.is_scalar:
            field_info = Field(default=default.arg)
        else:
            assert callable(default.arg)
            dotted_path = default.arg.__module__ + '.' + default.arg.__name__
            factory = resolve_dotted_path(dotted_path)
            assert callable(factory)
            field_info = Field(default_factory=factory)
    else:
        field_info = Field(default=default)
    return field_type, field_info


def from_sqla(db_model: Type[DeclarativeMeta], incl_many_to_one: bool = True, incl_one_to_many: bool = False,
              config: Type[BaseConfig] = OrmConfig, exclude: Container[str] = (),
              add_fields: dict[str, FieldDef] = None):
    assert isinstance(db_model, DeclarativeMeta)
    assert not (incl_one_to_many and incl_many_to_one)
    fields = {}
    for attr in inspect(db_model).attrs:
        if attr.key in exclude:
            continue
        if isinstance(attr, ColumnProperty):
            assert len(attr.columns) == 1
            column = attr.columns[0]
            fields[attr.key] = field_from_column(column)
        elif isinstance(attr, RelationshipProperty):
            related = attr.mapper
            assert isinstance(related, Mapper)
            if incl_many_to_one and attr.direction.name == 'MANYTOONE':
                fields[attr.key] = (related.class_.__name__, Field(default=None))
            if incl_one_to_many and attr.direction.name == 'ONETOMANY':
                fields[attr.key] = (list[related.class_.__name__], Field(default=None))
        else:
            raise AssertionError("Unknown attr type", attr)
    if add_fields is not None:
        fields |= add_fields
    name = db_model.__name__
    pydantic_model = create_model(name, __config__=config, **fields)
    pydantic_model.__name__ = name
    pydantic_model.update_forward_refs(**_local_namespace)
    _local_namespace[name] = pydantic_model
    return pydantic_model
