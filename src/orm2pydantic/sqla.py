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
    'field_from_relationship',
    'from_sqla'
]


FieldDef = tuple[type, FieldInfo]


_local_namespace = {}


class OrmConfig(BaseConfig):
    orm_mode = True


def field_from_column(col_prop: ColumnProperty) -> FieldDef:
    assert len(col_prop.columns) == 1
    column: Column = col_prop.columns[0]
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


def field_from_relationship(rel_prop: RelationshipProperty) -> FieldDef:
    assert isinstance(rel_prop.mapper, Mapper)
    if rel_prop.direction.name == 'MANYTOONE':
        return rel_prop.mapper.class_.__name__, Field(default=None)
    if rel_prop.direction.name == 'ONETOMANY':
        return list[rel_prop.mapper.class_.__name__], Field(default=None)


def from_sqla(db_model: Type[DeclarativeMeta], config: Type[BaseConfig] = OrmConfig, exclude: Container[str] = (),
              incl_relationships: bool = True, add_fields: dict[str, FieldDef] = None):
    assert isinstance(db_model, DeclarativeMeta)
    fields = {}
    for attr in inspect(db_model).attrs:
        if attr.key in exclude:
            continue
        if isinstance(attr, ColumnProperty):
            fields[attr.key] = field_from_column(attr)
        elif isinstance(attr, RelationshipProperty):
            if incl_relationships:
                fields[attr.key] = field_from_relationship(attr)
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
