__author__ = "Daniil Fajnberg"
__copyright__ = "Copyright © 2022 Daniil Fajnberg"
__license__ = """GNU LGPLv3.0

This file is part of orm2pydantic.

orm2pydantic is free software: you can redistribute it and/or modify it under the terms of
version 3.0 of the GNU Lesser General Public License as published by the Free Software Foundation.

orm2pydantic is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with orm2pydantic. 
If not, see <https://www.gnu.org/licenses/>."""

__doc__ = """
Functions for turning SQLAlchemy objects into corresponding Pydantic objects.
"""

from typing import Container, Type

from pydantic import create_model, BaseConfig, BaseModel, Field
from pydantic.fields import FieldInfo

from sqlalchemy.inspection import inspect
from sqlalchemy.orm import ColumnProperty, RelationshipProperty, Mapper
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.sql.schema import Column, ColumnDefault

from .utils import resolve_dotted_path


__all__ = [
    'field_from_column',
    'field_from_relationship',
    'sqla2pydantic'
]


FieldDef = tuple[type, FieldInfo]


_local_namespace: dict[str, BaseModel] = {}


class OrmConfig(BaseConfig):
    orm_mode = True


def field_from_column(col_prop: ColumnProperty) -> FieldDef:
    """
    Takes a regular field of an SQLAlchemy ORM model and returns a corresponding Pydantic field definition.

    Args:
         col_prop: Instance of `sqlalchemy.orm.ColumnProperty` (i.e. not a relationship field)

    Returns:
        2-tuple with the first element being the Python type of the field and the second being a
        `pydantic.fields.FieldInfo` instance with the correct `default` or `default_factory` parameter.
    """
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
    """
    Takes a relationship field of an SQLAlchemy ORM model and returns a corresponding Pydantic field definition.

    A Many-to-One relationship results in the type of the field being simply the name of the related model class,
    whereas a One-to-Many relationship results in the type being a list parametrized with the name of that class.

    Args:
         rel_prop: Instance of `sqlalchemy.orm.RelationshipProperty` (i.e. not a regular field)

    Returns:
        2-tuple with the first element being the type of the field and the second being a
        `pydantic.fields.FieldInfo` instance with the `default` parameter set to `None`.
    """
    assert isinstance(rel_prop.mapper, Mapper)
    if rel_prop.direction.name == 'MANYTOONE':
        return rel_prop.mapper.class_.__name__, Field(default=None)
    if rel_prop.direction.name == 'ONETOMANY':
        return list[rel_prop.mapper.class_.__name__], Field(default=None)


def sqla2pydantic(db_model: Type[DeclarativeMeta], config: Type[BaseConfig] = OrmConfig, exclude: Container[str] = (),
                  incl_relationships: bool = True, add_fields: dict[str, FieldDef] = None,
                  add_local_ns: dict[str, BaseModel] = None):
    """
    Takes an SQLAlchemy ORM model class and returns a matching Pydantic model class.

    Makes use of the `pydantic.create_model` function.

    Handles default values set on the database model properly, including factory functions.

    Can handle **acyclic** relationships between models by dynamically updating forward references using a local
    namespace of already created Pydantic models.

    Args:
        db_model:
            The SQLAlchemy model; must be an instance of `DeclarativeMeta`
        config (optional):
            The inner model config class passed via the `__config__` parameter to `pydantic.create_model`;
            by default the only explicit setting is `orm_mode = True`.
        exclude (optional):
            A container of strings, each of which represents the name of a field not to create in the Pydantic model;
            by default all fields of the original database model will be converted to Pydantic model fields.
        incl_relationships (optional):
            If set to `False`, fields representing relationships of the database model will not be converted.
            Note that including all relationships may result in circular relationships that Pydantic cannot handle.
            It may be advisable to selectively exclude certain relationship fields to avoid such issues.
            Set to `True` by default.
        add_fields (optional):
            May be passed a dictionary mapping additional field names (not present in the database model) to appropriate
            Pydantic field definitions; those fields will then also be present on the resulting Pydantic model.
        add_local_ns (optional):
            May be passed a dictionary mapping additional Pydantic model names to the corresponding classes;
            these will be passed to the `BaseModel.update_forward_refs` method in addition to those being tracked
            internally anyway.

    Returns:
        Pydantic model class
    """
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
    pydantic_model.update_forward_refs(**_local_namespace if add_local_ns is None else _local_namespace | add_local_ns)
    _local_namespace[name] = pydantic_model
    return pydantic_model
