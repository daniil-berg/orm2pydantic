from sqlalchemy.engine.create import create_engine
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.functions import now as db_now
from sqlalchemy.sql.schema import Column, ForeignKey as FKey
from sqlalchemy.sql.sqltypes import Integer, String, TIMESTAMP, Unicode

from orm2pydantic.sqla import sqla2pydantic


ORMBase = declarative_base()


def default_factory() -> str: return '1'


class AbstractBase(ORMBase):
    __abstract__ = True

    date_created = Column(TIMESTAMP(timezone=False), server_default=db_now())
    date_updated = Column(TIMESTAMP(timezone=False), server_default=db_now(), onupdate=db_now())


class StateProvince(AbstractBase):
    __tablename__ = 'state_province'

    id = Column(Integer, primary_key=True)
    country = Column(String(2), nullable=False, index=True)
    name = Column(Unicode(255), nullable=False, index=True)

    cities = relationship('City', backref='state_province', lazy='selectin')


class City(AbstractBase):
    __tablename__ = 'city'

    id = Column(Integer, primary_key=True)
    state_province_id = Column(Integer, FKey('state_province.id', ondelete='RESTRICT'), nullable=False, index=True)
    zip_code = Column(String(5), nullable=False, index=True)
    name = Column(Unicode(255), nullable=False, index=True)

    streets = relationship('Street', backref='city', lazy='selectin')


class Street(AbstractBase):
    __tablename__ = 'street'

    id = Column(Integer, primary_key=True)
    city_id = Column(Integer, FKey('city.id', ondelete='RESTRICT'), nullable=False, index=True)
    name = Column(Unicode(255), nullable=False, index=True)

    addresses = relationship('Address', backref='street', lazy='selectin')


class Address(AbstractBase):
    __tablename__ = 'address'

    id = Column(Integer, primary_key=True)
    street_id = Column(Integer, FKey('street.id', ondelete='RESTRICT'), nullable=False, index=True)
    house_number = Column(String(8), nullable=False, default=default_factory)
    supplement = Column(String(255))


def main_test() -> None:
    engine = create_engine("sqlite://")
    AbstractBase.metadata.create_all(engine)

    _PydanticStateProvince = sqla2pydantic(StateProvince, exclude=['cities'])
    _PydanticCity = sqla2pydantic(City, exclude=['streets'])
    _PydanticStreet = sqla2pydantic(Street, exclude=['addresses'])
    _PydanticAddress = sqla2pydantic(Address)

    with Session(engine) as session:
        bavaria = StateProvince(country="de", name="Bavaria")
        munich = City(zip_code='80333', name="Munich")
        bavaria.cities.append(munich)
        maximilian_street = Street(name="Maximilianstrasse")
        munich.streets.append(maximilian_street)
        some_address = Address()
        maximilian_street.addresses.append(some_address)
        session.add_all([bavaria, munich, maximilian_street, some_address])
        session.commit()

        address = _PydanticAddress.from_orm(some_address)

    assert address.house_number == '1'
    assert address.street.city.state_province.name == "Bavaria"


if __name__ == '__main__':
    main_test()
