# from collections import Counter
# from datetime import (
#     datetime, 
#     timedelta,
#     )
# import logging
# # from pprint import pprint
# from random import randint

# from faker import Faker
# from faker.providers import DynamicProvider
from sqlalchemy import (
    CHAR,
    Column,
    DATE,
    # Engine,
    ForeignKey,
    Integer, 
    NUMERIC,
    TIMESTAMP, 
    VARCHAR,
    )
# from sqlalchemy.ext.declarative import declarative_base  # in v.1.4 sqlalchemy
from sqlalchemy.orm import (
    declarative_base,
    relationship, 
    # Session,
    )
from sqlalchemy.sql import func  # select


"""У якості об'єкту, що сполучає стан бази та опис бази, в Python коді 
виступає Base, саме цей клас відповідає за "магію" синхронізації 
таблиць бази даних та їх описи в Python класах Person та Address."""
Base = declarative_base()


class Group(Base):
    __tablename__ = 'groups_'
    id = Column(Integer, primary_key=True)  # id - ім'я тут працювати, а Column('ім'я у БД', Integer, ...
    group_name = Column(CHAR(7), unique=True, nullable=False)  # , convert_unicode=True
    created_at = Column(TIMESTAMP(timezone=False), server_default=func.current_timestamp())
    # datetime.now() -> час коли запустився сервер (якщо не відкладений виклик - колбек), 
    # func.current_timestamp() -> коли створився запис.


class Student(Base):
    __tablename__ = 'students'
    id = Column(Integer, primary_key=True)
    name = Column(VARCHAR(50), unique=True, nullable=False)  # , convert_unicode=True
    # for SQL:
    group_id = Column(Integer, ForeignKey('groups_.id', onupdate='CASCADE', ondelete='SET NULL'))
    created_at = Column(TIMESTAMP(timezone=False), server_default=func.current_timestamp())
    # for SQLAlchemy, for usable query joins:
    group = relationship(Group)


class Teacher(Base):
    __tablename__ = 'teachers'
    id = Column(Integer, primary_key=True)
    name = Column(VARCHAR(50), unique=True, nullable=False)  # , convert_unicode=True
    created_at = Column(TIMESTAMP(timezone=False), server_default=func.current_timestamp())


class Subject(Base):
    __tablename__ = 'subjects'
    id = Column(Integer, primary_key=True)
    # One subject is taught by several teachers => unique=False: 
    subject = Column(CHAR(40), nullable=False)  # unique=True, , convert_unicode=True
    # for SQL:
    teacher_id = Column(Integer, ForeignKey('teachers.id', onupdate='CASCADE', ondelete='SET NULL'))
    created_at = Column(TIMESTAMP(timezone=False), server_default=func.current_timestamp())
    # for SQLAlchemy, for usable query joins:
    teacher = relationship(Teacher)

# records = relationship("Record", cascade="all, delete", backref="note")
# tags = relationship("Tag", secondary=note_m2m_tag, backref="notes", passive_deletes=True)
# backref===back_populates ->  вказати назву поля яке буде в класі "Tag"


# Tаблиця i! для зв'язку (many-to-many) між таблицями subjects та students:
class Assessment(Base):
    __tablename__ = 'assessments'
    id = Column(Integer, primary_key=True)
    value_ = Column(NUMERIC)
    date_of = Column(DATE, nullable=False)
    subject_id = Column(Integer, ForeignKey('subjects.id'))  # ForeignKey('subjects.id', ondelete='CASCADE')
    student_id = Column(Integer, ForeignKey('students.id'))
    created_at = Column(TIMESTAMP(timezone=False), server_default=func.current_timestamp())
    subject = relationship(Subject)  # передати можна просто клас можна назву класу (без різниці), for SQLAlchemy
    student = relationship(Student)  # for SQLAlchemy
