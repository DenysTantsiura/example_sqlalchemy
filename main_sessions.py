import logging
from sqlite3 import Error
# from time import time
from timeit import default_timer

from sqlalchemy import Text, create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import select


logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


def duration(fun):
    def inner(*args, **kwargs):
        start = default_timer()
        rez = fun(*args, **kwargs)
        logging.info(f'{default_timer()-start=} sec')

        return rez

    return inner


def create_projects_table(conn):
    try:
        c = conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS projects (
                id integer PRIMARY KEY,
                name text NOT NULL,
                begin_date text,
                end_date text
            );"""
        )
    except Error as e:
        print(e)


def create_tasks_table(conn):
    try:
        c = conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS tasks (
                id integer PRIMARY KEY,
                name text NOT NULL,
                priority integer,
                project_id integer NOT NULL,
                status_id integer NOT NULL,
                begin_date text NOT NULL,
                end_date text NOT NULL,
                FOREIGN KEY (project_id) REFERENCES projects (id)
            );"""
        )
    except Error as e:
        print(e)


logging.info(f'=== START_0')
# Робота з ORM починається зі створення об'єкта, що інкапсулює доступ до бази даних, 
# в SQLAlchemy він називається engine:
engine = create_engine('sqlite:///:memory:', echo=True)  # використовуємо SQLite базу даних у пам'яті
# engine = create_engine('sqlite:///sqlalchemy_example.db')
engine = create_engine('postgresql+psycopg2://scott:tiger@localhost/postgresql')
DBSession = sessionmaker(bind=engine)
session = DBSession()
logging.info(f'=== STEP 1: \n{engine}')

# Створення БД за допомогою моделей SQLAlchemy
"""У якості об'єкт, що сполучає стан бази та опис бази, в Python коді 
виступає Base, саме цей клас відповідає за "магію" синхронізації 
таблиць бази даних та їх описи в Python класах Person та Address."""
Base = declarative_base()


class Person(Base):
    __tablename__ = 'persons'
    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)

class Address(Base):
    __tablename__ = 'addresses'
    id = Column(Integer, primary_key=True)
    street_name = Column(String(250))
    street_number = Column(String(250))
    post_code = Column(String(250), nullable=False)
    person_id = Column(Integer, ForeignKey('persons.id'))
    person = relationship(Person)

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer(), primary_key=True)
    name = Column(String(20))
    articles = relationship('Article', back_populates='author')
# Параметр back_populates пов'язує ці відносини між собою.
# Можна будувати двонаправлені запити до таблиць. Альтернатива: backref, 
# достатньо оголосити в одному класі для двосторонніх відносин.
# для 1 до 1 - дод-й параметр uselist:
# userinfo = relationship('UserInfo', backref='user', uselist=False)
class Article(Base):
    __tablename__ = 'articles'
    id = Column(Integer(), primary_key=True)
    title = Column(String(255))
    content = Column(Text())
    user_id = Column(Integer(), ForeignKey('users.id'))
    author = relationship('User', back_populates='articles')


Base.metadata.create_all(engine) 
Base.metadata.bind = engine

# ORM підхід виразніший. Наприклад, додавання нових записів до таблиці — 
# це просто створення нових об'єктів класів Person та Address:
new_person = Person(name="Bill")
session.add(new_person)
session.commit()
# щоб зміни набули чинності та були записані до бази, обов'язково треба виконати 
# commit, після того, як ми додали дані методом add.
new_address = Address(post_code='00000', person=new_person)
session.add(new_address)
session.commit()

# Щоб отримати дані з бази, можна скористатися методом query
for person in session.query(Person).all():
    print(person.name)  # Bill

# Можна отримати доступ до інформації в таблиці статей через атрибут статей користувачів:
users = session.query(User).filter_by(name='Peter Miller').all()
for user in users:
    for article in user.articles:
        print(article.title, user.name)

# Або у зворотний бік:
article = session.query(Article).filter_by(title='Our country’s saddest day').one()
print(article.title, article.author.name)

logging.info(f'=== FINISH')

@duration
def main():
    print(default_timer())


if __name__ == "__main__":
    pass # main()





