import logging
from sqlite3 import Error
# from time import time
from timeit import default_timer

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, ForeignKey
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
logging.info(f'=== STEP 1: \n{engine}')

# Робота SQLAlchemy на core рівні
# потрібно створити спеціальний об'єкт-міст між базою даних та Python кодом. 
# Завдання цього об'єкту синхронізувати базу даних та опис цієї бази в Python об'єктах.
metadata = MetaData()
logging.info(f'=== STEP 2: \n{metadata}')

# Створимо дві таблиці: users, addresses:
users = Table('users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('fullname', String),
)

addresses = Table('addresses', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('email_address', String, nullable=False)
)

# Щоб створити описані вище таблиці у порожній базі даних, 
# можна "попросити" наш metadata об'єкт зробити це:
metadata.create_all(engine)
logging.info(f'=== STEP 3')

# створити нового користувача в базі:
ins = users.insert().values(name='jack', fullname='Jack Jones')
print(str(ins))     # INSERT INTO users (name, fullname) VALUES (:name, :fullname)
print(type(ins))

"""Цей код створює вираз SQL, його можна виконати, коли треба, 
або модифікувати в майбутньому. Щоб виконати, потрібно створити 
з'єднання до бази даних і виконати вираз, використовуючи це з'єднання:"""
# conn = engine.connect()
# result = conn.execute(ins)
# conn.close()

# Для отримання даних із таблиці
with engine.connect() as conn:
    result = conn.execute(ins)
    s = select(users)
    result = conn.execute(s)
    for row in result:
        print('line:')
        print(row)  # (1, u'jack', u'Jack Jones')


logging.info(f'=== FINISH')

@duration
def main():
    print(default_timer())


if __name__ == "__main__":
    pass # main()





