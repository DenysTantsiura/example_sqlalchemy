import logging
from sqlite3 import Error
# from time import time
from timeit import default_timer

from sqlalchemy import create_engine


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


# Робота з ORM починається зі створення об'єкта, що інкапсулює доступ до бази даних, 
# в SQLAlchemy він називається engine:
engine = create_engine('sqlite:///:memory:', echo=True)  # використовуємо SQLite базу даних у пам'яті







@duration
def main():
    print(default_timer())


if __name__ == "__main__":
    main()





