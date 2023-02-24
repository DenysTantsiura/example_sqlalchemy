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
DBSession = sessionmaker(bind=engine)
session = DBSession()
logging.info(f'=== STEP 1: \n{engine}')

# Створення БД за допомогою моделей SQLAlchemy
"""У якості об'єкт, що сполучає стан бази та опис бази, в Python коді 
виступає Base, саме цей клас відповідає за "магію" синхронізації 
таблиць бази даних та їх описи в Python класах Person та Address."""
Base = declarative_base()

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

# from rel_one_to_many import User, Article, session

# Create (створення)
"""Щоб використовувати SQLAlchemy для додавання даних до БД, 
нам потрібно тільки створити екземпляр відповідного класу, 
викликати session.add та після session.commit."""

user = User(name='Boris Johnson')
session.add(user)
session.commit()

article = Article(title='Our country’s saddest day', content='Lorem ipsum...', user_id=user.id)
session.add(article)
session.commit()

# Read (читання)
"""Якщо ми знаємо ідентифікатор користувача, ми можемо використовувати метод get 
Метод для отримання значення поля може безпосередньо використовувати властивості класу:"""
user = session.query(User).get(1)
print(user.id, user.name)
# Метод all використовують, щоб отримати всі результати запиту:
users = session.query(User).all()
for user in users:
    print(user.id, user.name)
# Є також методи first, scalar з one. Різниця між трьома:
# first — Повертає перший об'єкт запису, якщо він є.
# one — Запитує всі рядки і викликає виняток, якщо щось повертається, крім одного результату.
# scalar — Повертає перший елемент першого результату, None, якщо результату немає, або помилку, якщо їх більше ніж один результат.

# Метод filter_by використовується для фільтрації по певному полю, або його аналог метод filter трохи з іншим синтаксисом. 
# Давайте відфільтруємо по полю:
user1 = session.query(User).filter_by(name='Boris Johnson').first()
user2 = session.query(User).filter(User.name == 'Boris Johnson').scalar()
print(user1.id, user1.name)
print(user2.id, user2.name)

# Update (оновлення)
# Подібно до доданих даних, додавайте і фіксуйте після оновлення даних.
article = session.query(Article).get(1)
article.content = 'Very important content for the article'
session.add(article)
session.commit()

# Delete (видалення)
# Видалення відбувається напряму — викликом методу delete для отриманого об'єкту:
article = session.query(Article).get(1)
session.delete(article)
session.commit()


logging.info(f'=== FINISH')

@duration
def main():
    print(default_timer())


if __name__ == "__main__":
    pass # main()





