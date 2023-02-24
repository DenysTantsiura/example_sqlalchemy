import logging
from typing import Union

from sqlalchemy import (
    create_engine, 
    Engine,
    )
from sqlalchemy.orm import (
    Session,
    sessionmaker,
    )


logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


def create_connection(host='localhost', user='postgres', database='postgres', password='567234') \
        -> Union[int, tuple[Engine, Session]]:
    """Create a database connection (session) to a PostgreSQL database (engine)."""
    try:
        # Робота з ORM починається зі створення об'єкта, що інкапсулює доступ до бази даних, 
        # в SQLAlchemy він називається engine: 
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{database}')
        #   dialect[+driver]://user:password@host/dbname[?key=value..]  # ? dbname = postgresql ?
        # сесії, які приховують створення з'єднань з базою та дають можливість виконувати 
        # кілька транзакцій одним запитом для економії ресурсів.
        # створюємо клас DBSession, об'єкти якого є окремими сесіями доступу до бази даних. 
        # Кожна така сесія може зберігати набір транзакцій і виконувати їх тільки коли це дійсно потрібно. 
        # Таке "ледаче" виконання зменшує навантаження на базу та прискорює роботу програми.
        db_session = sessionmaker(bind=engine)
        # Сесія в ORM — це об'єкт, за допомогою якого ви можете керувати, коли саме накопичені 
        # зміни будуть застосовані до бази. Для цього є метод commit. Є методи для додавання 
        # одного або кількох об'єктів до бази (add, add_all).
        session = db_session()
        logging.debug(f'=== STEP 1 - Engine: \n{engine}')
    
    except Exception as error:  # except Error as error:
        logging.error(f'Wrong insert. error:\n{error}')
        return 1

    return engine, session
