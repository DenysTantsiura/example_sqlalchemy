import configparser  # for work with *.ini (config.ini)
import logging
import pathlib
from typing import Union

from sqlalchemy import (
    create_engine, 
    Engine,
    )
from sqlalchemy.orm import (
    Session,
    sessionmaker,
    )

from authentication import get_password


CONFIG_FILE = 'config.ini'

logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')

file_config = pathlib.Path(__file__).parent.parent.joinpath(CONFIG_FILE)  # try?
config = configparser.ConfigParser()
config.read(file_config)

user = config.get('DB_DEV', 'user')
password = config.get('DB_DEV', 'password')
password = get_password()
database = config.get('DB_DEV', 'db_name')
host = config.get('DB_DEV', 'host')
# port = config.get('DB_DEV', 'port')

url_to_db = f'postgresql+psycopg2://{user}:{password}@{host}/{database}'  # if try?


def create_connection(*args, **kwargs) -> Union[int, tuple[Engine, Session]]:
    """Create a database connection (session) to a PostgreSQL database (engine)."""
    try:
        # Робота з ORM починається зі створення об'єкта, що інкапсулює доступ до бази даних, 
        # в SQLAlchemy він називається engine,  echo щоб бачити запити БД в консолі, 10- скільки конектів до БД
        engine_ = create_engine(url_to_db, echo=True, pool_size=10)
        # engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
        #   dialect[+driver]://user:password@host/dbname[?key=value..]  # ? dbname = postgresql ?
        # сесії, які приховують створення з'єднань з базою та дають можливість виконувати 
        # кілька транзакцій одним запитом для економії ресурсів.
        # створюємо клас DBSession, об'єкти якого є окремими сесіями доступу до бази даних. 
        # Кожна така сесія може зберігати набір транзакцій і виконувати їх тільки коли це дійсно потрібно. 
        # Таке "ледаче" виконання зменшує навантаження на базу та прискорює роботу програми.
        db_session = sessionmaker(bind=engine_)
        # Сесія в ORM — це об'єкт, за допомогою якого ви можете керувати, коли саме накопичені 
        # зміни будуть застосовані до бази. Для цього є метод commit. Є методи для додавання 
        # одного або кількох об'єктів до бази (add, add_all).
        session_ = db_session()
        logging.debug(f'=== STEP 1 - Engine is Ok: \n{engine_}')
    
    except Exception as error:  # except Error as error:
        logging.error(f'Wrong connect. error:\n{error}')
        return 1

    return engine_, session_


engine, session = create_connection()
