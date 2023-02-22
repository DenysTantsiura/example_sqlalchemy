from collections import Counter
from datetime import (
    datetime, 
    timedelta,
    )
import logging
# from pprint import pprint
from random import randint
from timeit import default_timer
from typing import Optional

from faker import Faker
from faker.providers import DynamicProvider
from psycopg2 import Error # DatabaseError
from sqlalchemy import (
    create_engine,
    CHAR,
    Column,
    DATE,
    Engine,
    ForeignKey,
    Integer, 
    NUMERIC,
    TIMESTAMP, 
    VARCHAR,
    )
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    declarative_base,
    relationship, 
    Session,
    sessionmaker,
    )
from sqlalchemy.sql import func  # select

from authentication import get_password
from connect_to_db_postgresql import create_connection
# from sql_requests_postgresql import sql_requests, sql_script


NUMBER_OF_GROUPS = 3
NUMBER_OF_STUDENTS = randint(30, 50)
NUMBER_OF_TEACHERS = randint(3, 5)
NUMBER_OF_SUBJECTS = randint(5, 8)
NUMBER_OF_ASSESSMENTS = 19 * NUMBER_OF_SUBJECTS * NUMBER_OF_STUDENTS  # randint(1, 19)
# SQL_CREATED_FILE = './create_tables_postgresql.sql'
YEAR_STUDY_START = 2022
HOST = 'balarama.db.elephantsql.com'
USER = 'scgkgtyo'
DATABASE = 'scgkgtyo'
PASSWORD = get_password()

logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')

"""У якості об'єкту, що сполучає стан бази та опис бази, в Python коді 
виступає Base, саме цей клас відповідає за "магію" синхронізації 
таблиць бази даних та їх описи в Python класах Person та Address."""
Base = declarative_base()


class Group(Base):
    __tablename__ = 'groups_'
    id = Column(Integer, primary_key=True)
    group_name = Column(CHAR(7), unique=True, nullable=False)  # , convert_unicode=True
    created_at = Column(TIMESTAMP(timezone=False), server_default=func.current_timestamp())


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
    subject = Column(CHAR(30), nullable=False)  # unique=True, , convert_unicode=True
    # for SQL:
    teacher_id = Column(Integer, ForeignKey('teachers.id', onupdate='CASCADE', ondelete='SET NULL'))
    created_at = Column(TIMESTAMP(timezone=False), server_default=func.current_timestamp())
    # for SQLAlchemy, for usable query joins:
    teacher = relationship(Teacher)


class Assessment(Base):
    __tablename__ = 'assessments'
    id = Column(Integer, primary_key=True)
    value_ = Column(NUMERIC)
    date_of = Column(DATE, nullable=False)
    subject_id = Column(Integer, ForeignKey('subjects.id'))
    student_id = Column(Integer, ForeignKey('students.id'))
    created_at = Column(TIMESTAMP(timezone=False), server_default=func.current_timestamp())
    subject = relationship(Subject)
    student = relationship(Student)


def create_all_tables(engine: Engine) -> None:
    """Create a tables by Base in engine."""
    try:
        # Щоб створити описані вище таблиці у порожній базі даних, 
        # можна "попросити" наш metadata об'єкт зробити це:
        Base.metadata.create_all(engine) 
        Base.metadata.bind = engine
        # create tables: # Base.metadata.create_all(bind=engine, tables=tables_)
        logging.debug(f'=== STEP 3: tables created.')

    except Exception as error:
        logging.error(f'Error: {error}\nwhen try created tables.')


def drop_table_if_exists(engine: Engine) -> None:
    """Drop TABLEs if exists."""
    # https://stackoverflow.com/questions/35918605/how-to-delete-a-table-in-sqlalchemy
    tables_ = [
        Group.__table__,
        Student.__table__,
        Teacher.__table__,
        Subject.__table__,
        Assessment.__table__,
        ]
    # tables= an iterator of sqlalchemy.sql.schema.Table instances:
    Base.metadata.drop_all(bind=engine, tables=tables_, checkfirst=True)
    logging.debug(f'=== STEP 2: \nDROP TABLE IF EXISTS ...:')
    [logging.debug(f'{el}') for el in tables_]
    # pprint(tables_)


def fake_data_generator() -> tuple:
    """Generate fake data about students assessments."""
    fake_data = Faker('uk_UA')  # uk-UA
    
    fake_groups = [f'Group-{number}' for number in range(1, NUMBER_OF_GROUPS + 1)]
    fake_students = [fake_data.name() for _ in range(NUMBER_OF_STUDENTS)]
    fake_teachers = [fake_data.name() for _ in range(NUMBER_OF_TEACHERS)]
    # fake_subjects = []
    # fake_assessments = []
      
    subjects_provider = DynamicProvider(
             provider_name='subjects',
             elements=['Mathematics', 'Economics', 'Physics', 'History', ],
        )

    # then add new provider to faker instance
    fake_data.add_provider(subjects_provider)
    
    assessments_provider = DynamicProvider(
             provider_name='assessments',
             elements=list(range(1, 6)),
        )

    fake_data.add_provider(assessments_provider)	
        
    fake_assessments = [fake_data.assessments() for _ in range(NUMBER_OF_ASSESSMENTS)]

    fake_subjects = [fake_data.subjects() for _ in range(len(subjects_provider.elements))]
    [fake_subjects.append(fake_data.job())
        for _ in range(NUMBER_OF_SUBJECTS - len(subjects_provider.elements))]

    logging.info(f'Fake data generated.')

    return fake_groups, fake_students, fake_teachers, fake_subjects, fake_assessments


def insert_data_to_db(session: Session, set_of_data: tuple) -> Optional[int]:
    """Insertind data to DataBase."""
    groups, teachers, students, subjects, assessments = set_of_data
    try:
        [session.add(Group(group_name=group)) for group in groups]
        [session.add(Teacher(name=name)) for name in teachers]
        [session.add(Student(name=name, group_id=id)) for name, id in students]
        [session.add(Subject(subject=subject, teacher_id=id)) for subject, id in subjects]
        [session.add(Assessment(value_=p1, date_of=p2, subject_id=p3, student_id=p4)) for p1, p2, p3, p4 in assessments]
    
        session.commit()
        # session.close()  # ?

    except Exception as error:  # except Error as error:
        logging.error(f'Wrong insert. error:\n{error}')
        return 1
        
    logging.info(f'=== STEP 4:DataBase created, data inserted.')


def prepare_data_to_insert(groups: list, students: list, teachers: list, subjects: list, assessments: list) -> tuple:
    """Converting list data to list of tuples."""
    for_groups = [group for group in groups]
    for_teachers = [teacher for teacher in teachers]
    for_students = [(student, randint(1, NUMBER_OF_GROUPS)) for student in students]
    for_subjects = [(subject, randint(1, NUMBER_OF_TEACHERS)) for subject in subjects]
        
    # до 20 оцінок у кожного студента з усіх предметів:       
    for_assessments = []
    student_id = 1
    for value in assessments:
                
        # до 20 оцінок у кожного студента з усіх предметів:
        if Counter(elem[3] for elem in for_assessments).get(student_id, 0) >= randint(6, 19):
            student_id += 1

        if student_id > NUMBER_OF_STUDENTS:
            break
            
        for_assessments.append((value,
                                # datetime(2023, 2, randint(1, 28)).date(),
                                random_study_day(),
                                randint(1, NUMBER_OF_SUBJECTS),
                                student_id))
    
    logging.info(f'Fake data prepared.')

    return for_groups, for_teachers, for_students, for_subjects, for_assessments
 

def random_study_day():
    start_date = datetime.strptime(f'{YEAR_STUDY_START}-09-01', '%Y-%m-%d')
    end_date = datetime.strptime(f'{YEAR_STUDY_START+1}-06-15', '%Y-%m-%d')

    current_date = start_date + timedelta(randint(1, (end_date - start_date).days - 9))  # 9 = Saturday Sunday + last week

    while current_date.isoweekday() in (6, 7):  # Saturday Sunday
        current_date += timedelta(1)
    
    return current_date


def main():
    # engine, session = create_connection()
    # Робота з ORM починається зі створення об'єкта, що інкапсулює доступ до бази даних, 
    # в SQLAlchemy він називається engine:
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{DATABASE}')
        # dialect[+driver]://user:password@host/dbname[?key=value..]  # ? dbname = postgresql ?
    # сесії, які приховують створення з'єднань з базою та дають можливість виконувати 
    # кілька транзакцій одним запитом для економії ресурсів.
    # створюємо клас DBSession, об'єкти якого є окремими сесіями доступу до бази даних. 
    # Кожна така сесія може зберігати набір транзакцій і виконувати їх тільки коли це дійсно потрібно. 
    # Таке "ледаче" виконання зменшує навантаження на базу та прискорює роботу програми.
    DBSession = sessionmaker(bind=engine)
    # Сесія в ORM — це об'єкт, за допомогою якого ви можете керувати, коли саме накопичені 
    # зміни будуть застосовані до бази. Для цього є метод commit. Є методи для додавання 
    # одного або кількох об'єктів до бази (add, add_all).
    session = DBSession()
    logging.debug(f'=== STEP 1 - Engine: \n{engine}')
  
    drop_table_if_exists(engine)
    
    create_all_tables(engine)

    insert_data_to_db(session, prepare_data_to_insert(*fake_data_generator()))
    
    logging.info(f'Recorded {NUMBER_OF_GROUPS} group(s).')
    logging.info(f'Recorded {NUMBER_OF_STUDENTS} student(s).')
    logging.info(f'Recorded {NUMBER_OF_TEACHERS} teacher(s).')
    logging.info(f'Recorded {NUMBER_OF_SUBJECTS} subject(s).')
    logging.info(f'Recorded overall {NUMBER_OF_ASSESSMENTS} assessment(s).')


if __name__ == '__main__':  # !?
    print('step-0')
    main()
    # sql_requests(sql_script, HOST, USER, DATABASE, PASSWORD)
