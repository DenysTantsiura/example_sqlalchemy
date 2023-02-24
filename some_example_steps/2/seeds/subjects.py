import logging
from random import randint

from faker import Faker
from faker.providers import DynamicProvider

from database.connect_to_db_postgresql import session
from database.models import Subject, Teacher
from seeds.seedsconfig import (
    NUMBER_OF_SUBJECTS,
    NUMBER_OF_TEACHERS,
    )


logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')
try:
    number_of_subjects = len(session.query(Subject).all()) or NUMBER_OF_SUBJECTS
except Exception:
    number_of_subjects = NUMBER_OF_SUBJECTS
# number_of_subjects = number_of_subjects or NUMBER_OF_SUBJECTS
#logging.info(f'\t\t\t=== STEP: {number_of_subjects=}.')

try:
    number_of_teachers = len(session.query(Teacher).all()) or NUMBER_OF_TEACHERS
except Exception:
    number_of_teachers = NUMBER_OF_TEACHERS
# number_of_teachers = number_of_teachers or NUMBER_OF_TEACHERS
#logging.info(f'\t\t\t=== STEP: {number_of_teachers=}.')

def create_subjects() -> bool:
    """Create fake subjects."""
    fake_data = Faker('uk_UA')
    # fake_subjects = [fake_data.job() for _ in range(number_of_subjects)]
    # for_subjects = [(subject, randint(1, number_of_teachers)) for subject in fake_subjects]
    # [session.add(Subject(subject=subject, teacher_id=id)) for subject, id in for_subjects]
    # session.commit()
    try:
        fake_subjects = [fake_data.job() for _ in range(number_of_subjects)]
        for_subjects = [(subject, randint(1, number_of_teachers)) for subject in fake_subjects]
        [session.add(Subject(subject=subject, teacher_id=id)) for subject, id in for_subjects]
        session.commit()

    except Exception as error:  # except Error as error:
        logging.error(f'\t\t\tWrong insert subjects, error:\n{error}')
        session.rollback()
        return False
    
    logging.info(f'\t\t\t=== STEP: Subjects added.')

    return True


if __name__ == '__main__':
    create_subjects()
