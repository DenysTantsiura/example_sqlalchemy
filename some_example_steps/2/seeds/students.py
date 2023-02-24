import logging
from random import randint

from faker import Faker

from database.connect_to_db_postgresql import session
from database.models import Student
from seeds.seedsconfig import (
    NUMBER_OF_STUDENTS,
    NUMBER_OF_GROUPS,
    )


logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


def create_students() -> bool:
    """Create fake students."""
    fake_data = Faker('uk_UA')
    # fake_students = [fake_data.name() for _ in range(NUMBER_OF_STUDENTS)]
    # for_students = [(student, randint(1, NUMBER_OF_GROUPS)) for student in fake_students]
    # [session.add(Student(name=name, group_id=id)) for name, id in for_students]
    # session.commit()

    try:
        fake_students = [fake_data.name() for _ in range(NUMBER_OF_STUDENTS)]
        for_students = [(student, randint(1, NUMBER_OF_GROUPS)) for student in fake_students]
        [session.add(Student(name=name, group_id=id)) for name, id in for_students]
        session.commit()

    except Exception as error:  # except Error as error:
        logging.error(f'\t\t\tWrong insert students, error:\n{error}')
        session.rollback()
        return False
    
    logging.info(f'\t\t\t=== STEP: Students added.')

    return True


if __name__ == '__main__':
    create_students()
