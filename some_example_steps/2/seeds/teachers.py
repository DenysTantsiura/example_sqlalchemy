import logging

from faker import Faker

from database.connect_to_db_postgresql import session
from database.models import Teacher
from seeds.seedsconfig import NUMBER_OF_TEACHERS


logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


def create_teachers() -> bool:
    """Create fake teachers."""
    fake_data = Faker('uk_UA')
    # fake_teachers = [fake_data.name() for _ in range(NUMBER_OF_TEACHERS)]
    # [session.add(Teacher(name=name)) for name in fake_teachers]
    # session.commit()
    try:
        fake_teachers = [fake_data.name() for _ in range(NUMBER_OF_TEACHERS)]
        [session.add(Teacher(name=name)) for name in fake_teachers]
        session.commit()

    except Exception as error:  # except Error as error:
        logging.error(f'\t\t\tWrong insert teachers, error:\n{error}')
        session.rollback()
        return False
    
    logging.info(f'\t\t\t=== STEP: Teachers added.')

    return True


if __name__ == '__main__':
    create_teachers()
