import logging

# export PYTHONPATH="${PYTHONPATH}:/1prj/example_sqlalchemy/"
# from database.connect_to_db_postgresql import session, engine

from my_select import selections
import seed
from seed import (
    NUMBER_OF_GROUPS,
    NUMBER_OF_STUDENTS,
    NUMBER_OF_TEACHERS,
    NUMBER_OF_SUBJECTS,
    NUMBER_OF_ASSESSMENTS,
    )


logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


def main():
    """Create and write data to tables."""
    try:
        seed.create_groups()
        logging.info(f'\t\t\tRecorded {NUMBER_OF_GROUPS} group(s).')
        seed.create_students()
        logging.info(f'\t\t\tRecorded {NUMBER_OF_STUDENTS} student(s).')
        seed.create_teachers()
        logging.info(f'\t\t\tRecorded {NUMBER_OF_TEACHERS} teacher(s).')
        seed.create_subjects()
        logging.info(f'\t\t\tRecorded {NUMBER_OF_SUBJECTS} subject(s).')
        seed.create_assessments()
        logging.info(f'\t\t\tRecorded overall {NUMBER_OF_ASSESSMENTS} assessment(s).')

    except Exception as error:  # except Error as error:
        logging.error(f'Wrong insert groups, error:\n{error}')
        return False


if __name__ == '__main__':
    main()
    selections()


# alembic downgrade base
# alembic revision --autogenerate -m 'Init'
# alembic upgrade head
