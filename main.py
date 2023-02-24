import logging

# from sqlalchemy.sql import text
# from sqlalchemy.orm import joinedload

# export PYTHONPATH="${PYTHONPATH}:/1prj/example_sqlalchemy/"
from database.connect_to_db_postgresql import session, engine

from my_select import selections
import seed
from seed import (
    NUMBER_OF_GROUPS,
    NUMBER_OF_STUDENTS,
    NUMBER_OF_TEACHERS,
    NUMBER_OF_SUBJECTS,
    NUMBER_OF_ASSESSMENTS,
    )
# from drop_tables import drop_table_if_exists


logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


def main():
    """Create and write data to tables."""
    
    # drop_table_if_exists(engine)
    # session.add('DROP TABLE public.teachers CASCADE;')
    # session.commit()
    # session.execute(text('TRUNCATE TABLE public.assessments CASCADE;'))
    # session.execute(text('TRUNCATE TABLE public.subjects CASCADE;'))
    # session.execute(text('TRUNCATE TABLE public.students CASCADE;'))
    # session.execute(text('TRUNCATE TABLE public.teachers CASCADE;'))
    # session.execute(text('TRUNCATE TABLE public.groups_ CASCADE;'))
    # session.commit()

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
