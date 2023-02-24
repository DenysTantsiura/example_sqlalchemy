import logging

# export PYTHONPATH="${PYTHONPATH}:/1prj/example_sqlalchemy/"
from database.connect_to_db_postgresql import session
from database.models import Group
from seeds.seedsconfig import NUMBER_OF_GROUPS

# session = connect_to_db_postgresql.session
logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


def create_groups() -> bool:
    """Create fake groups."""
    # fake_groups = [f'Group-{number}' for number in range(1, NUMBER_OF_GROUPS + 1)]
    # [session.add(Group(group_name=group)) for group in fake_groups]
    # session.commit()
    try:
        fake_groups = [f'Group-{number}' for number in range(1, NUMBER_OF_GROUPS + 1)]
        [session.add(Group(group_name=group)) for group in fake_groups]
        session.commit()

    except Exception as error:  # except Error as error:
        logging.error(f'\t\t\tWrong insert groups, error:\n{error}')
        session.rollback()
        return False
    
    logging.info(f'\t\t\t=== STEP: Groups added.')

    return True


if __name__ == '__main__':
    create_groups()
