import logging

from sqlalchemy import Engine

from database.models import (
        Group,
        Student,
        Teacher,
        Subject,
        Assessment,
        Base,
)


logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


def drop_table_if_exists(engine: Engine) -> None:
    """Drop TABLEs if exists."""
    # https://stackoverflow.com/questions/35918605/how-to-delete-a-table-in-sqlalchemy
    tables_ = [
        Assessment.__table__,
        Subject.__table__,
        Student.__table__,
        Teacher.__table__,
        Group.__table__,
        ]
    
    logging.debug(f'\ntables_:\t\t{tables_}\n')
    # tables= an iterator of sqlalchemy.sql.schema.Table instances:
    # Base.metadata.drop_all(bind=engine, tables=tables_, checkfirst=True)  # checkfirst ...
    try:
        Base.metadata.drop_all(bind=engine, tables=tables_)
        # Base.metadata.create_all(engine) 
        # Base.metadata.bind = engine

    except Exception as error:
        logging.debug(f'\t\tError Droping:\n{error}\n')
        return False
    
    logging.debug(f'=== STEP 2: \nDROP TABLE IF EXISTS ...:')
    [logging.debug(f'{el}') for el in tables_]
