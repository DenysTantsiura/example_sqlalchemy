import logging

from authentication import get_password
from connect_to_db_postgresql import create_connection
from seed import (
    NUMBER_OF_GROUPS,
    NUMBER_OF_STUDENTS,
    NUMBER_OF_TEACHERS,
    NUMBER_OF_SUBJECTS,
    NUMBER_OF_ASSESSMENTS,
    create_all_tables,
    drop_table_if_exists,
    fake_data_generator,
    insert_data_to_db,
    prepare_data_to_insert,
    )


HOST = 'balarama.db.elephantsql.com'
USER = 'scgkgtyo'
DATABASE = 'scgkgtyo'
PASSWORD = get_password()

logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


def main():

    engine, session = create_connection(host=HOST, user=USER, database=DATABASE, password=PASSWORD)
    
    drop_table_if_exists(engine)
    
    if not create_all_tables(engine):
        return False

    if insert_data_to_db(session, prepare_data_to_insert(*fake_data_generator())):
        logging.info(f'Recorded {NUMBER_OF_GROUPS} group(s).')
        logging.info(f'Recorded {NUMBER_OF_STUDENTS} student(s).')
        logging.info(f'Recorded {NUMBER_OF_TEACHERS} teacher(s).')
        logging.info(f'Recorded {NUMBER_OF_SUBJECTS} subject(s).')
        logging.info(f'Recorded overall {NUMBER_OF_ASSESSMENTS} assessment(s).')


if __name__ == '__main__':
    main()
    # sql_requests(sql_script, HOST, USER, DATABASE, PASSWORD)
