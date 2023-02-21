from contextlib import contextmanager

from psycopg2 import connect, Error  # DatabaseError


@contextmanager
def create_connection(host='localhost', user='postgres', database='postgres', password='567234'):
    """Create a database connection to a PostgreSQL database."""
    conn = None
    try:
        conn = connect(host=host, user=user, database=database, password=password)
        yield conn
        conn.commit()
        
    except Error as error:
        print(error)
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()
