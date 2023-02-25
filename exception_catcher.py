from functools import wraps
import logging
from typing import Callable

from database.connect_to_db_postgresql import session


logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


def exeption_catcher(*param):
    
    def wrapper(func: Callable) -> Callable:
        
        @wraps(func)  # for save name of function
        def wrapped(*args, **kwargs) -> bool:  # Any
            
            try:
                function_result = func(*args, **kwargs)
            
            except Exception as error:  # except Error as error:
                logging.error(f'\t\t\tWrong execute {func.__name__}, error:\n{error}')
                session.rollback()
                return False  # function_result = False
            
            logging.info(f'\t\t\t=== STEP-{param}: {func.__name__} done.')
            # function_result = True
            
            return function_result
                
        return wrapped
    
    return wrapper
