from functools import wraps
import logging
from typing import Callable  #, Any

from database.connect_to_db_postgresql import session


logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


def exeption_catcher():
    
    def wrapper(func: Callable) -> Callable:
        
        @wraps(func)  # for save name of function
        def wrapped(*args, **kwargs) -> bool:  # Any
            
            try:
                return func(*args, **kwargs)
            
            except Exception as error:  # except Error as error:
                logging.error(f'\t\t\tWrong insert groups, error:\n{error}')
                session.rollback()
                function_result = False
            
            logging.info(f'\t\t\t=== STEP: Groups added.')
            function_result = True
            
            # finally:
            #     total = round(time.time() - start, 3)
            #     print(f'Work duration:\t{total} s.')
                
        return wrapped
    
    return wrapper
