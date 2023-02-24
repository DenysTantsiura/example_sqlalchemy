from collections import Counter
from datetime import (
    datetime, 
    timedelta,
    )
import logging
from random import randint

from faker import Faker
from faker.providers import DynamicProvider

from database.connect_to_db_postgresql import session
from database.models import Assessment, Student, Subject
from seeds.seedsconfig import (
    NUMBER_OF_ASSESSMENTS,
    NUMBER_OF_STUDENTS,
    YEAR_STUDY_START,
    NUMBER_OF_SUBJECTS,
    )


logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')
try:
    number_of_subjects = len(session.query(Subject).all()) or NUMBER_OF_SUBJECTS
except Exception:
    number_of_subjects = NUMBER_OF_SUBJECTS

try:
    number_of_students = len(session.query(Student).all()) or NUMBER_OF_STUDENTS
except Exception:
    number_of_students = NUMBER_OF_STUDENTS


def create_assessments() -> bool:
    """Create fake assessments."""
    fake_data = Faker('uk_UA')
    try:
        assessments_provider = DynamicProvider(
             provider_name='assessments',
             elements=list(range(1, 6)),
        )
        fake_data.add_provider(assessments_provider)	
        fake_assessments = [fake_data.assessments() for _ in range(NUMBER_OF_ASSESSMENTS)]
        # до 20 оцінок у кожного студента з усіх предметів:       
        for_assessments = []
        student_id = 1
        for value in fake_assessments:
                    
            # до 20 оцінок у кожного студента з усіх предметів:
            if Counter(elem[3] for elem in for_assessments).get(student_id, 0) >= randint(6, 19):
                student_id += 1

            if student_id > number_of_students:
                break  # ? why not 'while' whole loop
                
            for_assessments.append((value,
                                    # datetime(2023, 2, randint(1, 28)).date(),
                                    random_study_day(),
                                    randint(1, number_of_subjects),
                                    student_id))

        [session.add(Assessment(value_=p1, date_of=p2, subject_id=p3, student_id=p4)) for p1, p2, p3, p4 in for_assessments]
        session.commit()

    except Exception as error:  # except Error as error:
        logging.error(f'\t\t\tWrong insert assessments, error:\n{error}')
        session.rollback()
        return False
    
   
    logging.info(f'\t\t\t=== STEP: Assesments added.')

    return True


def random_study_day():
    start_date = datetime.strptime(f'{YEAR_STUDY_START}-09-01', '%Y-%m-%d')
    end_date = datetime.strptime(f'{YEAR_STUDY_START+1}-06-15', '%Y-%m-%d')

    current_date = start_date + timedelta(randint(1, (end_date - start_date).days - 9))  # 9=Saturday Sunday + last week

    while current_date.isoweekday() in (6, 7):  # Saturday Sunday
        current_date += timedelta(1)
    
    return current_date


if __name__ == '__main__':
    create_assessments()
