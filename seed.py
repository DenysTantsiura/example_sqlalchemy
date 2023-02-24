from collections import Counter
from datetime import (
    datetime, 
    timedelta,
    )
import logging
from random import randint

from faker import Faker
from faker.providers import DynamicProvider

# export PYTHONPATH="${PYTHONPATH}:/1prj/example_sqlalchemy/"
from database.connect_to_db_postgresql import session
from database.models import (
    Group,
    Student,
    Teacher,
    Subject,
    Assessment
    )
from exception_catcher import exeption_catcher


NUMBER_OF_GROUPS = 3
NUMBER_OF_STUDENTS = randint(30, 50)
NUMBER_OF_TEACHERS = randint(3, 5)
NUMBER_OF_SUBJECTS = randint(5, 8)
NUMBER_OF_ASSESSMENTS = 19 * NUMBER_OF_SUBJECTS * NUMBER_OF_STUDENTS  # randint(1, 19)
YEAR_STUDY_START = 2022

logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')


@exeption_catcher(1)
def create_groups() -> bool:
    """Create fake groups."""
    fake_groups = [f'Group-{number}' for number in range(1, NUMBER_OF_GROUPS + 1)]
    [session.add(Group(group_name=group)) for group in fake_groups]
    session.commit()

    return True


@exeption_catcher(2)
def create_students() -> bool:
    """Create fake students."""
    fake_data = Faker('uk_UA')

    fake_students = [fake_data.name() for _ in range(NUMBER_OF_STUDENTS)]
    for_students = [(student, randint(1, NUMBER_OF_GROUPS)) for student in fake_students]
    [session.add(Student(name=name, group_id=id)) for name, id in for_students]
    session.commit()

    return True


@exeption_catcher(3)
def create_teachers() -> bool:
    """Create fake teachers."""
    fake_data = Faker('uk_UA')

    fake_teachers = [fake_data.name() for _ in range(NUMBER_OF_TEACHERS)]
    [session.add(Teacher(name=name)) for name in fake_teachers]
    session.commit()

    return True


@exeption_catcher(4)
def create_subjects() -> bool:
    """Create fake subjects."""
    fake_data = Faker('uk_UA')

    try:
        number_of_subjects = len(session.query(Subject).all()) or NUMBER_OF_SUBJECTS

    except Exception:
        number_of_subjects = NUMBER_OF_SUBJECTS

    try:
        number_of_teachers = len(session.query(Teacher).all()) or NUMBER_OF_TEACHERS

    except Exception:
        number_of_teachers = NUMBER_OF_TEACHERS

    fake_subjects = [fake_data.job() for _ in range(number_of_subjects)]
    for_subjects = [(subject, randint(1, number_of_teachers)) for subject in fake_subjects]
    [session.add(Subject(subject=subject, teacher_id=id)) for subject, id in for_subjects]
    session.commit()

    return True


@exeption_catcher(5)
def create_assessments() -> bool:
    """Create fake assessments."""
    fake_data = Faker('uk_UA')

    try:
        number_of_subjects = len(session.query(Subject).all()) or NUMBER_OF_SUBJECTS

    except Exception:
        number_of_subjects = NUMBER_OF_SUBJECTS

    try:
        number_of_students = len(session.query(Student).all()) or NUMBER_OF_STUDENTS

    except Exception:
        number_of_students = NUMBER_OF_STUDENTS

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

    return True


def random_study_day() -> datetime.date:
    start_date = datetime.strptime(f'{YEAR_STUDY_START}-09-01', '%Y-%m-%d')
    end_date = datetime.strptime(f'{YEAR_STUDY_START+1}-06-15', '%Y-%m-%d')

    current_date = start_date + timedelta(randint(1, (end_date - start_date).days - 9))  # 9=Saturday Sunday + last week

    while current_date.isoweekday() in (6, 7):  # Saturday Sunday
        current_date += timedelta(1)
    
    return current_date


if __name__ == '__main__':
    create_groups()
    create_students()
    create_teachers()
    create_subjects()
    create_assessments()
