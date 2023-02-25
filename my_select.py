from pprint import pprint

from sqlalchemy import func, desc

from database.connect_to_db_postgresql import session
from database.models import (
    Group,
    Student,
    Subject,
    Teacher,
    Assessment,
    )


def select_1():
    """Знайти 5 студентів із найбільшим середнім балом з усіх предметів."""
    result = (
        session.query(
            Student.id,
            Student.name,
            func.round(func.avg(Assessment.value_), 1).label('Success_rate')
            )
        .select_from(Assessment).join(Student)
        .group_by(Student.id)
        .order_by(desc('Success_rate'))
        .limit(5)
        .all()  # виконання всього запиту
        )

    return result


def select_2():
    """Знайти студента із найвищим середнім балом з певного предмета."""
    result = (
        session.query(
            Student.id.label('ID'), 
            Student.name, 
            func.round(func.avg(Assessment.value_), 1).label('Success_rate'),
            Subject.subject
            )
        .select_from(Assessment)
        .join(Student)
        .join(Subject)
        .filter(Subject.id == 2)
        .group_by(Student.id, Subject.subject, Assessment.subject_id)
        # .group_by(Subject.subject)
        # .group_by(Assessment.subject_id)
        .order_by(desc('Success_rate'), Assessment.subject_id)
        # .order_by(Assessment.subject_id)
        .limit(1)
        .all()
        )

    return result


def select_3():
    """Знайти середній бал у групах з певного предмета."""
    result = (
        session.query(
            Group.id,
            Group.group_name,
            Subject.subject, 
            func.avg(Assessment.value_).label('Average_success_rate')
            )
        .select_from(Assessment)
        .join(Subject)
        .join(Student)
        .join(Group)
        .filter(Subject.id == 2)
        .group_by(Group.id, Subject.subject)
        .all()
        )

    return result


def select_4():
    """Знайти середній бал на потоці (по всій таблиці оцінок)."""
    result = (
        session.query(
            func.round(func.avg(Assessment.value_), 3).label('Whole_success_rate')
            )
        .select_from(Assessment)
        .all()
        )

    return result


def select_5():
    """Знайти які курси читає певний викладач."""
    result = (
        session.query(
            Teacher.id,
            Teacher.name,
            Subject.subject
            )
        .select_from(Subject)
        .join(Teacher)
        .filter(Teacher.id == 5)
        .all()
        )

    return result


def select_6():
    """Знайти список студентів у певній групі."""
    result = (
        session.query(
            Student.id,
            Student.name, 
            Group.group_name
            )
        .select_from(Student)
        .join(Group)
        .filter(Group.id == 2)
        .all()
        )

    return result


def select_7():
    """Знайти оцінки студентів у окремій групі з певного предмета."""
    result = (
        session.query(
            Assessment.value_,
            Student.name, 
            Group.group_name,
            Subject.subject
            )
        .select_from(Assessment)
        .join(Student)
        .join(Group)
        .join(Subject)
        .filter(Subject.id == 4, Group.id == 3)
        .order_by(Student.id)  # not important / doesn't matter
        .all()
        )

    return result


def select_8():
    """Знайти середній бал, який ставить певний викладач зі своїх предметів."""
    result = (
        session.query(
            func.round(func.avg(Assessment.value_), 1).label('Success_rate'),
            Teacher.name
            )
        .select_from(Assessment)
        .join(Subject)
        .join(Teacher)
        .filter(Teacher.id == 3)
        .group_by(Teacher.id)
        .all()
        )

    return result


def select_9():
    """Знайти список курсів, які відвідує певний студент."""
    result = (
        session.query(
            Student.id,
            Student.name, 
            Subject.subject
            )
        .select_from(Assessment)
        .join(Subject)
        .join(Student)
        .filter(Student.id == 9)
        .group_by(Subject.subject, Student.id)
        .all()
        )

    return result


def select_10():
    """Список курсів, які певному студенту читає певний викладач."""
    result = (
        session.query(
            Subject.id,
            Subject.subject,
            Student.name, 
            Teacher.name
            )
        .select_from(Assessment)
        .join(Subject)
        .join(Student)
        .join(Teacher)
        .filter(Student.id == 8, Teacher.id == 2)
        .group_by(Subject.id, Student.id, Teacher.id)
        .all()
        )

    return result


def select_11():
    """Середній бал, який певний викладач ставить певному студентові.
        Якщо не викладає някий предмет групі студента то []."""
    result = (
        session.query(
            func.round(func.avg(Assessment.value_), 1).label('Success_rate'),
            Student.name, 
            Teacher.name
            )
        .select_from(Assessment)
        .join(Subject)
        .join(Student)
        .join(Teacher)
        .filter(Student.id == 9, Teacher.id == 3)
        .group_by(Student.id, Teacher.id)
        .all()
        )

    return result


def select_12():
    """Оцінки студентів у певній групі з певного предмета 
    з останнього заняття (учбового року, а не для кожного студента)."""
    sub_query = (
        session.query(
            Assessment.date_of
            )
        .select_from(Assessment)
        .join(Student)
        .join(Group)
        .filter(Group.id == 3, Assessment.subject_id == 2)
        .order_by(desc(Assessment.date_of))
        .limit(1)
        .all()  # виконання всього запиту
        )
    print(f'\n\n\t\t{sub_query}\n\n')
    result = (
        session.query(
            Assessment.value_,
            Student.name, 
            Group.group_name,
            Subject.subject,
            Assessment.date_of
            )
        .select_from(Assessment)
        .join(Subject)
        .join(Student)
        .join(Group)
        .filter(Group.id == 3, Subject.id == 2, Assessment.date_of == sub_query[0][0])
        .group_by(Student.id, Group.id, Subject.id, Assessment.date_of, Assessment.id)
        .order_by(Student.id, desc(Assessment.date_of))
        .all()  # виконання всього запиту
        )

    return result


def select_13():
    """Оцінки студентів у певній групі з певного предмета 
    з останнього заняття (учбового року, а не для кожного студента).[alternative]."""
    sub_query = (
        session.query(
            Assessment.date_of
            )
        .select_from(Assessment)
        .join(Student)
        .join(Group)
        .filter(Group.id == 3, Assessment.subject_id == 2)
        .order_by(desc(Assessment.date_of))
        .limit(1)
        # .all()  # виконання всього запиту
        )
    print(f'\n\n\t\t{sub_query}\n\n')
    result = (
        session.query(
            Assessment.value_,
            Student.name, 
            Group.group_name,
            Subject.subject,
            Assessment.date_of
            )
        .select_from(Assessment)
        .join(Subject)
        .join(Student)
        .join(Group)
        .filter(Group.id == 3, Subject.id == 2, Assessment.date_of == sub_query)
        .group_by(Student.id, Group.id, Subject.id, Assessment.date_of, Assessment.id)
        .order_by(Student.id, desc(Assessment.date_of))
        .all()  # виконання всього запиту
        )

    return result


'''
def select_14():
    """Оцінки студентів у певній групі з певного предмета ...."""
    result = (
        session.query(
            Assessment.value_,
            Student.name, 
            Group.group_name,
            Subject.subject,
            Assessment.date_of
            )
        .select_from(Assessment)
        .join(Subject)
        .join(Student)
        .join(Group)
        .filter(Group.id == 3, Subject.id == 2)
        .group_by(Student.id, Group.id, Subject.id, Assessment.date_of, Assessment.id)
        #.group_by(Student.id)
        .order_by(Student.id, desc(Assessment.date_of))
        #.order_by(desc(Assessment.date_of))
        .all()
        )

    return result
'''


def selections():
    """Execute many SELECT-ions from all select_№ functions
        and print results."""
    [pprint(globals()[f'select_{i}']()) 
        for i in range(1, 14) if not print(f'''\n\n{globals()[f'select_{i}'].__doc__[:-1]}:\n''')]


if __name__ == '__main__':
    selections()
  
    # pprint(select_12())
    