import argparse
from datetime import datetime
from pprint import pprint

from database.connect_to_db_postgresql import session
from database.models import Student, Teacher, Group, Subject, Assessment
from exception_catcher import exeption_catcher


# prog - назва програми (за замовчуванням: os.path.basename(sys.argv[0]))
parser = argparse.ArgumentParser(
        description = 'Simple example sqlalchemy-alembic.'
    )
# parser.add_argument('-a', '--action', action='store_true', help='Choice of action: create, list, update, remove.')
parser.add_argument('-a', '--action', type=str, help=': Choice of action: create, list, update, remove.')
parser.add_argument('-m', '--model', type=str, help=': Choice of table: Student, Teacher, Group, Subject, Assessment.')
parser.add_argument('-n', '--name', type=str, help=': Choice of NAME.')
parser.add_argument('-id', '--id', type=int, help=': Choice of ID.')  # type=int,
parser.add_argument('-subid', '--subject_id', type=int, help=': Choice of subject_id.')
parser.add_argument('-sid', '--student_id', type=int, help=': Choice of student_id.')
parser.add_argument('-gid', '--group_id', type=int, help=': Choice of group_id.')
parser.add_argument('-tid', '--teacher_id', type=int, help=': Choice of teacher_id.')
parser.add_argument('-aid', '--assessment_id', type=int, help=': Choice of assessment_id.')
parser.add_argument('-ast', '--assessment_value_', type=int, help=': Choice of assessment_value_.')
parser.add_argument('-ad', '--assessment_date_of', type=str, help=': Choice of assessment_date_of (YYYY-MM-DD). Example: 2023-02-25')

# ArgumentParser аналізує аргументи за допомогою методу parse_args()
arguments = parser.parse_args()  # автоматично визначатиме аргументи командного рядка з sys.argv

name = arguments.name 
group_id = arguments.group_id
group_name = arguments.name
subject = arguments.name 
teacher_id = arguments.teacher_id
value_ = arguments.assessment_value_ 
subject_id = arguments.subject_id
student_id = arguments.student_id
assessment_id = arguments.assessment_id
date_of = arguments.assessment_date_of

if not arguments.model:
    arguments.model = 'Group' if group_id else 'Student' if student_id else 'Teacher' if teacher_id else 'Subject' if subject_id else 'Assessment' if assessment_id else False

if arguments.id:
    group_id = arguments.id
    student_id = arguments.id 
    teacher_id = arguments.id
    subject_id = arguments.id
    assessment_id = arguments.id

else:
    arguments.id = group_id or student_id or teacher_id or subject_id or assessment_id


def create_student(name=arguments.name, group_id=arguments.group_id, *args):
    student = Student(name=name, group_id=int(group_id))
    
    return student if student else False


def create_teacher(name=arguments.name, *args):
    teacher = Teacher(name=name)
    
    return teacher if teacher else False


def create_group(group_name=arguments.name, *args):
    group = Group(group_name=group_name)
    
    return group if group else False  # 0


def create_subject(subject=arguments.name, teacher_id=arguments.teacher_id, *args):
    subject = Subject(subject=subject, teacher_id=int(teacher_id))
    
    return subject if subject else False


def create_assessment(value_=arguments.assessment_value_, 
            subject_id=arguments.subject_id,
            student_id=arguments.student_id,
            date_of=arguments.assessment_date_of, *args):
        
    assessment = Assessment(
            value_=value_, 
            subject_id=int(subject_id),
            student_id=int(student_id),
            date_of=datetime.strptime(date_of, '%Y-%m-%d')
            )
     
    return assessment if assessment else False


CREATING = {'Student': create_student,
          'Teacher': create_teacher,
          'Group': create_group,
          'Subject': create_subject,
      'Assessment': create_assessment,
    }


MODELS = {'Student': Student,
          'Teacher': Teacher,
          'Group': Group,
          'Subject': Subject,
        'Assessment': Assessment,
    }


@exeption_catcher(6)
def handler_insert():
    
    new_insert = CREATING[arguments.model]()
    #new_insert = CREATING.get(arguments.model, id)()
    session.add(new_insert) if new_insert else None
    session.commit()


'''
SELECTION = {'Student': select_students,
          'Teacher': select_teachers,
          'Group': select_groups,
          'Subject': select_subjects,
      'Assessment': select_assessments,
    }
'''


@exeption_catcher(7)
def handler_select():
    if not arguments.id:
        query = session.query('*').select_from(MODELS[arguments.model]).all()
    else:
        query = session.query('*').select_from(MODELS[arguments.model]).filter(MODELS[arguments.model].id == arguments.id).all()
    pprint(query)


@exeption_catcher(8)
def handler_update():
    try:

        new_update = CREATING[arguments.model](
                name=name, 
                group_id=group_id,
                group_name=group_name,
                subject=subject, 
                teacher_id=teacher_id,
                value_=value_, 
                subject_id=subject_id,
                student_id=student_id,
                date_of=date_of
                )

        session.add(new_update)
        session.commit()

    except Exception:
        print('No parameters.')


@exeption_catcher(9)
def handler_delete():
    if not arguments.id:
        session.query(MODELS[arguments.model]).delete()

    else:
        object_to_delete = session.query(MODELS[arguments.model]).get(arguments.id)
        session.delete(object_to_delete)
    session.commit()


ACTIONS = {'create': handler_insert,
          'list': handler_select,
          'update': handler_update,
          'remove': handler_delete,
    }


if __name__ == '__main__':
    if not MODELS.get(arguments.model, None):
        print(f'Incorect Model: {arguments.model}.')
        exit()

    try:
        ACTIONS[arguments.action]()
            
    except KeyError:
        print(f'I cant find this command: {arguments.action}')


'''
python main.py -h
python main.py -a create -m Group -n 'Group-4'
python main.py -a create -m Group
python main.py -a create -m Teacher --name 'Boris Jonson'
python main.py -a create -m Student -n 'Ned Larips' -gid 2
python main.py -a create -m Subject -n 'Development' -tid 1
python main.py -a create -m Assessment -ast 5 -subid 1 -sid 1 -ad 2022-12-12
python main.py -a list -aid 5
python main.py -a list -m Group
python main.py -a list -m Group -id 3
python main.py -a list -m Teacher
python main.py -a list -m Student
python main.py -a list -m Subject
python main.py -a list -m Assessment
python main.py -a remove -m Group -id 11
python main.py -a remove -m Group 
python main.py -a remove -sid 7
python main.py -a 
python main.py -a 
python main.py -a 
python main.py -a 
python main.py -a 
python main.py -a 
python main.py -a 
python main.py -a 
python main.py -a 
'''
