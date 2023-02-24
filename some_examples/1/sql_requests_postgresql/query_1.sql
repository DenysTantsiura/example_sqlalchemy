-- Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
SELECT 
    s.id, 
    s.name AS "Student", 
    ROUND(avg(a.value_), 1) AS "Success rate"
FROM 
    assessments a 
    INNER JOIN 
    students s
		ON a.student_id = s.id
GROUP BY s.id
ORDER BY avg(a.value_) DESC 
LIMIT 5;
/* from sqlalchemy import func, desc
SELECT s.id, s.name AS "Student", ROUND(avg(a.value_), 1) AS "Success rate"	->
	session.query(Student.id, Student.name, func.round(func.avg(Assessment.value_), 1).label('Success_rate'))
FROM assessments a	->	select_from(Assessment)  
JOIN students s	->	join(Student)
GROUP BY s.id	->	group_by(Student.id)
ORDER BY avg(a.value_) DESC	->	order_by(desc('Success_rate'))
LIMIT 5	->	limit(5)
====
session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
=====
session.query(Student.id, Student.name, func.round(func.avg(Assessment.value_), 1).label('Success_rate'))\
	.select_from(Assessment).join(Student).group_by(Student.id).order_by(desc('Success_rate')).limit(5).all()

Для запитів оформити окремий файл my_select.py, де будуть 10 функцій 
від select_1 до select_10. Виконання функцій повинно повертати 
результат аналогічний попередньої домашньої роботи. При запитах 
використовуємо механізм сесій SQLAlchemy
  */