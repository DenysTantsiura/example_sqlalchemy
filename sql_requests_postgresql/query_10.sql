/*
Список курсів, які певному студенту читає певний викладач.
*/
SELECT
  sub.id, 
  sub.subject AS Subject,
  s.name AS Student,
  t.name AS Teacher
FROM
  assessments AS a
  JOIN
  subjects AS sub
	  ON a.subject_id = sub.id
  JOIN
  students AS s
	  ON a.student_id = s.id
  JOIN
  teachers AS t
	  ON sub.teacher_id = t.id
WHERE s.id = 9 AND t.id = 3 -- Student = "Дмитро Атаманюк" AND Teacher = "Соломія Гузій"
GROUP BY Subject, sub.id, Student, Teacher;
