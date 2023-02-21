/*
Середній бал, який певний викладач ставить певному студентові.
*/
SELECT
  ROUND(AVG(a.value_), 1) AS Average, 
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
WHERE s.id = 9 AND t.id = 3 -- Student = "Роксолана Гук" AND Teacher = "Соломія Гузій"
GROUP BY Student, Teacher; -- s.id
