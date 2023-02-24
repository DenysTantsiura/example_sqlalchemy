/*
Знайти середній бал, який ставить певний викладач зі своїх предметів.
*/
SELECT
  ROUND(AVG(a.value_), 1) AS Average, 
  t.name AS Teacher
FROM
  assessments AS a
  JOIN
  subjects AS sub
	  ON a.subject_id = sub.id
  JOIN
  teachers AS t
	  ON sub.teacher_id = t.id
WHERE t.id = 3 -- Teacher = "Соломія Гузій"
GROUP BY Teacher;