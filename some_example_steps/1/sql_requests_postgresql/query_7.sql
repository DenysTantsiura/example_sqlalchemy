/*
Знайти оцінки студентів у окремій групі з певного предмета.
*/
SELECT
  a.value_ AS Assessment, 
  s.name AS Student,
  g.group_name AS Group_,
  sub.subject AS Subject
FROM
  assessments AS a
  JOIN
  subjects AS sub
	  ON a.subject_id = sub.id
  JOIN
  students AS s
	  ON a.student_id = s.id
  JOIN
  groups_ AS g
	  ON s.group_id = g.id
WHERE g.id = 2 AND sub.id = 4 -- Group_ = "Group-2" AND Subject = "History"
ORDER BY Student;
