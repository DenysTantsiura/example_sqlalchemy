/*
Оцінки студентів у певній групі з певного предмета на останньому занятті.
*/ 
SELECT
  a.value_ AS Assessment, 
  s.name AS Student,
  g.group_name AS Group_,
  sub.subject AS Subject,
  DATE(a.date_of) AS Date_of -- --CONVERT(varchar, a.date_of,2) AS [YYYY-MM.DD]
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
WHERE g.id = 3 AND sub.id = 2 -- Group_ = "Group-2" AND Subject = "History" --AND a.date_of = MAX(a.date_of)
GROUP BY Student, Group_, Subject, Date_of, Assessment  -- bug ? no grouping by Student
ORDER BY Student, Date_of DESC;

