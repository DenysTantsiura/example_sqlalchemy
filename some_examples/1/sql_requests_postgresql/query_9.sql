/*
Знайти список курсів, які відвідує студент.
*/
SELECT
  s.id, 
  s.name AS Student,
  sub.subject AS Subject
FROM
  assessments AS a
  JOIN
  subjects AS sub
	  ON a.subject_id = sub.id
  JOIN
  students AS s
	  ON a.student_id = s.id
WHERE s.id = 9 -- Student = "Венедикт Забашта"
GROUP BY Subject, s.id;
