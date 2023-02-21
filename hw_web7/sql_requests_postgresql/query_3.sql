/*
Знайти середній бал у групах(і) з певного предмета.
*/
SELECT
  g.id, 
  g.group_name AS Group_,
  sub.subject AS Subject,
  AVG(a.value_) AS Average
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
WHERE sub.id = 5 -- Subject = "History"
GROUP BY
  g.id,
  Subject;
