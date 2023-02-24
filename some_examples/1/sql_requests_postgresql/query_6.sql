/*
Знайти список студентів у певній групі.
*/
SELECT
  s.id,
  s.name AS Student, 
  g.group_name AS Group_
FROM
  students AS s
  JOIN
  groups_ AS g
	  ON s.group_id = g.id
WHERE g.id = 3; -- Group_ = "Group-2";
--ORDER BY Group_;
