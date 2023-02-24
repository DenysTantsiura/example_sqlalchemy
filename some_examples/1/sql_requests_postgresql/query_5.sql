/*
Знайти які курси читає певний викладач.
*/
SELECT
  t.id,
  t.name AS Teacher,
  sub.subject AS Subject
FROM
  subjects AS sub
  JOIN
  teachers AS t
	  ON sub.teacher_id = t.id
WHERE t.id = 2; -- Teacher = "Соломія Гузій";
--ORDER BY Teacher;
