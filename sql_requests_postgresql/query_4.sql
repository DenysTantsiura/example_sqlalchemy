/*
Знайти середній бал на потоці (по всій таблиці оцінок).
*/
SELECT
  ROUND(AVG(a.value_), 2) AS "Absolute assessment"
FROM
  assessments AS a;
