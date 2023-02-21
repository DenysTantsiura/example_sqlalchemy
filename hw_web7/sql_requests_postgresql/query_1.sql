-- Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
SELECT 
    s.id, 
    s.name AS "Student", 
    ROUND(avg(a.value_), 1) AS "Success rate"
FROM 
    assessments a 
    INNER JOIN 
    students s
		ON a.student_id = s.id
GROUP BY s.id
ORDER BY avg(a.value_) DESC 
LIMIT 5;
