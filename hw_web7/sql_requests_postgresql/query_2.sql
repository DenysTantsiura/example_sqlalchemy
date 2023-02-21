/*
-- Знайти студента із найвищим середнім балом з певного предмета.
*/
SELECT 
	s.id AS "ID", 
	s.name AS Student, 
	ROUND(AVG(a.value_), 1) AS "Success rate", 
	sub.subject AS Subject
FROM 
	assessments AS a 
	INNER JOIN 
	students AS s
		ON a.student_id = s.id
	INNER JOIN 
	subjects AS sub
		ON a.subject_id = sub.id
WHERE 
	sub.id = 5 -- sub.subject = "History"
GROUP BY 
	s.id, Subject, a.subject_id
ORDER BY 
	AVG(a.value_) DESC, 
	a.subject_id LIMIT 1;

-- alternative not working... :
/*SELECT 
    first_.id, 
    first_.Student, 
    MAX(first_.Average) AS "Success rate", 
    first_.Subject  
FROM (SELECT 
        s.id AS id, 
        AVG(a.value_) AS Average, 
        s.name AS Student, 
        sub.subject AS Subject
	  FROM 
        assessments AS a 
	    INNER JOIN 
        students AS s
			ON a.student_id = s.id
	    INNER JOIN 
        subjects AS sub
			ON a.subject_id = sub.id
	    WHERE sub.id = 5 -- sub.subject = "History"
	  GROUP BY 
        Student, Subject) AS first_
GROUP BY 
    Subject;
*/