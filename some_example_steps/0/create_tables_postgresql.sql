/*The example shows various uses 
    of some similar commands (sequence)*/
			
-- Table: groups
DROP TABLE IF EXISTS groups_;
CREATE TABLE groups_ (
  id SERIAL PRIMARY KEY,
  group_name CHAR(7) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: students
DROP TABLE IF EXISTS students;
CREATE TABLE students (
  id SERIAL PRIMARY KEY,
  name VARCHAR(50) UNIQUE NOT NULL,
  group_id INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (group_id) REFERENCES groups_ (id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);
		
-- Table: teachers
DROP TABLE IF EXISTS teachers;
CREATE TABLE teachers (
  id SERIAL PRIMARY KEY,
  name VARCHAR(50) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
			
-- Table: subjects
DROP TABLE IF EXISTS subjects;
CREATE TABLE subjects (
  id SERIAL PRIMARY KEY,
  subject CHAR(30), 
  teacher_id INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (teacher_id) REFERENCES teachers (id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);			
				
-- Table: assessments
DROP TABLE IF EXISTS assessments;
CREATE TABLE assessments (
  id SERIAL PRIMARY KEY, 
  value_ NUMERIC CHECK (value_ > 0 AND value_ < 6), 
  date_of DATE NOT NULL,
  subject_id INTEGER, 
  student_id INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (subject_id) REFERENCES subjects (id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,       
  FOREIGN KEY (student_id) REFERENCES students (id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);					
	