-- SELECT Tables
SELECT * FROM max_date;
SELECT * FROM raw_data ORDER BY created_at DESC;
SELECT * FROM daily_solve   d ORDER BY created_at DESC;
SELECT * FROM companies_solved WHERE company <> 'N/A';
SELECT * FROM ques_difficulty;

-- DROP TABLE
DROP TABLE max_date;
DROP TABLE raw_data;
DROP TABLE companies_solved;
DROP TABLE ques_difficulty;
DROP TABLE daily_solved;