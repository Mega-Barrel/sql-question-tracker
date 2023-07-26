-- GET total question solved on various platforms (eg. leetcode, StrataScratch, etc)

WITH new_entries AS (
	SELECT
		difficulty,
		COUNT(difficulty) AS question_solved
	FROM
		raw_data
	WHERE
		DATE(created_at) > '2021-01-01'
	GROUP BY
		1
)

MERGE INTO
	ques_difficulty qd
USING
	new_entries ne
ON
	qd.difficulty = ne.difficulty
WHEN MATCHED
	THEN
	UPDATE SET 
		question_solved = qd.question_solved + ne.question_solved
WHEN NOT MATCHED THEN
  INSERT (difficulty, question_solved)
  values (ne.difficulty, ne.question_solved);
;