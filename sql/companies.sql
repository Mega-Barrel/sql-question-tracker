-- Get top 10 Companies, with most solved questions

WITH raw_company AS (
	SELECT
		unnest(company) AS company
	FROM
		raw_data
	WHERE
		DATE(created_at) > '2021-01-01'
)
,
new_entries AS (
	SELECT
		company,
		COUNT(company) AS question_solved
	FROM
		raw_company
	GROUP BY
		1
)

MERGE INTO
	companies_solved cs
USING
	new_entries ne
ON
	cs.company = ne.company
WHEN MATCHED
	THEN
	UPDATE SET 
		question_solved = cs.question_solved + ne.question_solved
WHEN NOT MATCHED THEN
  INSERT (company, question_solved)
  values (ne.company, ne.question_solved);
;