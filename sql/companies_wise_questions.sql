SELECT
  company,
  COUNT(company) AS comp_solved
FROM (
  SELECT 
    question_title,
    company
  FROM
    `analytics-engineering-101.coding_question.stg_raw_questions`,
    UNNEST(SPLIT(company, ' | ')) company
  WHERE
    TIMESTAMP_TRUNC(created_at, DAY) >= TIMESTAMP("2022-10-29")
) s
GROUP BY
  1
ORDER BY
  2 DESC
;