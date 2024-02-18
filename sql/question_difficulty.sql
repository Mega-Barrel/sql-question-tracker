-- Get total questions solved by difficulty level.

SELECT
    difficulty,
    COUNT(difficulty) AS diff
FROM
    `analytics-engineering-101.coding_question.stg_raw_questions`
WHERE 
    TIMESTAMP_TRUNC(created_at, DAY) >= TIMESTAMP("2022-10-29")
GROUP BY
    1
;