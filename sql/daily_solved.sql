{# Get Daily question solved #}

SELECT
    EXTRACT(DATE FROM created_at) AS dt,
    COUNT(page_id) AS daily_questions
FROM
    `analytics-engineering-101.coding_question.stg_raw_questions`
WHERE
    TIMESTAMP_TRUNC(created_at, DAY) >= TIMESTAMP("2022-10-29")
GROUP BY
    1
ORDER BY
    1 ASC
;
