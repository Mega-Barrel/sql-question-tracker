-- Get Daily Solved Questions

SELECT
	DATE(created_at) AS date,
	COUNT(DATE(created_at)) AS daily_solved
FROM
	raw_data
GROUP BY
	1
ORDER BY
	1
;