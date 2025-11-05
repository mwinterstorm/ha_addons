UPDATE 
	users u
JOIN (
	SELECT 
		user_id,
		MIN(DATE(timestamp)) AS first_contrib,
		MAX(DATE(timestamp)) AS last_contrib
	FROM contributions
	GROUP BY user_id
) c ON u.id = c.user_id
SET 
	u.date_joined = c.first_contrib,
	u.last_contrib = c.last_contrib;