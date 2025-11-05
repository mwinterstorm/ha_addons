CREATE OR REPLACE VIEW `view_user_new_returning_total_all_time` AS

SELECT
  d.local_day AS day,
  COUNT(DISTINCT CASE WHEN d.local_day = d.first_contrib_date THEN d.user_id END) AS new_users,
  COUNT(DISTINCT CASE WHEN d.local_day >  d.first_contrib_date THEN d.user_id END) AS returning_users,
  COUNT(DISTINCT d.user_id) AS total_daily_users,
  (
    SELECT COUNT(0)
    FROM (
      SELECT
        c2.user_id,
        CAST(CONVERT_TZ(MIN(c2.timestamp), 'UTC', 'Pacific/Auckland') AS DATE) AS first_contrib_date
      FROM contributions c2
      GROUP BY c2.user_id
    ) firsts
    WHERE firsts.first_contrib_date <= d.local_day
  ) AS total_users_all_time,
  -- Idle users per day: users whose last contribution on or before `day` was more than 14 days ago
  (
    SELECT COUNT(*)
    FROM (SELECT DISTINCT c_u.user_id FROM contributions c_u) u
    WHERE IFNULL(
      (
        SELECT MAX(CAST(CONVERT_TZ(c3.timestamp, 'UTC', 'Pacific/Auckland') AS DATE))
        FROM contributions c3
        WHERE c3.user_id = u.user_id
          AND CAST(CONVERT_TZ(c3.timestamp, 'UTC', 'Pacific/Auckland') AS DATE) <= d.local_day
      ), d.local_day
    ) <= d.local_day - INTERVAL 14 DAY
  ) AS idle_users,
  -- Optional: active users as of `day` = total_users_all_time - idle_users
  (
    (
      SELECT COUNT(0)
      FROM (
        SELECT c2.user_id,
               CAST(CONVERT_TZ(MIN(c2.timestamp), 'UTC', 'Pacific/Auckland') AS DATE) AS first_contrib_date
        FROM contributions c2
        GROUP BY c2.user_id
      ) firsts
      WHERE firsts.first_contrib_date <= d.local_day
    )
    -
    (
      SELECT COUNT(*)
      FROM (SELECT DISTINCT c_u.user_id FROM contributions c_u) u
      WHERE IFNULL(
        (
          SELECT MAX(CAST(CONVERT_TZ(c3.timestamp, 'UTC', 'Pacific/Auckland') AS DATE))
          FROM contributions c3
          WHERE c3.user_id = u.user_id
            AND CAST(CONVERT_TZ(c3.timestamp, 'UTC', 'Pacific/Auckland') AS DATE) <= d.local_day
        ), d.local_day
      ) <= d.local_day - INTERVAL 14 DAY
    )
  ) AS active_users
FROM (
  SELECT
    c.user_id,
    CAST(CONVERT_TZ(c.timestamp, 'UTC', 'Pacific/Auckland') AS DATE) AS local_day,
    (
      SELECT CAST(CONVERT_TZ(MIN(c2.timestamp), 'UTC', 'Pacific/Auckland') AS DATE)
      FROM contributions c2
      WHERE c2.user_id = c.user_id
    ) AS first_contrib_date
  FROM contributions c
) d
GROUP BY d.local_day
ORDER BY d.local_day DESC;