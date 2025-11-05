CREATE OR REPLACE VIEW `view_user_whales_and_minnows` AS

WITH user_agg AS (
    SELECT
        u.id AS id,
        u.full_name AS full_name,
        u.date_joined AS date_joined,
        COALESCE(SUM(c.fee), 0.00) AS total_fees,
        COALESCE(SUM(c.contribution), 0.00) AS total_contributions,
        TO_DAYS(CURDATE()) - TO_DAYS(u.date_joined) AS days_since_joining,
        CAST(MAX(c.timestamp) AS DATE) AS last_contribution_date
    FROM users u
    LEFT JOIN contributions c ON (u.id = c.user_id)
    GROUP BY u.id, u.full_name, u.date_joined
),
user_rates AS (
    SELECT
        ua.*,
        CASE WHEN ua.days_since_joining > 0 THEN ua.total_fees / ua.days_since_joining ELSE 0.00 END AS daily_average_fee,
        CASE WHEN ua.days_since_joining > 0 THEN ua.total_contributions / ua.days_since_joining ELSE 0.00 END AS daily_average_contribution,
        CASE WHEN ua.days_since_joining > 0 THEN ua.total_fees / ua.days_since_joining * 30.44 ELSE 0.00 END AS monthly_average_fee,
        CASE WHEN ua.days_since_joining > 0 THEN ua.total_contributions / ua.days_since_joining * 30.44 ELSE 0.00 END AS monthly_average_contribution
    FROM user_agg ua
),
fee_stats AS (
    SELECT
        AVG(user_rates.monthly_average_fee) AS avg_monthly_fee,
        STD(user_rates.monthly_average_fee) AS stddev_monthly_fee
    FROM user_rates
),
contrib_stats AS (
    SELECT
        AVG(user_rates.monthly_average_contribution) AS avg_monthly_contribution,
        STD(user_rates.monthly_average_contribution) AS stddev_monthly_contribution
    FROM user_rates
)
SELECT
    ur.id AS user_id,
    ur.full_name AS full_name,
    ur.date_joined AS date_joined,
    ur.last_contribution_date AS last_contribution_date,
    TO_DAYS(CURDATE()) - TO_DAYS(ur.last_contribution_date) AS days_since_last_contribution,
    ROUND(ur.total_fees, 2) AS total_fees,
    ROUND(ur.total_contributions, 2) AS total_contributions,
    ur.days_since_joining AS days_since_joining,
    ROUND(ur.daily_average_fee, 2) AS daily_average_fee,
    ROUND(ur.daily_average_contribution, 2) AS daily_average_contribution,
    ROUND(ur.monthly_average_fee, 2) AS monthly_average_fee,
    ROUND(ur.monthly_average_contribution, 2) AS monthly_average_contribution,
    CASE
        WHEN ur.last_contribution_date IS NULL
             OR TO_DAYS(CURDATE()) - TO_DAYS(ur.last_contribution_date) > 7 THEN 'inactive'
        WHEN ur.monthly_average_fee > fs.avg_monthly_fee + fs.stddev_monthly_fee THEN 'whale'
        WHEN ur.monthly_average_fee < fs.avg_monthly_fee - fs.stddev_monthly_fee THEN 'minnow'
        ELSE 'dolphin'
    END COLLATE utf8mb4_uca1400_ai_ci AS user_class_by_fee,
    CASE
        WHEN ur.last_contribution_date IS NULL
             OR TO_DAYS(CURDATE()) - TO_DAYS(ur.last_contribution_date) > 7 THEN 'inactive'
        WHEN ur.monthly_average_contribution > cs.avg_monthly_contribution + cs.stddev_monthly_contribution THEN 'whale'
        WHEN ur.monthly_average_contribution < cs.avg_monthly_contribution - cs.stddev_monthly_contribution THEN 'minnow'
        ELSE 'dolphin'
    END COLLATE utf8mb4_uca1400_ai_ci AS user_class_by_contribution
FROM user_rates ur
JOIN fee_stats fs
JOIN contrib_stats cs
ORDER BY ur.monthly_average_fee DESC;