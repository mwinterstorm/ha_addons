CREATE OR REPLACE VIEW `view_summary_periodic_stats` AS

with NZ_Time as (
    select cast(
            convert_tz(utc_timestamp(), 'UTC', 'Pacific/Auckland') as date
        ) AS `today_nz`,
        'Pacific/Auckland' AS `nz_timezone`,
        'UTC' AS `utc_timezone`
),
FinancialYearStart as (
    select if(
            month(`nz`.`today_nz`) >= 7,
            date_format(`nz`.`today_nz`, '%Y-07-01'),
            date_format(`nz`.`today_nz` - interval 1 year, '%Y-07-01')
        ) AS `current_fy_start`
    from `NZ_Time` `nz`
),
DateRanges as (
    select `nz`.`today_nz` AS `today_nz`,
        `nz`.`today_nz` - interval weekday(`nz`.`today_nz`) day AS `this_week_start`,
        `nz`.`today_nz` - interval weekday(`nz`.`today_nz`) day + interval 6 day AS `this_week_end`,
        `nz`.`today_nz` - interval weekday(`nz`.`today_nz`) day - interval 7 day AS `last_week_start`,
        `nz`.`today_nz` - interval weekday(`nz`.`today_nz`) day - interval 1 day AS `last_week_end`,
        date_format(`nz`.`today_nz`, '%Y-%m-01') AS `this_month_start`,
        last_day(`nz`.`today_nz`) AS `this_month_end`,
        date_format(`nz`.`today_nz` - interval 1 month, '%Y-%m-01') AS `last_month_start`,
        last_day(`nz`.`today_nz` - interval 1 month) AS `last_month_end`,
        `fys`.`current_fy_start` AS `current_fy_start`,
        `fys`.`current_fy_start` + interval 1 year - interval 1 day AS `current_fy_end`,
        `fys`.`current_fy_start` - interval 1 year AS `last_fy_start`,
        `fys`.`current_fy_start` - interval 1 day AS `last_fy_end`
    from (
            `NZ_Time` `nz`
            join `FinancialYearStart` `fys`
        )
),
DailyStats as (
    select cast(
            convert_tz(
                `c`.`timestamp`,
                `nz`.`utc_timezone`,
                `nz`.`nz_timezone`
            ) as date
        ) AS `stat_date`,
        count(distinct `c`.`user_id`) AS `daily_users`,
        sum(`c`.`contribution`) AS `daily_contributions`,
        sum(`c`.`fee`) AS `daily_fees`,
        sum(`c`.`fee`) / nullif(count(distinct `c`.`user_id`), 0) AS `daily_arpu`
    from (
            `contributions` `c`
            join `NZ_Time` `nz`
        )
    group by cast(
            convert_tz(
                `c`.`timestamp`,
                `nz`.`utc_timezone`,
                `nz`.`nz_timezone`
            ) as date
        )
),
MaxValues as (
    select (
            select max(`DailyStats`.`daily_users`)
            from `DailyStats`
        ) AS `max_users_value`,
(
            select `DailyStats`.`stat_date`
            from `DailyStats`
            order by `DailyStats`.`daily_users` desc,
                `DailyStats`.`stat_date` desc
            limit 1
        ) AS `max_users_date`,
(
            select max(`DailyStats`.`daily_contributions`)
            from `DailyStats`
        ) AS `max_contributions_value`,
(
            select `DailyStats`.`stat_date`
            from `DailyStats`
            order by `DailyStats`.`daily_contributions` desc,
                `DailyStats`.`stat_date` desc
            limit 1
        ) AS `max_contributions_date`,
(
            select max(`DailyStats`.`daily_fees`)
            from `DailyStats`
        ) AS `max_fees_value`,
(
            select `DailyStats`.`stat_date`
            from `DailyStats`
            order by `DailyStats`.`daily_fees` desc,
                `DailyStats`.`stat_date` desc
            limit 1
        ) AS `max_fees_date`,
(
            select max(`DailyStats`.`daily_arpu`)
            from `DailyStats`
        ) AS `max_arpu_value`,
(
            select `DailyStats`.`stat_date`
            from `DailyStats`
            order by `DailyStats`.`daily_arpu` desc,
                `DailyStats`.`stat_date` desc
            limit 1
        ) AS `max_arpu_date`
)
select 'unique_users' AS `metric`,
(
        select `DailyStats`.`daily_users`
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz`
    ) AS `today`,
(
        select `DailyStats`.`daily_users`
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz` - interval 1 day
    ) AS `yesterday`,
(
        select count(distinct `c`.`user_id`)
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`this_week_start` and `dr`.`this_week_end`
    ) AS `this_week`,
(
        select count(distinct `c`.`user_id`)
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`last_week_start` and `dr`.`last_week_end`
    ) AS `last_week`,
(
        select count(distinct `c`.`user_id`)
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`this_month_start` and `dr`.`this_month_end`
    ) AS `this_month`,
(
        select count(distinct `c`.`user_id`)
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`last_month_start` and `dr`.`last_month_end`
    ) AS `last_month`,
(
        select count(distinct `c`.`user_id`)
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`current_fy_start` and `dr`.`current_fy_end`
    ) AS `current_fy`,
(
        select count(distinct `c`.`user_id`)
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`last_fy_start` and `dr`.`last_fy_end`
    ) AS `last_fy`,
(
        select count(
                distinct `contributions`.`user_id`
            )
        from `contributions`
    ) AS `all_time`,
    `mv`.`max_users_value` AS `max_users_value`,
    `mv`.`max_users_date` AS `max_users_date`
from (
        `DateRanges` `dr`
        join `MaxValues` `mv`
    )
union all
select 'daily_avg_users' AS `metric`,
(
        select `DailyStats`.`daily_users`
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz`
    ) AS `Name_exp_2`,
(
        select `DailyStats`.`daily_users`
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz` - interval 1 day
    ) AS `Name_exp_3`,
(
        select round(avg(`DailyStats`.`daily_users`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_week_start` and `dr`.`this_week_end`
    ) AS `Name_exp_4`,
(
        select round(avg(`DailyStats`.`daily_users`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_week_start` and `dr`.`last_week_end`
    ) AS `Name_exp_5`,
(
        select round(avg(`DailyStats`.`daily_users`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_month_start` and `dr`.`this_month_end`
    ) AS `Name_exp_6`,
(
        select round(avg(`DailyStats`.`daily_users`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_month_start` and `dr`.`last_month_end`
    ) AS `Name_exp_7`,
(
        select round(avg(`DailyStats`.`daily_users`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`current_fy_start` and `dr`.`current_fy_end`
    ) AS `Name_exp_8`,
(
        select round(avg(`DailyStats`.`daily_users`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_fy_start` and `dr`.`last_fy_end`
    ) AS `Name_exp_9`,
(
        select round(avg(`DailyStats`.`daily_users`), 2)
        from `DailyStats`
    ) AS `(SELECT ROUND(AVG(daily_users), 2) FROM DailyStats)`,
    NULL AS `NULL`,
    NULL AS `NULL`
from `DateRanges` `dr`
union all
select 'contributions' AS `metric`,
(
        select round(`DailyStats`.`daily_contributions`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz`
    ) AS `Name_exp_2`,
(
        select round(`DailyStats`.`daily_contributions`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz` - interval 1 day
    ) AS `Name_exp_3`,
(
        select round(sum(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_week_start` and `dr`.`this_week_end`
    ) AS `Name_exp_4`,
(
        select round(sum(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_week_start` and `dr`.`last_week_end`
    ) AS `Name_exp_5`,
(
        select round(sum(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_month_start` and `dr`.`this_month_end`
    ) AS `Name_exp_6`,
(
        select round(sum(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_month_start` and `dr`.`last_month_end`
    ) AS `Name_exp_7`,
(
        select round(sum(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`current_fy_start` and `dr`.`current_fy_end`
    ) AS `Name_exp_8`,
(
        select round(sum(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_fy_start` and `dr`.`last_fy_end`
    ) AS `Name_exp_9`,
(
        select round(sum(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
    ) AS `(SELECT ROUND(SUM(daily_contributions), 2) FROM DailyStats)`,
    round(`mv`.`max_contributions_value`, 2) AS `ROUND(mv.max_contributions_value, 2)`,
    `mv`.`max_contributions_date` AS `max_contributions_date`
from (
        `DateRanges` `dr`
        join `MaxValues` `mv`
    )
union all
select 'daily_avg_contributions' AS `metric`,
(
        select round(`DailyStats`.`daily_contributions`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz`
    ) AS `Name_exp_2`,
(
        select round(`DailyStats`.`daily_contributions`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz` - interval 1 day
    ) AS `Name_exp_3`,
(
        select round(avg(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_week_start` and `dr`.`this_week_end`
    ) AS `Name_exp_4`,
(
        select round(avg(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_week_start` and `dr`.`last_week_end`
    ) AS `Name_exp_5`,
(
        select round(avg(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_month_start` and `dr`.`this_month_end`
    ) AS `Name_exp_6`,
(
        select round(avg(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_month_start` and `dr`.`last_month_end`
    ) AS `Name_exp_7`,
(
        select round(avg(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`current_fy_start` and `dr`.`current_fy_end`
    ) AS `Name_exp_8`,
(
        select round(avg(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_fy_start` and `dr`.`last_fy_end`
    ) AS `Name_exp_9`,
(
        select round(avg(`DailyStats`.`daily_contributions`), 2)
        from `DailyStats`
    ) AS `(SELECT ROUND(AVG(daily_contributions), 2) FROM DailyStats)`,
    NULL AS `NULL`,
    NULL AS `NULL`
from `DateRanges` `dr`
union all
select 'fees' AS `metric`,
(
        select round(`DailyStats`.`daily_fees`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz`
    ) AS `Name_exp_2`,
(
        select round(`DailyStats`.`daily_fees`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz` - interval 1 day
    ) AS `Name_exp_3`,
(
        select round(sum(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_week_start` and `dr`.`this_week_end`
    ) AS `Name_exp_4`,
(
        select round(sum(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_week_start` and `dr`.`last_week_end`
    ) AS `Name_exp_5`,
(
        select round(sum(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_month_start` and `dr`.`this_month_end`
    ) AS `Name_exp_6`,
(
        select round(sum(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_month_start` and `dr`.`last_month_end`
    ) AS `Name_exp_7`,
(
        select round(sum(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`current_fy_start` and `dr`.`current_fy_end`
    ) AS `Name_exp_8`,
(
        select round(sum(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_fy_start` and `dr`.`last_fy_end`
    ) AS `Name_exp_9`,
(
        select round(sum(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
    ) AS `(SELECT ROUND(SUM(daily_fees), 2) FROM DailyStats)`,
    round(`mv`.`max_fees_value`, 2) AS `ROUND(mv.max_fees_value, 2)`,
    `mv`.`max_fees_date` AS `max_fees_date`
from (
        `DateRanges` `dr`
        join `MaxValues` `mv`
    )
union all
select 'daily_avg_fees' AS `metric`,
(
        select round(`DailyStats`.`daily_fees`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz`
    ) AS `Name_exp_2`,
(
        select round(`DailyStats`.`daily_fees`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz` - interval 1 day
    ) AS `Name_exp_3`,
(
        select round(avg(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_week_start` and `dr`.`this_week_end`
    ) AS `Name_exp_4`,
(
        select round(avg(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_week_start` and `dr`.`last_week_end`
    ) AS `Name_exp_5`,
(
        select round(avg(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_month_start` and `dr`.`this_month_end`
    ) AS `Name_exp_6`,
(
        select round(avg(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_month_start` and `dr`.`last_month_end`
    ) AS `Name_exp_7`,
(
        select round(avg(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`current_fy_start` and `dr`.`current_fy_end`
    ) AS `Name_exp_8`,
(
        select round(avg(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_fy_start` and `dr`.`last_fy_end`
    ) AS `Name_exp_9`,
(
        select round(avg(`DailyStats`.`daily_fees`), 2)
        from `DailyStats`
    ) AS `(SELECT ROUND(AVG(daily_fees), 2) FROM DailyStats)`,
    NULL AS `NULL`,
    NULL AS `NULL`
from `DateRanges` `dr`
union all
select 'arpu' AS `metric`,
(
        select round(`DailyStats`.`daily_arpu`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz`
    ) AS `Name_exp_2`,
(
        select round(`DailyStats`.`daily_arpu`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz` - interval 1 day
    ) AS `Name_exp_3`,
(
        select round(
                sum(`c`.`fee`) / nullif(count(distinct `c`.`user_id`), 0),
                2
            )
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`this_week_start` and `dr`.`this_week_end`
    ) AS `Name_exp_4`,
(
        select round(
                sum(`c`.`fee`) / nullif(count(distinct `c`.`user_id`), 0),
                2
            )
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`last_week_start` and `dr`.`last_week_end`
    ) AS `Name_exp_5`,
(
        select round(
                sum(`c`.`fee`) / nullif(count(distinct `c`.`user_id`), 0),
                2
            )
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`this_month_start` and `dr`.`this_month_end`
    ) AS `Name_exp_6`,
(
        select round(
                sum(`c`.`fee`) / nullif(count(distinct `c`.`user_id`), 0),
                2
            )
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`last_month_start` and `dr`.`last_month_end`
    ) AS `Name_exp_7`,
(
        select round(
                sum(`c`.`fee`) / nullif(count(distinct `c`.`user_id`), 0),
                2
            )
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`current_fy_start` and `dr`.`current_fy_end`
    ) AS `Name_exp_8`,
(
        select round(
                sum(`c`.`fee`) / nullif(count(distinct `c`.`user_id`), 0),
                2
            )
        from (
                `contributions` `c`
                join `NZ_Time` `nz`
            )
        where cast(
                convert_tz(
                    `c`.`timestamp`,
                    `nz`.`utc_timezone`,
                    `nz`.`nz_timezone`
                ) as date
            ) between `dr`.`last_fy_start` and `dr`.`last_fy_end`
    ) AS `Name_exp_9`,
(
        select round(
                sum(`contributions`.`fee`) / nullif(
                    count(
                        distinct `contributions`.`user_id`
                    ),
                    0
                ),
                2
            )
        from `contributions`
    ) AS `Name_exp_10`,
    round(`mv`.`max_arpu_value`, 2) AS `ROUND(mv.max_arpu_value, 2)`,
    `mv`.`max_arpu_date` AS `max_arpu_date`
from (
        `DateRanges` `dr`
        join `MaxValues` `mv`
    )
union all
select 'daily_avg_arpu' AS `metric`,
(
        select round(`DailyStats`.`daily_arpu`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz`
    ) AS `Name_exp_2`,
(
        select round(`DailyStats`.`daily_arpu`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz` - interval 1 day
    ) AS `Name_exp_3`,
(
        select round(avg(`DailyStats`.`daily_arpu`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_week_start` and `dr`.`this_week_end`
    ) AS `Name_exp_4`,
(
        select round(avg(`DailyStats`.`daily_arpu`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_week_start` and `dr`.`last_week_end`
    ) AS `Name_exp_5`,
(
        select round(avg(`DailyStats`.`daily_arpu`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`this_month_start` and `dr`.`this_month_end`
    ) AS `Name_exp_6`,
(
        select round(avg(`DailyStats`.`daily_arpu`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_month_start` and `dr`.`last_month_end`
    ) AS `Name_exp_7`,
(
        select round(avg(`DailyStats`.`daily_arpu`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`current_fy_start` and `dr`.`current_fy_end`
    ) AS `Name_exp_8`,
(
        select round(avg(`DailyStats`.`daily_arpu`), 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` between `dr`.`last_fy_start` and `dr`.`last_fy_end`
    ) AS `Name_exp_9`,
(
        select round(avg(`DailyStats`.`daily_arpu`), 2)
        from `DailyStats`
    ) AS `(SELECT ROUND(AVG(daily_arpu), 2) FROM DailyStats)`,
    NULL AS `NULL`,
    NULL AS `NULL`
from `DateRanges` `dr`
union all
select 'daily_average_user_arpu' AS `metric`,
(
        select round(`DailyStats`.`daily_arpu`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz`
    ) AS `Name_exp_2`,
(
        select round(`DailyStats`.`daily_arpu`, 2)
        from `DailyStats`
        where `DailyStats`.`stat_date` = `dr`.`today_nz` - interval 1 day
    ) AS `Name_exp_3`,
(
        select round(
                (
                    select sum(`c`.`fee`)
                    from (
                            `contributions` `c`
                            join `NZ_Time` `nz`
                        )
                    where cast(
                            convert_tz(
                                `c`.`timestamp`,
                                `nz`.`utc_timezone`,
                                `nz`.`nz_timezone`
                            ) as date
                        ) between `dr`.`this_week_start` and `dr`.`this_week_end`
                ) / nullif(
                    (
                        select count(distinct `c`.`user_id`)
                        from (
                                `contributions` `c`
                                join `NZ_Time` `nz`
                            )
                        where cast(
                                convert_tz(
                                    `c`.`timestamp`,
                                    `nz`.`utc_timezone`,
                                    `nz`.`nz_timezone`
                                ) as date
                            ) between `dr`.`this_week_start` and `dr`.`this_week_end`
                    ),
                    0
                ) / (
                    to_days(`dr`.`today_nz`) - to_days(`dr`.`this_week_start`) + 1
                ),
                2
            )
    ) AS `Name_exp_4`,
(
        select round(
                (
                    select sum(`c`.`fee`)
                    from (
                            `contributions` `c`
                            join `NZ_Time` `nz`
                        )
                    where cast(
                            convert_tz(
                                `c`.`timestamp`,
                                `nz`.`utc_timezone`,
                                `nz`.`nz_timezone`
                            ) as date
                        ) between `dr`.`last_week_start` and `dr`.`last_week_end`
                ) / nullif(
                    (
                        select count(distinct `c`.`user_id`)
                        from (
                                `contributions` `c`
                                join `NZ_Time` `nz`
                            )
                        where cast(
                                convert_tz(
                                    `c`.`timestamp`,
                                    `nz`.`utc_timezone`,
                                    `nz`.`nz_timezone`
                                ) as date
                            ) between `dr`.`last_week_start` and `dr`.`last_week_end`
                    ),
                    0
                ) / (
                    to_days(`dr`.`last_week_end`) - to_days(`dr`.`last_week_start`) + 1
                ),
                2
            )
    ) AS `Name_exp_5`,
(
        select round(
                (
                    select sum(`c`.`fee`)
                    from (
                            `contributions` `c`
                            join `NZ_Time` `nz`
                        )
                    where cast(
                            convert_tz(
                                `c`.`timestamp`,
                                `nz`.`utc_timezone`,
                                `nz`.`nz_timezone`
                            ) as date
                        ) between `dr`.`this_month_start` and `dr`.`this_month_end`
                ) / nullif(
                    (
                        select count(distinct `c`.`user_id`)
                        from (
                                `contributions` `c`
                                join `NZ_Time` `nz`
                            )
                        where cast(
                                convert_tz(
                                    `c`.`timestamp`,
                                    `nz`.`utc_timezone`,
                                    `nz`.`nz_timezone`
                                ) as date
                            ) between `dr`.`this_month_start` and `dr`.`this_month_end`
                    ),
                    0
                ) / (
                    to_days(`dr`.`today_nz`) - to_days(`dr`.`this_month_start`) + 1
                ),
                2
            )
    ) AS `Name_exp_6`,
(
        select round(
                (
                    select sum(`c`.`fee`)
                    from (
                            `contributions` `c`
                            join `NZ_Time` `nz`
                        )
                    where cast(
                            convert_tz(
                                `c`.`timestamp`,
                                `nz`.`utc_timezone`,
                                `nz`.`nz_timezone`
                            ) as date
                        ) between `dr`.`last_month_start` and `dr`.`last_month_end`
                ) / nullif(
                    (
                        select count(distinct `c`.`user_id`)
                        from (
                                `contributions` `c`
                                join `NZ_Time` `nz`
                            )
                        where cast(
                                convert_tz(
                                    `c`.`timestamp`,
                                    `nz`.`utc_timezone`,
                                    `nz`.`nz_timezone`
                                ) as date
                            ) between `dr`.`last_month_start` and `dr`.`last_month_end`
                    ),
                    0
                ) / (
                    to_days(`dr`.`last_month_end`) - to_days(`dr`.`last_month_start`) + 1
                ),
                2
            )
    ) AS `Name_exp_7`,
(
        select round(
                (
                    select sum(`c`.`fee`)
                    from (
                            `contributions` `c`
                            join `NZ_Time` `nz`
                        )
                    where cast(
                            convert_tz(
                                `c`.`timestamp`,
                                `nz`.`utc_timezone`,
                                `nz`.`nz_timezone`
                            ) as date
                        ) between `dr`.`current_fy_start` and `dr`.`current_fy_end`
                ) / nullif(
                    (
                        select count(distinct `c`.`user_id`)
                        from (
                                `contributions` `c`
                                join `NZ_Time` `nz`
                            )
                        where cast(
                                convert_tz(
                                    `c`.`timestamp`,
                                    `nz`.`utc_timezone`,
                                    `nz`.`nz_timezone`
                                ) as date
                            ) between `dr`.`current_fy_start` and `dr`.`current_fy_end`
                    ),
                    0
                ) / (
                    to_days(`dr`.`today_nz`) - to_days(`dr`.`current_fy_start`) + 1
                ),
                2
            )
    ) AS `Name_exp_8`,
(
        select round(
                (
                    select sum(`c`.`fee`)
                    from (
                            `contributions` `c`
                            join `NZ_Time` `nz`
                        )
                    where cast(
                            convert_tz(
                                `c`.`timestamp`,
                                `nz`.`utc_timezone`,
                                `nz`.`nz_timezone`
                            ) as date
                        ) between `dr`.`last_fy_start` and `dr`.`last_fy_end`
                ) / nullif(
                    (
                        select count(distinct `c`.`user_id`)
                        from (
                                `contributions` `c`
                                join `NZ_Time` `nz`
                            )
                        where cast(
                                convert_tz(
                                    `c`.`timestamp`,
                                    `nz`.`utc_timezone`,
                                    `nz`.`nz_timezone`
                                ) as date
                            ) between `dr`.`last_fy_start` and `dr`.`last_fy_end`
                    ),
                    0
                ) / (
                    to_days(`dr`.`last_fy_end`) - to_days(`dr`.`last_fy_start`) + 1
                ),
                2
            )
    ) AS `Name_exp_9`,
(
        select round(
                (
                    select sum(`c`.`fee`)
                    from `contributions` `c`
                    where 1 = 1
                ) / nullif(
                    (
                        select count(distinct `c`.`user_id`)
                        from `contributions` `c`
                        where 1 = 1
                    ),
                    0
                ) / (
                    to_days(
                        (
                            select max(
                                    cast(
                                        convert_tz(
                                            `contributions`.`timestamp`,
                                            'UTC',
                                            'Pacific/Auckland'
                                        ) as date
                                    )
                                )
                            from `contributions`
                        )
                    ) - to_days(
                        (
                            select min(
                                    cast(
                                        convert_tz(
                                            `contributions`.`timestamp`,
                                            'UTC',
                                            'Pacific/Auckland'
                                        ) as date
                                    )
                                )
                            from `contributions`
                        )
                    ) + 1
                ),
                2
            )
    ) AS `(SELECT ROUND(AVG(daily_arpu), 2) FROM DailyStats)`,
    NULL AS `NULL`,
    NULL AS `NULL`
from `DateRanges` `dr`