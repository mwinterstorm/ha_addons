CREATE OR REPLACE VIEW `view_user_cohort_retention` AS

select `c`.`cohort_month` AS `cohort_month`,
    timestampdiff(
        MONTH,
        str_to_date(concat(`c`.`cohort_month`, '-01'), '%Y-%m-%d'),
        `a`.`activity_month`
    ) AS `months_since_join`,
    count(distinct `a`.`user_id`) AS `active_users`,
    `c`.`cohort_size` AS `cohort_size`,
    round(
        count(distinct `a`.`user_id`) / `c`.`cohort_size` * 100,
        1
    ) AS `pct_retained`
from (
        (
            select `users`.`id` AS `user_id`,
                date_format(`users`.`date_joined`, '%Y-%m') AS `cohort_month`,
                count(0) over (
                    partition by date_format(`users`.`date_joined`, '%Y-%m')
                ) AS `cohort_size`
            from `users`
            where `users`.`date_joined` is not null
        ) `c`
        join (
            select `contributions`.`user_id` AS `user_id`,
                str_to_date(
                    date_format(
                        convert_tz(
                            `contributions`.`timestamp`,
                            'UTC',
                            'Pacific/Auckland'
                        ),
                        '%Y-%m-01'
                    ),
                    '%Y-%m-%d'
                ) AS `activity_month`
            from `contributions`
            group by `contributions`.`user_id`,
                str_to_date(
                    date_format(
                        convert_tz(
                            `contributions`.`timestamp`,
                            'UTC',
                            'Pacific/Auckland'
                        ),
                        '%Y-%m-01'
                    ),
                    '%Y-%m-%d'
                )
        ) `a` on(
            `a`.`user_id` = `c`.`user_id`
            and `a`.`activity_month` >= str_to_date(concat(`c`.`cohort_month`, '-01'), '%Y-%m-%d')
        )
    )
group by `c`.`cohort_month`,
    timestampdiff(
        MONTH,
        str_to_date(concat(`c`.`cohort_month`, '-01'), '%Y-%m-%d'),
        `a`.`activity_month`
    )
order by `c`.`cohort_month`,
    timestampdiff(
        MONTH,
        str_to_date(concat(`c`.`cohort_month`, '-01'), '%Y-%m-%d'),
        `a`.`activity_month`
    )