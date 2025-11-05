CREATE OR REPLACE VIEW `view_summary_medians` AS

with user_metrics as (
    select `u`.`id` AS `id`,
        `u`.`full_name` AS `full_name`,
        `u`.`date_joined` AS `date_joined`,
        coalesce(sum(`c`.`contribution`), 0.00) AS `total_contributions`,
        coalesce(sum(`c`.`fee`), 0.00) AS `total_fees`,
        to_days(curdate()) - to_days(`u`.`date_joined`) AS `days_since_joining`,
        cast(max(`c`.`timestamp`) as date) AS `last_contribution_date`
    from (
            `users` `u`
            left join `contributions` `c` on(`u`.`id` = `c`.`user_id`)
        )
    group by `u`.`id`,
        `u`.`full_name`,
        `u`.`date_joined`
),
user_daily_averages as (
    select `um`.`id` AS `id`,
case
            when `um`.`days_since_joining` > 0 then `um`.`total_contributions` / `um`.`days_since_joining`
            else 0.00
        end AS `daily_average_contribution`,
case
            when `um`.`days_since_joining` > 0 then `um`.`total_fees` / `um`.`days_since_joining`
            else 0.00
        end AS `daily_average_fee`,
case
            when `um`.`total_contributions` > 0 then `um`.`total_fees` / `um`.`total_contributions`
            else 0.00
        end AS `user_fee_percentage`
    from `user_metrics` `um`
    where `um`.`last_contribution_date` is not null
        and to_days(curdate()) - to_days(`um`.`last_contribution_date`) <= 7
),
ranked as (
    select `uda`.`daily_average_contribution` AS `daily_average_contribution`,
        `uda`.`daily_average_fee` AS `daily_average_fee`,
        `uda`.`user_fee_percentage` AS `user_fee_percentage`,
        row_number() over (
            order by `uda`.`daily_average_contribution`
        ) AS `rn_contrib`,
        row_number() over (
            order by `uda`.`daily_average_fee`
        ) AS `rn_fee`,
        row_number() over (
            order by `uda`.`user_fee_percentage`
        ) AS `rn_fee_percentage`,
        count(0) over () AS `total`
    from `user_daily_averages` `uda`
),
medians as (
    select avg(
            case
                when `ranked`.`rn_contrib` in (
                    floor((`ranked`.`total` + 1) / 2),
                    floor((`ranked`.`total` + 2) / 2)
                ) then `ranked`.`daily_average_contribution`
            end
        ) AS `median_daily_contribution`,
        avg(
            case
                when `ranked`.`rn_fee` in (
                    floor((`ranked`.`total` + 1) / 2),
                    floor((`ranked`.`total` + 2) / 2)
                ) then `ranked`.`daily_average_fee`
            end
        ) AS `median_daily_fee`,
        avg(
            case
                when `ranked`.`rn_fee_percentage` in (
                    floor((`ranked`.`total` + 1) / 2),
                    floor((`ranked`.`total` + 2) / 2)
                ) then `ranked`.`user_fee_percentage`
            end
        ) AS `median_of_per_user_fee_percentages`
    from `ranked`
)
select `m`.`median_daily_contribution` AS `median_daily_contribution`,
    `m`.`median_daily_fee` AS `median_daily_fee`,
    `m`.`median_of_per_user_fee_percentages` AS `median_of_per_user_fee_percentages`,
case
        when `m`.`median_daily_contribution` > 0 then `m`.`median_daily_fee` / `m`.`median_daily_contribution`
        else NULL
    end AS `percentage_of_medians`
from `medians` `m`