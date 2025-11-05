CREATE OR REPLACE VIEW `view_user_cumulative_stats_by_individual` AS

select `u`.`id` AS `id`,
    `u`.`full_name` AS `full_name`,
    `u`.`date_joined` AS `date_joined`,
    cast(sum(`c`.`contribution`) as decimal(18, 2)) AS `total_contributions`,
    cast(sum(`c`.`fee`) as decimal(18, 2)) AS `total_fees`,
    cast(sum(`c`.`transaction_count`) as decimal(18, 0)) AS `total_transactions`,
    count(`c`.`id`) AS `num_contributions`,
    to_days(max(`c`.`timestamp`)) - to_days(min(`c`.`timestamp`)) AS `lifespan_days`
from (
        `users` `u`
        join `contributions` `c` on(`u`.`id` = `c`.`user_id`)
    )
group by `u`.`id`
order by cast(sum(`c`.`fee`) as decimal(18, 2)) desc