CREATE OR REPLACE VIEW `view_aml_over_350_per_week` AS

select `c1`.`user_id` AS `user_id`,
    cast(`c1`.`timestamp` as date) AS `period_end`,
    sum(`c2`.`transaction_count`) AS `weekly_transaction_count`
from (
        `contributions` `c1`
        join `contributions` `c2` on(
            `c1`.`user_id` = `c2`.`user_id`
            and `c2`.`timestamp` between `c1`.`timestamp` - interval 6 day
            and `c1`.`timestamp`
        )
    )
group by `c1`.`user_id`,
    `c1`.`timestamp`
having `weekly_transaction_count` > 350
order by `c1`.`user_id`,
    cast(`c1`.`timestamp` as date)