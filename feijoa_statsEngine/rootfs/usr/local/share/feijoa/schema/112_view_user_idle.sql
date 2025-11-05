CREATE OR REPLACE VIEW `view_user_idle_users` AS

select `u`.`id` AS `id`,
    `u`.`full_name` AS `full_name`,
    max(`c`.`timestamp`) AS `last_contribution_date`,
    to_days(curdate()) - to_days(max(`c`.`timestamp`)) AS `days_since_last_contribution`
from (
        `users` `u`
        left join `contributions` `c` on(`u`.`id` = `c`.`user_id`)
    )
group by `u`.`id`
having `days_since_last_contribution` > 14
order by to_days(curdate()) - to_days(max(`c`.`timestamp`)) desc