CREATE OR REPLACE VIEW `view_aml_over 8000` AS

with ContributionAnniversaries as (
    select `c`.`user_id` AS `user_id`,
        `c`.`contribution` AS `contribution`,
        `u`.`full_name` AS `full_name`,
        `u`.`date_joined` AS `date_joined`,
        floor(
            (
                to_days(`c`.`timestamp`) - to_days(`u`.`date_joined`)
            ) / 365.25
        ) AS `anniversary_year`
    from (
            `contributions` `c`
            join `users` `u` on(`c`.`user_id` = `u`.`id`)
        )
    where `u`.`date_joined` is not null
)
select `ContributionAnniversaries`.`user_id` AS `user_id`,
    `ContributionAnniversaries`.`full_name` AS `full_name`,
    `ContributionAnniversaries`.`date_joined` + interval `ContributionAnniversaries`.`anniversary_year` year AS `period_start`,
    `ContributionAnniversaries`.`date_joined` + interval `ContributionAnniversaries`.`anniversary_year` + 1 year AS `period_end`,
    sum(`ContributionAnniversaries`.`contribution`) AS `total_contribution`
from `ContributionAnniversaries`
group by `ContributionAnniversaries`.`user_id`,
    `ContributionAnniversaries`.`full_name`,
    `ContributionAnniversaries`.`date_joined`,
    `ContributionAnniversaries`.`anniversary_year`
having `total_contribution` > 8000
order by `ContributionAnniversaries`.`user_id`,
    `ContributionAnniversaries`.`date_joined` + interval `ContributionAnniversaries`.`anniversary_year` year