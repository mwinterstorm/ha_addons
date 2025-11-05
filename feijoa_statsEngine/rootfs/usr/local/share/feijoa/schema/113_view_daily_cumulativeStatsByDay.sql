CREATE OR REPLACE VIEW `view_daily_cumulative_stats_by_day` AS

with ContribNZ as (
    select cast(
            convert_tz(`c`.`timestamp`, 'UTC', 'Pacific/Auckland') as date
        ) AS `local_day`,
        coalesce(`c`.`contribution`, 0) AS `contribution`,
        coalesce(`c`.`fee`, 0) AS `fee`,
        coalesce(`c`.`transaction_count`, 0) AS `tx`
    from `contributions` `c`
),
daily_summary as (
    select `ContribNZ`.`local_day` AS `date`,
        sum(`ContribNZ`.`contribution`) AS `daily_contributions`,
        sum(`ContribNZ`.`fee`) AS `daily_fees`,
        sum(`ContribNZ`.`tx`) AS `daily_transactions`
    from `ContribNZ`
    group by `ContribNZ`.`local_day`
)
select `ds`.`date` AS `date`,
    cast(`ds`.`daily_contributions` as decimal(18, 2)) AS `daily_contributions`,
    cast(`ds`.`daily_fees` as decimal(18, 2)) AS `daily_fees`,
    cast(`ds`.`daily_transactions` as decimal(18, 0)) AS `daily_transactions`,
    cast(
        sum(`ds`.`daily_contributions`) over (
            order by `ds`.`date` rows between unbounded preceding and current row
        ) as decimal(18, 2)
    ) AS `cumulative_contributions`,
    cast(
        sum(`ds`.`daily_contributions`) over (
            order by `ds`.`date` rows between unbounded preceding and current row
        ) + 1872.61 as decimal(18, 2)
    ) AS `adj_cumulative_contributions`,
    cast(
        sum(`ds`.`daily_fees`) over (
            order by `ds`.`date` rows between unbounded preceding and current row
        ) as decimal(18, 2)
    ) AS `cumulative_fees`,
    cast(
        sum(`ds`.`daily_fees`) over (
            order by `ds`.`date` rows between unbounded preceding and current row
        ) + 187.26 as decimal(18, 2)
    ) AS `adj_cumulative_fees`,
    cast(
        sum(`ds`.`daily_transactions`) over (
            order by `ds`.`date` rows between unbounded preceding and current row
        ) as decimal(18, 0)
    ) AS `cumulative_transactions`,
    cast(
        sum(`ds`.`daily_transactions`) over (
            order by `ds`.`date` rows between unbounded preceding and current row
        ) + 3937 as decimal(18, 0)
    ) AS `adj_cumulative_transactions`
from `daily_summary` `ds`