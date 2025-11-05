CREATE TABLE `users` (
  `id_row` int(11) NOT NULL AUTO_INCREMENT,
  `id` varchar(255) NOT NULL,
  `full_name` varchar(511) GENERATED ALWAYS AS (concat(`first_name`,' ',`last_name`)) VIRTUAL,
  `first_name` text DEFAULT NULL,
  `last_name` text DEFAULT NULL,
  `date_joined` date DEFAULT NULL,
  `last_contrib` timestamp NULL DEFAULT NULL,
  `users_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `users_modified` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id_row`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `contributions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `timestamp` timestamp NULL DEFAULT NULL,
  `user_id` varchar(255) NOT NULL,
  `contribution` double DEFAULT NULL,
  `fee` double DEFAULT NULL,
  `total_with_fee` double GENERATED ALWAYS AS (`contribution` + `fee`) STORED,
  `fee_percentage` double GENERATED ALWAYS AS (`fee` / nullif(`contribution`,0) * 100) STORED,
  `transaction_count` int(11) DEFAULT NULL,
  `contributions_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `contributions_modified` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `contrib_date` date GENERATED ALWAYS AS (cast(`timestamp` as date)) STORED,
  PRIMARY KEY (`id`),
  KEY `user_id_idx` (`user_id`),
  UNIQUE KEY `uniq_user_day` (`user_id`,`contrib_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
