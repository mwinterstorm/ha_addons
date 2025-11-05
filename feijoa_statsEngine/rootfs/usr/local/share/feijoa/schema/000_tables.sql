-- Feijoa Stats Engine base tables.

CREATE TABLE IF NOT EXISTS `users` (
  `id_row` INT NOT NULL AUTO_INCREMENT,
  `id` VARCHAR(255) NOT NULL,
  `full_name` VARCHAR(511) GENERATED ALWAYS AS (CONCAT(`first_name`, ' ', `last_name`)) VIRTUAL,
  `first_name` TEXT DEFAULT NULL,
  `last_name` TEXT DEFAULT NULL,
  `date_joined` DATE DEFAULT NULL,
  `last_contrib` TIMESTAMP NULL DEFAULT NULL,
  `users_created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  `users_modified` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),
  PRIMARY KEY (`id_row`),
  UNIQUE KEY `uniq_user_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `contributions` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `timestamp` TIMESTAMP NULL DEFAULT NULL,
  `user_id` VARCHAR(255) NOT NULL,
  `contribution` DOUBLE DEFAULT NULL,
  `fee` DOUBLE DEFAULT NULL,
  `tax` DOUBLE DEFAULT NULL,
  `transaction_count` INT DEFAULT NULL,
  `source_file` VARCHAR(255) DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (`id`),
  KEY `idx_contributions_user` (`user_id`),
  CONSTRAINT `fk_contrib_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
