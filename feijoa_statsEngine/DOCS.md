# Feijoa Stats Engine Importer add-on

## Configuration

| Option | Description | Default |
| --- | --- | --- |
| `aws_access_key_id` | AWS access key used to read the S3 bucket. | `""` |
| `aws_secret_access_key` | AWS secret key. | `""` |
| `aws_region` | AWS region that hosts the S3 bucket. | `ap-southeast-2` |
| `s3_bucket` | Name of the S3 bucket containing contribution exports. | `""` |
| `s3_prefix` | Optional S3 prefix to scope the import. | `new_contributions/` |
| `chunk_size` | Batch size when writing contributions. | `1000` |
| `users_chunk_size` | Batch size when writing user records. | `1000` |
| `use_cron` | Run the importer on a schedule using supercronic. | `false` |
| `schedule` | Cron expression used when `use_cron` is `true`. | `0 3 * * *` |
| `mariadb_host` | Hostname of the MariaDB add-on (typically `core-mariadb`). | `core-mariadb` |
| `mariadb_port` | MariaDB port. | `3306` |
| `mariadb_user` | MariaDB user with permissions to the target database. | `""` |
| `mariadb_password` | Password for `mariadb_user`. | `""` |
| `mariadb_database` | Target database name. | `feijoa_contribution_stats` |
| `timezone` | Timezone for logs and cron scheduling. | `Pacific/Auckland` |
| `log_level` | Add-on log verbosity (`debug`, `info`, `warning`, `error`). | `info` |

### MariaDB add-on integration

1. Install the [official MariaDB add-on](https://github.com/home-assistant/addons/tree/master/mariadb) in Home Assistant.
2. Create a database and user for the importer. Example SQL:

   ```sql
   CREATE DATABASE your_database_name CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   CREATE USER 'your_user_name'@'%' IDENTIFIED BY 'your_secret_password';
   GRANT ALL PRIVILEGES ON your_database_name.* TO 'your_user_name'@'%'; -- Note: ALL PRIVILEGES is recommended. At a minimum, the user needs SELECT, INSERT, UPDATE, DELETE, and CREATE VIEW permissions.
   FLUSH PRIVILEGES;
   ```

3. Enter the database credentials into the Feijoa add-on options.

4. db will be initialised on first run. If you need to re initialise, delete the file in the addon_config directory.