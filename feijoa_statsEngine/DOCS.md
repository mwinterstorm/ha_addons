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
| `mariadb_database` | Target database name. | `feijoa` |
| `timezone` | Timezone for logs and cron scheduling. | `Pacific/Auckland` |
| `log_level` | Add-on log verbosity (`debug`, `info`, `warning`, `error`). | `info` |

### MariaDB add-on integration

1. Install the [official MariaDB add-on](https://github.com/home-assistant/addons/tree/master/mariadb) in Home Assistant.
2. Create a database and user for the importer. Example SQL:

   ```sql
   CREATE DATABASE feijoa CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   CREATE USER 'feijoa'@'%' IDENTIFIED BY 'superSecret';
   GRANT ALL PRIVILEGES ON feijoa.* TO 'feijoa'@'%';
   FLUSH PRIVILEGES;
   ```

3. Enter the database credentials into the Feijoa add-on options.

### Automatic schema bootstrap

On first start the add-on runs any SQL files found in `/usr/local/share/feijoa/schema/` against the configured database. The scripts execute in lexical order, so prefix filenames (`000_tables.sql`, `010_views.sql`, etc.) to control the sequence. After a successful run a sentinel file is written to `/data/feijoa_schema_bootstrapped`; delete that file if you need to reapply the schema.

The bundled `000_tables.sql` creates the `users` and `contributions` tables. Replace or extend the files in the `schema` directory with your own schema and rebuild the add-on when they change.

### Importer implementation

The image currently includes a placeholder Python script at `/usr/local/share/feijoa/s3_contributions_to_sql.py`. Replace it with your actual importer implementation and update `/usr/local/share/feijoa/requirements.txt` with the required Python dependencies. Rebuild the add-on after making changes.

The importer runs with the following environment variables sourced from the add-on options:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- `S3_BUCKET`, `S3_PREFIX`
- `CHUNK_SIZE`, `USERS_CHUNK_SIZE`
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
- `TZ`

If `use_cron` is enabled, the importer runs on the specified schedule using [supercronic](https://github.com/aptible/supercronic). Otherwise the script executes once at start-up.
