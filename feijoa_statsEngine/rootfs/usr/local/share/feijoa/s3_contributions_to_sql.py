#!/usr/bin/env python3

"""Feijoa Stats Engine importer placeholder.

Replace this module with the actual importer that pulls contribution data
from S3 and persists it into MariaDB. Environment variables are populated
via the add-on options and can be accessed with ``os.environ``.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone


def main() -> int:
    """Log configuration and exit.

    This placeholder helps validate that the add-on wiring works end-to-end.
    """
    now = datetime.now(timezone.utc).isoformat()
    print(f"[{now}] Feijoa importer placeholder running.")
    print("AWS region:", os.environ.get("AWS_REGION", "<unset>"))
    print("S3 bucket:", os.environ.get("S3_BUCKET", "<unset>"))
    print("MariaDB host:", os.environ.get("DB_HOST", "<unset>"))
    print("MariaDB database:", os.environ.get("DB_NAME", "<unset>"))
    print("Chunk size:", os.environ.get("CHUNK_SIZE", "<unset>"))
    print("Users chunk size:", os.environ.get("USERS_CHUNK_SIZE", "<unset>"))
    print("Cron run:", os.environ.get("CRON_ITERATION", "0"))
    print("Timezone:", os.environ.get("TZ", "<unset>"))
    print("Replace /usr/local/share/feijoa/s3_contributions_to_sql.py with")
    print("your actual importer implementation.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
