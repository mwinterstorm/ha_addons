#!/usr/bin/env python3
import argparse, csv, io, os, re
from decimal import Decimal
import boto3
import datetime

DB_ENABLED = False
try:
    import pymysql  # type: ignore
    DB_ENABLED = True
except Exception:
    pass

KEY_RE = re.compile(r"""^new_contributions/
         year=(?P<year>\d{4})/
         month=(?P<month>\d{2})/
         day=(?P<day>\d{2})/
         (?P<filename>[^/]+\.csv)$""", re.VERBOSE)

def parse_date_from_key(key: str) -> str:
    m = KEY_RE.match(key)
    if not m:
        raise ValueError(f"Bad key path: {key}")
    filename = m.group('filename')
    # Look for _YYYYMMDD_HHMMSS in the filename
    date_part_match = re.search(r"_(\d{8})_(\d{6})", filename)
    if not date_part_match:
        raise ValueError(f"Filename does not contain expected timestamp pattern: {filename}")
    yyyymmdd = date_part_match.group(1)
    hhmmss = date_part_match.group(2)
    # Parse to "YYYY-MM-DD HH:MM:SS"
    try:
        year = yyyymmdd[:4]
        month = yyyymmdd[4:6]
        day = yyyymmdd[6:8]
        hour = hhmmss[:2]
        minute = hhmmss[2:4]
        second = hhmmss[4:6]
        return f"{year}-{month}-{day} {hour}:{minute}:{second}"
    except Exception as e:
        raise ValueError(f"Error parsing date from filename '{filename}': {e}")

def dec(val: str) -> str: return str(Decimal(val))
def intify(val: str) -> int: return int(val)
def sql_escape(value: str) -> str: return value.replace("\\","\\\\").replace("'","''")

def chunked(seq, n):
    buf=[]; 
    for x in seq:
        buf.append(x)
        if len(buf)==n:
            yield buf; buf=[]
    if buf: yield buf

def sql_users_inserts(customer_ids, users_chunk_size):
    for chunk in chunked(customer_ids, users_chunk_size):
        values = ",\n  ".join(f"('{sql_escape(uid)}')" for uid in chunk)
        yield ("INSERT INTO users (id)\nVALUES\n  "
               + values +
               "\nON DUPLICATE KEY UPDATE id = id;")

def sql_contributions_inserts(date_str: str, rows, chunk_size):
    for chunk in chunked(rows, chunk_size):
        vals=[]
        for r in chunk:
            vals.append(
                f"('{date_str}', '{sql_escape(r['customer_id'])}', "
                f"{dec(r['amount'])}, {dec(r['fee_total'])}, {intify(r['transaction_count'])})"
            )
        yield ("INSERT INTO contributions "
               "(timestamp, user_id, contribution, fee, transaction_count)\nVALUES\n  "
               + ",\n  ".join(vals)
               + "\nON DUPLICATE KEY UPDATE timestamp = timestamp;")

def connect_db():
    cfg = {
        "host": os.environ.get("DB_HOST","localhost"),
        "port": int(os.environ.get("DB_PORT","3306")),
        "user": os.environ.get("DB_USER","root"),
        "password": os.environ.get("DB_PASSWORD",""),
        "database": os.environ["DB_NAME"],
        "charset": "utf8mb4", "autocommit": False,
        "cursorclass": pymysql.cursors.Cursor,
    }
    return pymysql.connect(**cfg)

def exec_sql(conn, statements):
    with conn.cursor() as cur:
        for stmt in statements:
            if stmt.strip():
                cur.execute(stmt)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="new_contributions/")
    ap.add_argument("--outdir", default="sql_out")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--region", default=None)
    ap.add_argument("--exec-db", action="store_true")
    ap.add_argument("--chunk-size", type=int, default=1000)
    ap.add_argument("--users-chunk-size", type=int, default=1000)
    args = ap.parse_args()

    if args.profile:
        boto3.setup_default_session(profile_name=args.profile, region_name=args.region)
    elif args.region:
        boto3.setup_default_session(region_name=args.region)

    if args.exec_db and not DB_ENABLED:
        raise SystemExit("PyMySQL not available (needed for --exec-db).")

    s3 = boto3.client("s3")

    per_day = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".csv") and KEY_RE.match(key):
                day_ts = parse_date_from_key(key)
                per_day.setdefault(day_ts, []).append(key)

    if not per_day:
        print("# No matching CSVs.")
        return

    if not args.exec_db:
        os.makedirs(args.outdir, exist_ok=True)

    conn = connect_db() if args.exec_db else None
    try:
        for day_ts, keys in sorted(per_day.items()):
            rows=[]
            for key in sorted(keys):
                obj = s3.get_object(Bucket=args.bucket, Key=key)
                text = obj["Body"].read().decode("utf-8-sig")
                rdr = csv.DictReader(io.StringIO(text))
                required = {"contribution_id","customer_id","amount","fee_total","transaction_count"}
                missing = required - set(rdr.fieldnames or [])
                if missing: raise ValueError(f"{key}: missing columns: {sorted(missing)}")
                for r in rdr:
                    _ = dec(r["amount"]); _ = dec(r["fee_total"]); _ = intify(r["transaction_count"])
                    if not r.get("customer_id"): raise ValueError(f"{key}: empty customer_id")
                    rows.append(r)
            if not rows: continue

            user_ids = sorted({r["customer_id"] for r in rows})
            statements=[]
            statements.extend(sql_users_inserts(user_ids, args.users_chunk_size))
            statements.extend(sql_contributions_inserts(day_ts, rows, args.chunk_size))

            if args.exec_db:
                exec_sql(conn, statements)
                conn.commit()
                print(f"[{datetime.datetime.now().isoformat()}] Imported {len(rows)} rows for {day_ts} in chunks "
                      f"(users≤{args.users_chunk_size}, rows≤{args.chunk_size})")
            else:
                out = os.path.join(args.outdir, f"contributions_{day_ts[:10].replace('-','')}.sql")
                with open(out, "w", encoding="utf-8") as f:
                    f.write("-- generated\n")
                    for s in statements: f.write(s+"\n")
                print(f"Wrote {out} (rows: {len(rows)})")
    except Exception:
        if conn: conn.rollback()
        raise
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()