#!/usr/bin/with-contenv bash
set -euo pipefail

: "${S3_BUCKET:?S3_BUCKET is required (export it in the container env)}"
S3_PREFIX="${S3_PREFIX:-new_contributions/}"
CHUNK_SIZE="${CHUNK_SIZE:-1000}"
USERS_CHUNK_SIZE="${USERS_CHUNK_SIZE:-1000}"

echo "[import.sh] bucket=${S3_BUCKET} prefix=${S3_PREFIX} exec-db chunk=${CHUNK_SIZE} users-chunk=${USERS_CHUNK_SIZE}"

exec python3 /app/s3_contributions_to_sql.py \
  --bucket "${S3_BUCKET}" \
  --prefix "${S3_PREFIX}" \
  --exec-db \
  --chunk-size "${CHUNK_SIZE}" \
  --users-chunk-size "${USERS_CHUNK_SIZE}"