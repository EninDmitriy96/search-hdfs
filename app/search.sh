#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"

QUERY="$1"

echo "Searching for: $QUERY"

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
  --master yarn \
  --archives /app/.venv.tar.gz#.venv \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
  /app/query.py "$QUERY"