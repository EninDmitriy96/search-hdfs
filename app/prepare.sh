#!/bin/bash

# Make all Python scripts executable
chmod +x /app/mapreduce/*.py

# Install netcat for Cassandra check
apt-get update -qq && apt-get install -y netcat-openbsd

# Make sure Cassandra loader exists
cp /app/mapreduce/cassandra_loader.py /app/mapreduce/
chmod +x /app/mapreduce/cassandra_loader.py
