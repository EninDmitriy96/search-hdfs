#!/bin/bash

chmod +x /app/mapreduce/*.py

apt-get update -qq && apt-get install -y netcat-openbsd

cp /app/mapreduce/cassandra_loader.py /app/mapreduce/
chmod +x /app/mapreduce/cassandra_loader.py
