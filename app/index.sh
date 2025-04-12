#!/bin/bash
echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

echo "Input file is:"
echo $1

echo "Copying data to HDFS..."
hdfs dfs -rm -r -f /index/data/*

if [ -d "/app/data" ]; then
    mkdir -p /tmp/data_clean
    file_count=0
    for file in /app/data/*; do
        if [ -f "$file" ]; then
            filename=$(basename "$file")
            clean_filename=$(echo "$filename" | tr -cd 'A-Za-z0-9_.-')
            cp "$file" "/tmp/data_clean/$clean_filename"
            file_count=$((file_count+1))
        fi
    done
    
    hdfs dfs -put -f /tmp/data_clean/* /index/data/
    echo "Data copied to HDFS successfully: $file_count files"
    
    rm -rf /tmp/data_clean
else
    echo "No data directory found, creating sample data"
    mkdir -p /app/data
    echo "Sample text for document 1" > /app/data/1_Sample_Document.txt
    hdfs dfs -put -f /app/data/* /index/data/
fi

chmod +x /app/*.sh
chmod +x /app/mapreduce/*.py

echo "Waiting for Cassandra to be ready..."
for i in {1..30}; do
    if timeout 1 bash -c "</dev/tcp/cassandra-server/9042"; then
        echo "Cassandra is ready"
        break
    fi
    echo "Waiting for Cassandra ($i/30)..."
    sleep 2
done

echo "Cleaning existing Cassandra data..."
python3 -c "
from cassandra.cluster import Cluster
try:
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    session.execute(\"DROP KEYSPACE IF EXISTS search_engine\")
    print('Keyspace search_engine dropped successfully')
    cluster.shutdown()
except Exception as e:
    print(f'Error dropping keyspace: {e}')
"


INPUT_PATH=${1:-/index/data}

chmod +x /app/mapreduce/*.py

hdfs dfs -mkdir -p /tmp/index
hdfs dfs -rm -r -f /tmp/index/output1
hdfs dfs -rm -r -f /tmp/index/output2

echo "Running first MapReduce job..."
# First MapReduce job: Extract terms from documents
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "$INPUT_PATH" \
  -output /tmp/index/output1

echo "Running second MapReduce job..."
# Second MapReduce job: Calculate document statistics without Cassandra integration
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -input /tmp/index/output1 \
  -output /tmp/index/output2

echo "Loading data into Cassandra..."
hdfs dfs -cat /tmp/index/output2/part-* | python3 /app/mapreduce/cassandra_loader.py

echo "Indexing completed successfully!"
