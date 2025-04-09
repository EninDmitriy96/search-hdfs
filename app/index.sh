#!/bin/bash
echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

echo "Input file is:"
echo $1

# Use default path if no input is provided
INPUT_PATH=${1:-/index/data}

# Make sure the Python scripts are executable
chmod +x /app/mapreduce/*.py

# Create temporary directories for intermediate results
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

# Load the output into Cassandra
echo "Loading data into Cassandra..."
hdfs dfs -cat /tmp/index/output2/part-* | python3 /app/mapreduce/cassandra_loader.py

echo "Indexing completed successfully!"
