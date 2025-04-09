#!/bin/bash
set -e

echo "Starting search engine application..."

service ssh restart 

bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt  

rm -f .venv.tar.gz

venv-pack -o .venv.tar.gz

hdfs dfs -mkdir -p /index/data
hdfs dfs -mkdir -p /user/root
hdfs dfs -chmod -R 777 /user/root

echo "Copying data to HDFS..."
if [ -d "/app/data" ]; then
    find /app/data -type f -print0 | while IFS= read -r -d '' file; do
        hdfs dfs -put -f "$file" /index/data/ || echo "Warning: Failed to copy $file"
    done
    echo "Data copy process completed (some files might have been skipped)"
else
    echo "No data directory found, creating sample data"
    mkdir -p /app/data
    echo "Sample text for document 1" > /app/data/1_A_Sample_Document.txt
    hdfs dfs -put -f /app/data/* /index/data/
fi

chmod +x /app/*.sh
chmod +x /app/mapreduce/*.py

echo "Running indexer..."
bash index.sh /index/data

echo "Running sample search..."
bash search.sh "this is a query!"