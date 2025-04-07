#!/bin/bash
set -e

echo "Starting search engine application..."

# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install required packages
pip install -r requirements.txt  

# Remove existing package if present to avoid VenvPackError
rm -f .venv.tar.gz

# Package the virtual env for distribution to executors
venv-pack -o .venv.tar.gz

# Prepare HDFS directories
hdfs dfs -mkdir -p /index/data
hdfs dfs -mkdir -p /user/root
hdfs dfs -chmod -R 777 /user/root

echo "Copying data to HDFS..."
# Modified copy command to handle special characters
if [ -d "/app/data" ]; then
    # Use find to handle special characters in filenames
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

# Make sure all scripts are executable
chmod +x /app/*.sh
chmod +x /app/mapreduce/*.py

# Run the indexer
echo "Running indexer..."
bash index.sh /index/data

# Run a sample search
echo "Running sample search..."
bash search.sh "this is a query!"