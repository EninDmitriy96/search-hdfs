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

# Install required Python packages
pip install -r requirements.txt

rm -f .venv.tar.gz

venv-pack -o .venv.tar.gz

hdfs dfs -mkdir -p /index/data
hdfs dfs -mkdir -p /user/root
hdfs dfs -chmod -R 777 /user/root

# Run the indexer
echo "Running indexer..."
bash index.sh /index/data

# Run the ranker
echo "Running sample search..."
bash search.sh "this is a query!"
EOF
chmod +x /app/app.sh



