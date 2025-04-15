# big-data-assignment2-2025

# How to run
## Step 1: Install prerequisites
- Docker
- Docker compose
## Step 2: Run the command
```bash
docker compose up -d 
```
This will create 3 containers, a master node and a worker node for Hadoop, and Cassandra server. The master node will run the script `app/app.sh` as an entrypoint.
```
docker exec -it cluster-master bash -c
```
Within the container the engine may be used as follows:
```
./app.sh
./search.sh “search query”
```
