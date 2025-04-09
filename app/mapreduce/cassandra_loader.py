#!/usr/bin/env python3
import sys
import time
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel

def connect_with_retry(max_attempts=10, delay=5):
    for attempt in range(max_attempts):
        try:
            cluster = Cluster(['cassandra-server'])
            session = cluster.connect()
            return cluster, session
        except Exception as e:
            print(f"Attempt {attempt+1}/{max_attempts} to connect to Cassandra failed: {e}", file=sys.stderr)
            if attempt < max_attempts - 1:
                time.sleep(delay)
    
    raise Exception("Failed to connect to Cassandra after multiple attempts")

try:
    # Connect to Cassandra with retries
    cluster, session = connect_with_retry()
    
    # Create keyspace if it doesn't exist
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
    """)
    
    # Use the keyspace
    session.execute("USE search_engine")
    
    # Create tables for storing the index
    session.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            doc_id text PRIMARY KEY,
            doc_title text,
            doc_length int
        );
    """)
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS term_frequencies (
            term text,
            doc_id text,
            term_freq int,
            PRIMARY KEY (term, doc_id)
        );
    """)
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS term_document_count (
            term text PRIMARY KEY,
            doc_count int
        );
    """)
    
    # Read data from stdin and insert into Cassandra
    doc_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    term_freq_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    term_count_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    
    doc_count = 0
    term_freq_count = 0
    term_count_count = 0
    batch_size = 100
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split('\t')
        record_type = parts[0]
        
        if record_type == "DOC":
            _, doc_id, doc_title, doc_length = parts
            doc_batch.add(
                "INSERT INTO documents (doc_id, doc_title, doc_length) VALUES (%s, %s, %s)",
                (doc_id, doc_title, int(doc_length))
            )
            doc_count += 1
            
            if doc_count >= batch_size:
                session.execute(doc_batch)
                doc_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                doc_count = 0
                
        elif record_type == "TERM_FREQ":
            _, term, doc_id, term_freq = parts
            term_freq_batch.add(
                "INSERT INTO term_frequencies (term, doc_id, term_freq) VALUES (%s, %s, %s)",
                (term, doc_id, int(term_freq))
            )
            term_freq_count += 1
            
            if term_freq_count >= batch_size:
                session.execute(term_freq_batch)
                term_freq_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                term_freq_count = 0
                
        elif record_type == "TERM_DOC_COUNT":
            _, term, doc_count = parts
            term_count_batch.add(
                "INSERT INTO term_document_count (term, doc_count) VALUES (%s, %s)",
                (term, int(doc_count))
            )
            term_count_count += 1
            
            if term_count_count >= batch_size:
                session.execute(term_count_batch)
                term_count_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                term_count_count = 0
    
    # Execute any remaining batches
    if doc_count > 0:
        session.execute(doc_batch)
    
    if term_freq_count > 0:
        session.execute(term_freq_batch)
        
    if term_count_count > 0:
        session.execute(term_count_batch)
    
    print("Successfully loaded index data into Cassandra")
    
except Exception as e:
    print(f"Error: {str(e)}", file=sys.stderr)
    sys.exit(1)
finally:
    if 'cluster' in locals():
        cluster.shutdown()
