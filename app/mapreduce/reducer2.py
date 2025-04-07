#!/usr/bin/env python3
import sys
from collections import defaultdict
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel

# Document statistics
doc_stats = defaultdict(lambda: {'title': '', 'length': 0, 'terms': defaultdict(int)})
term_doc_count = defaultdict(set)  # Which documents contain each term

# Process each line from the mapper
for line in sys.stdin:
    try:
        line = line.strip()
        
        # Skip empty lines
        if not line:
            continue
        
        # Parse the input
        parts = line.split('\t')
        if len(parts) < 4:
            continue
            
        doc_id, term, doc_title, term_freq = parts
        term_freq = int(term_freq)
        
        # Update document statistics
        doc_stats[doc_id]['title'] = doc_title
        doc_stats[doc_id]['length'] += term_freq
        doc_stats[doc_id]['terms'][term] = term_freq
        
        # Track which documents contain this term
        term_doc_count[term].add(doc_id)
        
    except Exception as e:
        # Log error but continue processing
        print(f"Error processing line: {str(e)}", file=sys.stderr)
        continue

# Connect to Cassandra and store the data
try:
    # Connect to Cassandra
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    
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
    
    # Insert document data
    for doc_id, stats in doc_stats.items():
        # Insert document info
        session.execute(
            "INSERT INTO documents (doc_id, doc_title, doc_length) VALUES (%s, %s, %s)",
            (doc_id, stats['title'], stats['length'])
        )
        
        # Insert term frequencies for this document
        for term, freq in stats['terms'].items():
            session.execute(
                "INSERT INTO term_frequencies (term, doc_id, term_freq) VALUES (%s, %s, %s)",
                (term, doc_id, freq)
            )
    
    # Insert term document counts
    for term, docs in term_doc_count.items():
        session.execute(
            "INSERT INTO term_document_count (term, doc_count) VALUES (%s, %s)",
            (term, len(docs))
        )
    
    # Close the Cassandra connection
    cluster.shutdown()
    
except Exception as e:
    print(f"Error with Cassandra: {str(e)}", file=sys.stderr)