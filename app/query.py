#!/usr/bin/env python3
import sys
import re
import math
import time
from pyspark import SparkContext, SparkConf
from functools import partial

def main():
    # Get the query
    if len(sys.argv) > 1:
        query = sys.argv[1]
    else:
        print("Enter your search query: ")
        query = sys.stdin.readline().strip()
    
    print(f"Searching for: {query}")
    
    # Parse the query into terms
    query_terms = re.findall(r'\b\w+\b', query.lower())
    
    if not query_terms:
        print("Empty query. Please provide search terms.")
        sys.exit(0)
    
    # Initialize Spark context
    conf = SparkConf().setAppName("BM25 Search Engine")
    sc = SparkContext(conf=conf)
    
    try:
        # Connect to Cassandra to get metadata, but don't distribute this connection
        from cassandra.cluster import Cluster
        
        # Connect to Cassandra with retry
        max_attempts = 10
        connected = False
        
        for attempt in range(max_attempts):
            try:
                # Make this less verbose
                if attempt == 0:
                    print("Connecting to Cassandra...")
                
                cluster = Cluster(['cassandra-server'])
                session = cluster.connect()
                
                # Check if keyspace exists
                keyspaces = list(session.execute("SELECT keyspace_name FROM system_schema.keyspaces"))
                keyspace_exists = any(ks.keyspace_name == 'search_engine' for ks in keyspaces)
                
                if not keyspace_exists:
                    print("Error: search_engine keyspace does not exist. Run indexer first.")
                    sc.stop()
                    cluster.shutdown()
                    sys.exit(1)
                
                session.set_keyspace('search_engine')
                connected = True
                break
                
            except Exception as e:
                if attempt == 0:
                    print(f"Retrying connection to Cassandra...")
                
                if 'cluster' in locals():
                    cluster.shutdown()
                
                if attempt < max_attempts - 1:
                    time.sleep(2)
                else:
                    print("Failed to connect to Cassandra after multiple attempts")
                    sc.stop()
                    sys.exit(1)
        
        if not connected:
            print("Could not connect to Cassandra")
            sc.stop()
            sys.exit(1)
            
        # Get total number of documents
        rows = list(session.execute("SELECT COUNT(*) FROM documents"))
        total_docs = rows[0].count if rows else 0
        
        if total_docs == 0:
            print("No documents in the index.")
            sc.stop()
            cluster.shutdown()
            sys.exit(0)
        
        print(f"Searching {total_docs} documents...")
        
        # Get document lengths and store them in a dictionary
        rows = list(session.execute("SELECT doc_id, doc_length FROM documents"))
        doc_lengths = {row.doc_id: row.doc_length for row in rows}
        
        # Calculate average document length
        avg_doc_length = sum(doc_lengths.values()) / total_docs if total_docs > 0 else 0
        
        # Get all documents - just IDs for distribution
        doc_ids = list(doc_lengths.keys())
        
        # Broadcast the query terms and other constants to avoid serialization
        bc_query_terms = sc.broadcast(query_terms)
        bc_total_docs = sc.broadcast(total_docs)
        bc_avg_doc_length = sc.broadcast(avg_doc_length)
        bc_doc_lengths = sc.broadcast(doc_lengths)
        
        # Constants for BM25
        k1 = 1.2  # Term frequency saturation parameter
        b = 0.75  # Document length normalization parameter
        
        # Function to calculate BM25 score for a document and query
        # This will be executed on the worker nodes
        def calculate_bm25_score(doc_id):
            from cassandra.cluster import Cluster
            
            # Create a new connection in the executor
            try:
                worker_cluster = Cluster(['cassandra-server'])
                worker_session = worker_cluster.connect('search_engine')
                
                # Get local copies of the broadcast variables
                query_terms = bc_query_terms.value
                total_docs = bc_total_docs.value
                avg_doc_length = bc_avg_doc_length.value
                doc_lengths = bc_doc_lengths.value
                
                total_score = 0
                
                for term in query_terms:
                    # Get term frequency in document
                    rows = list(worker_session.execute(
                        "SELECT term_freq FROM term_frequencies WHERE term = %s AND doc_id = %s",
                        (term, doc_id)
                    ))
                    tf = rows[0].term_freq if rows else 0
                    
                    if tf > 0:
                        # Get document frequency for term
                        rows = list(worker_session.execute(
                            "SELECT doc_count FROM term_document_count WHERE term = %s",
                            (term,)
                        ))
                        df = rows[0].doc_count if rows else 0
                        
                        # Document length
                        dl = doc_lengths[doc_id]
                        
                        # Calculate IDF component
                        idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1.0)
                        
                        # Calculate term frequency component with normalization
                        tf_component = tf * (k1 + 1) / (tf + k1 * (1 - b + b * dl / avg_doc_length))
                        
                        # Add score for this term
                        total_score += idf * tf_component
                
                return (doc_id, total_score)
            
            except Exception as e:
                return (doc_id, 0)
            finally:
                if 'worker_cluster' in locals():
                    worker_cluster.shutdown()
        
        # Display a message during processing
        print("Calculating BM25 scores...")
        
        # Calculate BM25 scores for all documents
        doc_rdd = sc.parallelize(doc_ids)
        doc_scores = doc_rdd.map(calculate_bm25_score)
        
        # Get top 10 documents
        top_docs = doc_scores.filter(lambda x: x[1] > 0).sortBy(lambda x: -x[1]).take(10)
        
        # Get titles for top documents using driver session
        if top_docs:
            print("\n" + "="*60)
            print(" "*20 + "TOP 10 SEARCH RESULTS")
            print("="*60)
            for i, (doc_id, score) in enumerate(top_docs, 1):
                rows = list(session.execute(
                    "SELECT doc_title FROM documents WHERE doc_id = %s",
                    (doc_id,)
                ))
                title = rows[0].doc_title if rows else "Unknown"
                print(f"{i}. {title} (ID: {doc_id}, Score: {score:.4f})")
            print("="*60)
        else:
            print("\n" + "="*60)
            print("No matching documents found.")
            print("="*60)
        
    except Exception as e:
        print(f"Error during search: {str(e)}")
    
    finally:
        # Clean up
        sc.stop()
        if 'cluster' in locals():
            cluster.shutdown()

if __name__ == "__main__":
    main()

