def main():
    import sys
    import re
    import math
    from pyspark import SparkContext
    from pyspark.conf import SparkConf
    from cassandra.cluster import Cluster

    if len(sys.argv) > 1:
        query = sys.argv[1]
    else:
        print("Enter your search query: ")
        query = sys.stdin.readline().strip()
    
    print(f"Searching for: {query}")

    query_terms = re.findall(r'\b\w+\b', query.lower())
    
    if not query_terms:
        print("Empty query. Please provide search terms.")
        sys.exit(0)
    
    conf = SparkConf().setAppName("BM25 Search Engine")
    sc = SparkContext(conf=conf)
    
    try:
        cluster = Cluster(['cassandra-server'])
        session = cluster.connect('search_engine')
        
        rows = list(session.execute("SELECT COUNT(*) FROM documents"))
        total_docs = rows[0].count if rows else 0
        
        if total_docs == 0:
            print("No documents in the index.")
            sc.stop()
            cluster.shutdown()
            sys.exit(0)
        
        rows = list(session.execute("SELECT doc_id, doc_length FROM documents"))
        doc_lengths = {row.doc_id: row.doc_length for row in rows}
        
        avg_doc_length = sum(doc_lengths.values()) / total_docs if total_docs > 0 else 0
        
        k1 = 1.2
        b = 0.75
        
        doc_rdd = sc.parallelize(list(doc_lengths.keys()))
        
        def calculate_bm25_score(doc_id):
            total_score = 0
            
            for term in query_terms:
                rows = list(session.execute(
                    "SELECT term_freq FROM term_frequencies WHERE term = %s AND doc_id = %s",
                    (term, doc_id)
                ))
                tf = rows[0].term_freq if rows else 0
                
                if tf > 0:
                    rows = list(session.execute(
                        "SELECT doc_count FROM term_document_count WHERE term = %s",
                        (term,)
                    ))
                    df = rows[0].doc_count if rows else 0
                    
                    dl = doc_lengths[doc_id]
                    idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1.0)
                    tf_component = tf * (k1 + 1) / (tf + k1 * (1 - b + b * dl / avg_doc_length))
                    total_score += idf * tf_component
                    
            return total_score
        
        doc_scores = doc_rdd.map(lambda doc_id: (doc_id, calculate_bm25_score(doc_id)))
        top_docs = doc_scores.filter(lambda x: x[1] > 0).sortBy(lambda x: -x[1]).take(10)
        if top_docs:
            print("\nTop 10 search results:")
            for i, (doc_id, score) in enumerate(top_docs, 1):
                rows = list(session.execute(
                    "SELECT doc_title FROM documents WHERE doc_id = %s",
                    (doc_id,)
                ))
                title = rows[0].doc_title if rows else "Unknown"
                print(f"{i}. {doc_id}: {title} (Score: {score:.4f})")
        else:
            print("No matching documents found.")
        
    except Exception as e:
        print(f"Error during search: {str(e)}")
    
    finally:
        sc.stop()
        if 'cluster' in locals():
            cluster.shutdown()

if __name__ == "__main__":
    main()