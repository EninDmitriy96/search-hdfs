#!/usr/bin/env python3
import sys
from collections import defaultdict

doc_stats = defaultdict(lambda: {'title': '', 'length': 0, 'terms': defaultdict(int)})
term_doc_count = defaultdict(set)

for line in sys.stdin:
    try:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split('\t')
        if len(parts) < 4:
            continue
            
        doc_id, term, doc_title, term_freq = parts
        term_freq = int(term_freq)
        
        doc_stats[doc_id]['title'] = doc_title
        doc_stats[doc_id]['length'] += term_freq
        doc_stats[doc_id]['terms'][term] = term_freq
        
        term_doc_count[term].add(doc_id)
        
    except Exception as e:
        print(f"Error processing line: {str(e)}", file=sys.stderr)
        continue

for doc_id, stats in doc_stats.items():
    print(f"DOC\t{doc_id}\t{stats['title']}\t{stats['length']}")

for doc_id, stats in doc_stats.items():
    for term, freq in stats['terms'].items():
        print(f"TERM_FREQ\t{term}\t{doc_id}\t{freq}")

for term, docs in term_doc_count.items():
    print(f"TERM_DOC_COUNT\t{term}\t{len(docs)}")
