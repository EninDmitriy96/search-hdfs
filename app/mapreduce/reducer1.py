
#!/usr/bin/env python3
import sys
from collections import defaultdict

term_doc_counts = defaultdict(lambda: defaultdict(int))
doc_titles = {}

for line in sys.stdin:
    line = line.strip()
    
    try:
        term, doc_id, doc_title, count = line.split('\t')
        count = int(count)
        term_doc_counts[term][doc_id] += count
        doc_titles[doc_id] = doc_title
        
    except ValueError:
        continue

for term in term_doc_counts:
    for doc_id in term_doc_counts[term]:
        count = term_doc_counts[term][doc_id]
        title = doc_titles[doc_id]
        print(f"{term}\t{doc_id}\t{title}\t{count}")
