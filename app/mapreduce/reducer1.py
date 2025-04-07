
#!/usr/bin/env python3
import sys
from collections import defaultdict

# Dictionary to store term frequencies per document
term_doc_counts = defaultdict(lambda: defaultdict(int))
doc_titles = {}

# Process each line from the mapper
for line in sys.stdin:
    line = line.strip()
    
    try:
        term, doc_id, doc_title, count = line.split('\t')
        count = int(count)
        
        # Add to term frequency counts
        term_doc_counts[term][doc_id] += count
        
        # Store document title
        doc_titles[doc_id] = doc_title
        
    except ValueError:
        # Skip lines that don't have the expected format
        continue

# Output the term frequencies for each document
for term in term_doc_counts:
    for doc_id in term_doc_counts[term]:
        count = term_doc_counts[term][doc_id]
        title = doc_titles[doc_id]
        print(f"{term}\t{doc_id}\t{title}\t{count}")
