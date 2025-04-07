#!/usr/bin/env python3
import sys

for line in sys.stdin:
    try:
        line = line.strip()
        
        if not line:
            continue
        
        parts = line.split('\t')
        if len(parts) < 4:
            continue
            
        term, doc_id, doc_title, term_freq = parts
        print(f"{doc_id}\t{term}\t{doc_title}\t{term_freq}")
        
    except Exception as e:
        print(f"Error processing line: {str(e)}", file=sys.stderr)
        continue