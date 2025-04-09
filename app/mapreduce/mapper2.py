#!/usr/bin/env python3
import sys

# Process each line from the previous reducer
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
            
        term, doc_id, doc_title, term_freq = parts
        
        # Rearrange the data to group by document
        print(f"{doc_id}\t{term}\t{doc_title}\t{term_freq}")
        
    except Exception as e:
        # Log error but continue processing
        print(f"Error processing line: {str(e)}", file=sys.stderr)
        continue
