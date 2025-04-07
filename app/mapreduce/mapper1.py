#!/usr/bin/env python3
import sys
import re
import os

for line in sys.stdin:
    try:
        line = line.strip()
        
        if not line:
            continue
        
        file_path = os.environ.get('mapreduce_map_input_file') or os.environ.get('map_input_file')
        
        doc_id = "unknown"
        doc_title = "unknown"
        
        if file_path:
            filename = os.path.basename(file_path)
            
            if '_A_' in filename:
                parts = filename.split('_A_')
                if len(parts) >= 2:
                    doc_id = parts[0]
                    doc_title = parts[1].replace('.txt', '')
        
        words = re.findall(r'\b\w+\b', line.lower())
        
        for word in words:
            if len(word) > 1:
                print(f"{word}\t{doc_id}\t{doc_title}\t1")
    
    except Exception as e:
        print(f"Error processing line: {str(e)}", file=sys.stderr)
        continue