#!/usr/bin/env python3
import sys
import re
import os

for line in sys.stdin:
    try:
        line = line.strip()
        
        if not line:
            continue
        
        filepath = os.environ.get('map_input_file') or os.environ.get('mapreduce_map_input_file')
        
        doc_id = "unknown"
        doc_title = "unknown"
        
        if filepath:
            filename = os.path.basename(filepath)
            
            if '_' in filename:
                parts = filename.split('_')
                if len(parts) >= 2:
                    doc_id = parts[0]
                    doc_title = '_'.join(parts[1:]).replace('.txt', '')
                    print(f"DEBUG: Parsed filename={filename}, doc_id={doc_id}, doc_title={doc_title}", file=sys.stderr)
        
        words = re.findall(r'\b\w+\b', line.lower())
        
        for word in words:
            if len(word) > 1:
                print(f"{word}\t{doc_id}\t{doc_title}\t1")
    
    except Exception as e:
        print(f"ERROR in mapper: {str(e)}, file: {filepath if 'filepath' in locals() else 'unknown'}", file=sys.stderr)
        continue
