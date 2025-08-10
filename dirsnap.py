#!/usr/bin/env python3
"""
dirsnap.py — Track diffs in a subdirectory between runs (no Git required).

Features
- Added / Removed / Modified / Renamed detection between runs
- Works without Git
- Verbosity levels, including terse "<LETTER> <path>" output
- Glob ignores (files/dirs)
- Persists state to a JSON file

Exit codes
- 0: no changes
- 1: changes detected
- 2: usage / runtime error
"""

from collections import deque
import pathlib
import sys
import argparse
import filesystem as fs
import metadata_manager as mm
    

def parser():
    parser = argparse.ArgumentParser(description="Directory Snapshot Tool")
    parser.add_argument("subdir", type=str, help="Subdirectory to scan")
    parser.add_argument("--metadata_path", '-mp', type=str, help="Path to metadata file", default='.dirsnap_state1.json')
    parser.add_argument("--metadata_filetype", '-mt', type=str, choices=['json', 'yaml'], default='json', help="Filetype for metadata. Currently only JSON supported")
    parser.add_argument("--ignore", '-i', action='append', default=[], help="Glob patterns to ignore")
    parser.add_argument("--probe_bytes", '-pb', type=int, default=int(2**20), help="Max bytes to probe from")
    parser.add_argument("--verbosity", '-v', type=int, choices=[0, 1], default=0, help="Verbosity level: 0=terse, 1=normal")
    parser.add_argument("--chunk_size", '-cs', type=int, default=int(2**20), help="Chunk size for file hashing")
    return parser

def main():
    args = parser().parse_args()
    
    try:
        files = fs.scan_subdir(pathlib.Path('.'), args.subdir)
        filtered_files = list(fs.filter_patterns(files, args.ignore))
        file_hashes = fs.hash_files(iter(filtered_files), args.chunk_size)
        file_classification = fs.classify_binary_or_text(iter(filtered_files), args.probe_bytes) 
        new_metadata = fs.merge_into_metadata(iter(filtered_files), file_classification, file_hashes)
        
        old_metadata = mm.load_metadata(pathlib.Path(args.metadata_path), args.metadata_filetype)
        diffs = mm.compute_diffs(old_metadata, new_metadata)
        changed = any(map(bool, (
            len(diffs["added"]),
            len(diffs["removed"]),
            len(diffs["moved"]),
            len(diffs["modified"]),
        )))

        mm.print_changes(diffs, args.verbosity, pathlib.Path(args.subdir))
        mm.save_metadata(new_metadata, pathlib.Path(args.metadata_path), args.metadata_filetype)
        
        consume = lambda it: deque(it, maxlen=0)
        
        consume((print("No changes were made"),) for _ in [None]) if (not changed and args.verbosity > 0) else None # TODO: CHANGE HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        sys.exit(1 if changed else 0)
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(e.with_traceback())
        sys.exit(2)
    
if __name__ == "__main__":
    main()