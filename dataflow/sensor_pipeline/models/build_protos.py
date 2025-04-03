#!/usr/bin/env python3
"""
Build script to compile protocol buffer definitions into Python classes.
Run this script whenever the .proto files are updated.
"""

import os
import subprocess
import sys
from pathlib import Path

def build_protos():
    """Compile all .proto files in the current directory to Python."""
    
    script_dir = Path(__file__).parent.absolute()
    proto_files = list(script_dir.glob('*.proto'))
    
    if not proto_files:
        print("No .proto files found in", script_dir)
        return
    
    for proto_file in proto_files:
        print(f"Compiling {proto_file}...")
        
        try:
            result = subprocess.run([
                "protoc",
                f"--python_out={script_dir}",
                f"--proto_path={script_dir}",
                f"--proto_path={os.path.expanduser('~/.local/include')}", 
                f"--proto_path=/usr/local/include", 
                str(proto_file)
            ], check=True, capture_output=True)
            
            print(f"Successfully compiled {proto_file}")
            
        except subprocess.CalledProcessError as e:
            print(f"Error compiling {proto_file}:")
            print(e.stderr.decode())
            return 1
        except FileNotFoundError:
            print("Error: 'protoc' compiler not found.")
            return 1
    
    print("All proto files compiled successfully.")
    return 0

if __name__ == "__main__":
    sys.exit(build_protos()) 