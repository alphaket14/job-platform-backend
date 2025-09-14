#!/usr/bin/env python3
"""
Example script showing how to use the feeds table copier.
This demonstrates different ways to copy the feeds table between databases.
"""

import os
import sys
from copy_feeds_table import FeedsTableCopier

def main():
    """Example usage of the FeedsTableCopier."""
    
    # Example 1: Copy from development to production database
    print("=== Example 1: Development to Production Copy ===")
    
    # Connection strings (replace with your actual connection strings)
    source_conn = "postgresql://user:password@dev-server:5432/dev_db"
    dest_conn = "postgresql://user:password@prod-server:5432/prod_db"
    
    # Initialize copier
    copier = FeedsTableCopier(source_conn, dest_conn)
    
    # Test connections first
    if copier.test_connections():
        print("✅ Connections successful!")
        
        # Compare schemas
        if copier.compare_schemas():
            print("✅ Schemas compatible!")
            
            # Copy data with overwrite (truncates destination first)
            results = copier.copy_feeds_data(batch_size=50, overwrite=True)
            
            if results['success']:
                print(f"✅ Copy successful! Copied {results['rows_copied']} rows")
                
                # Verify the copy
                verification = copier.verify_copy()
                print(f"Verification: {verification}")
            else:
                print(f"❌ Copy failed: {results['errors']}")
        else:
            print("❌ Schema comparison failed!")
    else:
        print("❌ Connection test failed!")
    
    print("\n" + "="*60 + "\n")
    
    # Example 2: Test mode only
    print("=== Example 2: Test Mode Only ===")
    
    # This only tests connections and compares schemas without copying
    copier2 = FeedsTableCopier(source_conn, dest_conn)
    
    if copier2.test_connections():
        print("✅ Source and destination connections work!")
        
        if copier2.compare_schemas():
            print("✅ Schemas are compatible!")
            print("Ready for copy operation!")
        else:
            print("❌ Schemas are not compatible!")
    else:
        print("❌ Connection test failed!")
    
    print("\n" + "="*60 + "\n")
    
    # Example 3: Verify existing copy
    print("=== Example 3: Verify Existing Copy ===")
    
    copier3 = FeedsTableCopier(source_conn, dest_conn)
    verification = copier3.verify_copy()
    
    if 'error' not in verification:
        print(f"Source count: {verification['source_count']}")
        print(f"Destination count: {verification['dest_count']}")
        print(f"Counts match: {verification['counts_match']}")
        print(f"Samples match: {verification['samples_match']}")
        
        if verification['counts_match'] and verification['samples_match']:
            print("✅ Copy verification successful!")
        else:
            print("⚠️  Copy verification shows discrepancies!")
    else:
        print(f"❌ Verification failed: {verification['error']}")


if __name__ == "__main__":
    # Note: This example uses placeholder connection strings
    # Replace them with your actual database connection strings
    print("⚠️  This is an example script with placeholder connection strings.")
    print("Please update the connection strings with your actual database credentials.")
    print()
    
    # Uncomment the line below to run the example
    # main()
    
    print("Example script ready. Update connection strings and uncomment main() to run.")