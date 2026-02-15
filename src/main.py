import sys
import os
import json
from client import run_data_collection
from normalizer import run_field_normalization
from analyzer import run_data_analysis
from classifier import run_classification
from classification_visualiser import run_visualization
# Import both functions from our new router_logger
from router_logger import processAndSplit, processBatch 

def run_initialization():
    print(">>> Starting System Initialization (Training Phase)...")
    
    # 1. Collect 1000 records to learn the schema
    print("\n--- Step 1: Data Collection ---")
    run_data_collection()
    
    # 2. Normalize the data (flatten structure)
    print("\n--- Step 2: Normalization ---")
    run_field_normalization()
    
    # 3. Analyze fields (calculate stats like sparsity, cardinality)
    print("\n--- Step 3: Data Analysis ---")
    run_data_analysis()
    
    # 4. Classify fields (Generate field_metadata.json)
    print("\n--- Step 4: Classification ---")
    run_classification()
    
    # 5. Visualize the decision boundary
    print("\n--- Step 5: Visualization ---")
    run_visualization()
    
    # 6. Route the initial training data
    #    We reuse the router logic to put the 1000 training records into the DBs
    print("\n--- Step 6: Routing Initial Data ---")
    
    # Robustly find the raw_data.json file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_data_path = os.path.join(script_dir, '..', 'data', 'raw_data.json')
    
    processBatch(raw_data_path)
    
    print("\n>>> Initialization Complete. Rules generated and data routed.")

def run_router(count):
    print(f"\n>>> Starting Router for {count} records...")
    print("Using rules from 'field_metadata.json' to route data.")
    processAndSplit(count)

def clear_logs():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(script_dir, '..', 'data', 'router_logger.txt')
    
    with open(log_path, 'w') as f:
        pass # Truncate file
    print(">>> Logs cleared.")

def clear_records():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(script_dir, '..', 'data', 'sql_records.json')
    mongo_path = os.path.join(script_dir, '..', 'data', 'mongo_records.json')
    
    # Initialize with empty lists
    with open(sql_path, 'w') as f:
        json.dump([], f)
        
    with open(mongo_path, 'w') as f:
        json.dump([], f)
        
    print(">>> SQL and Mongo records cleared.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py initialise       -> Runs pipeline & routes initial batch")
        print("  python main.py router <count>   -> Routes <count> new records")
        print("  python main.py clearLogs        -> Clears router_logger.txt")
        print("  python main.py clearRecords     -> Clears sql and mongo jsons")
        sys.exit(1)

    command = sys.argv[1]

    if command == "initialise":
        run_initialization()

    elif command == "router":
        count = 10
        if len(sys.argv) > 2:
            try:
                count = int(sys.argv[2])
            except ValueError:
                print("Invalid count provided. Defaulting to 10.")
        run_router(count)
        
    elif command == "clearLogs":
        clear_logs()
        
    elif command == "clearRecords":
        clear_records()

    else:
        print(f"Unknown command: {command}")