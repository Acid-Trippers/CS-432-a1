import sys
from client import run_data_collection
from normalizer import run_field_normalization
from analyzer import run_data_analysis
from classifier import run_classification
from classification_visualiser import run_visualization
from router_logger import processAndSplit  # Importing the router logic

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
    
    # 4. Classify fields (Decide SQL vs Mongo)
    print("\n--- Step 4: Classification ---")
    run_classification()
    
    # 5. Visualize the decision boundary
    print("\n--- Step 5: Visualization ---")
    run_visualization()
    
    print("\n>>> Initialization Complete. 'field_metadata.json' is ready.")

def run_router(count):
    print(f"\n>>> Starting Router for {count} records...")
    print("Using rules from 'classification_results.json' to route data.")
    processAndSplit(count)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py initialise       -> Runs the full training pipeline (1000 records)")
        print("  python main.py router <count>   -> Routes <count> new records using established rules")
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "initialise":
        run_initialization()

    elif command == "router":
        # Default to 10 records if count not provided
        count = 10
        if len(sys.argv) > 2:
            try:
                count = int(sys.argv[2])
            except ValueError:
                print("Invalid count provided. Defaulting to 10.")
        
        run_router(count)

    else:
        print(f"Unknown command: {command}")
        print("Available commands: 'initialise', 'router'")