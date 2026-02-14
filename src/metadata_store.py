import json
import os
from datetime import datetime

class MetadataStore:
    def __init__(self, metadata_path="../data/system_metadata.json"):
        self.metadata_path = metadata_path
        self.registry = self._load_initial_registry()

    def _load_initial_registry(self):
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, 'r') as f:
                return json.load(f)
        return self._get_empty_state()

    def _get_empty_state(self):
        return {
            "system_info": {
                "version": "1.0",
                "project": "CS-432-Assignment-1",
                "created_at": datetime.now().isoformat()
            },
            "config": {
                "sql_table": "sensor_data",
                "mongo_collection": "extra_metrics"
            },
            "current_schema": {
                "sql_columns": {},
                "mirrored_fields": []
            },
            "run_history": []
        }

    def sync_from_pipeline(self, analysis_file, sql_fields, mirrored_fields, total_processed):
        if not os.path.exists(analysis_file):
            print(f"Warning: {analysis_file} not found. Metadata sync skipped.")
            return
        
        with open(analysis_file, 'r') as f:
            analysis_data = json.load(f)

        type_mapping = {}
        for field in analysis_data.get("fields", []):
            if field["field_name"] in sql_fields:
                type_mapping[field["field_name"]] = field["dominant_type"]

        run_time = datetime.now().isoformat()
        
        # 1. Update the 'Current' structure for backends to use immediately
        self.registry["current_schema"]["sql_columns"] = type_mapping
        self.registry["current_schema"]["mirrored_fields"] = list(mirrored_fields)
        
        # 2. Append to history for auditing
        history_entry = {
            "timestamp": run_time,
            "record_count": total_processed,
            "status": "success",
            "schema_snapshot": type_mapping
        }
        self.registry["run_history"].append(history_entry)
        
        self._save()

    def reset_metadata(self):
        self.registry = self._get_empty_state()
        self._save()
        print(f"Metadata at {self.metadata_path} has been reset.")

    def get_sql_schema(self):
        return self.registry["current_schema"]["sql_columns"]

    def _save(self):
        os.makedirs(os.path.dirname(self.metadata_path), exist_ok=True)
        with open(self.metadata_path, 'w') as f:
            json.dump(self.registry, f, indent=4)

if __name__ == "__main__":
    store = MetadataStore()
    # store.reset_metadata()
    print("Metadata Store active with historical tracking.")

# EXPLANATION OF LOGIC:
# 1. CURRENT_SCHEMA VS HISTORY: 'current_schema' always holds the latest column-to-type 
#    mappings for your SQL backend. 'run_history' acts as a permanent ledger of every 
#    ingestion attempt.
# 2. DATA TYPE PERSISTENCE: By storing 'dominant_type' in the history, you can track 
#    if a field changed from 'integer' to 'float' over the life of your project.
# 3. RESET METHOD: Like the TimestampManager, this allows you to clear out old project 
#    state if you want to start a completely new data simulation run.
# 4. ERROR PREVENTION: Adding a check for the existence of the analysis_file ensures 
#    the registry doesn't get corrupted with empty data if the analyzer failed.