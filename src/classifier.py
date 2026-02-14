import json
import os
from metadata_store import MetadataStore
from timestamp_manager import TimestampManager

class HybridClassifier:
    def __init__(self, analysis_file: str):
        # Thresholds for SQL inclusion: must appear in 50% of records and be 95% type-stable
        self.sql_threshold = 0.5
        self.stability_threshold = 0.95
        # Mandatory fields that must exist in both databases for relationship linking
        self.mirrored_fields = {"username", "timestamp", "device_id"}
        self.analysis_file = analysis_file

    def generate_backend_decision(self, output_file="../data/backend_decision.json"):
        """
        Reads analysis results and categorizes every field into SQL, Mongo, or Both.
        Outputs a manifest that backends will use to filter raw data.
        """
        if not os.path.exists(self.analysis_file):
            return None
        
        with open(self.analysis_file, 'r') as f:
            analysis = json.load(f)
        
        # Manifest structure to guide the backends
        decision_manifest = {
            "sql_only": [],    # Fields for PostgreSQL only
            "mongo_only": [],  # Fields for MongoDB only
            "both": []         # Overlapping fields for linking
        }
        
        sql_list = []
        
        for field in analysis.get("fields", []):
            name = field["field_name"]
            freq = field["frequency"]
            stability = field["type_stability"]
            is_nested = field["is_nested"]
            
            # Logic: SQL needs flat, frequent, and type-stable data
            is_sql_stable = (freq > self.sql_threshold and 
                             stability > self.stability_threshold and 
                             not is_nested)
            
            # Metrics metadata for the manifest
            field_entry = {
                "field": name,
                "metrics": {
                    "frequency": freq,
                    "stability": stability,
                    "is_nested": is_nested
                }
            }

            # 1. Handle "Both" (Mirrored Fields)
            if name in self.mirrored_fields:
                decision_manifest["both"].append(field_entry)
                sql_list.append(name)
            
            # 2. Handle "SQL Only"
            elif is_sql_stable:
                decision_manifest["sql_only"].append(field_entry)
                sql_list.append(name)
            
            # 3. Handle "Mongo Only" (Unstable, Sparse, or Nested)
            else:
                decision_manifest["mongo_only"].append(field_entry)

        # Save the manifest to disk
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(decision_manifest, f, indent=4)
            
        return decision_manifest, sql_list

if __name__ == "__main__":
    ANALYSIS_FILE = "../data/analyzed_data.json"
    DATA_FILE = "../data/normalized_data.json"
    DECISION_FILE = "../data/backend_decision.json"
    
    classifier = HybridClassifier(ANALYSIS_FILE)
    
    # 1. Generate the strategy manifest
    result = classifier.generate_backend_decision(DECISION_FILE)

    if result:
        manifest, sql_list = result
        
        # 2. Sync with MetadataStore (Master Plan)
        meta = MetadataStore()
        total_records = 0
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r') as f:
                total_records = len(json.load(f))

        meta.sync_from_pipeline(
            analysis_file=ANALYSIS_FILE,
            sql_fields=sql_list,
            mirrored_fields=classifier.mirrored_fields,
            total_processed=total_records
        )

        # 3. Sync with TimestampManager (Audit Log)
        latest_ts = "0000-00-00"
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r') as f:
                records = json.load(f)
                if records:
                    latest_ts = max([r.get('timestamp', '0000') for r in records])
        
        ts_log = TimestampManager()
        ts_log.update_timestamps(latest_ts, total_records)

        print(f"Manifest Generated: {DECISION_FILE}")
        print(f"Metadata and Audit Logs updated for {total_records} records.")
    else:
        print("Analysis file not found. Run analyzer.py first.")