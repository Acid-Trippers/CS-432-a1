import json
import os
from datetime import datetime, timezone

def get_current_server_time() -> str:
    return datetime.now(timezone.utc).isoformat()

class TimestampManager:
    def __init__(self, storage_path="data/timestamp_registry.json"):
        self.storage_path = storage_path
        self.state = self._load_state()

    def _load_state(self):
        if os.path.exists(self.storage_path):
            with open(self.storage_path, 'r') as f:
                return json.load(f)
        return self._get_empty_state()

    def _get_empty_state(self):
        return {
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "description": "Historical log of data ingestion windows"
            },
            "summary": {
                "first_run": None,
                "latest_run": None,
                "total_runs": 0,
                "last_data_point": None
            },
            "history": []
        }

    def update_timestamps(self, batch_latest_timestamp, record_count):
        run_time = datetime.now().isoformat()
        
        if not self.state["summary"]["first_run"]:
            self.state["summary"]["first_run"] = run_time
        
        self.state["summary"]["latest_run"] = run_time
        self.state["summary"]["last_data_point"] = batch_latest_timestamp
        self.state["summary"]["total_runs"] += 1
        
        run_entry = {
            "run_id": self.state["summary"]["total_runs"],
            "executed_at": run_time,
            "data_up_to": batch_latest_timestamp,
            "records_processed": record_count
        }
        self.state["history"].append(run_entry)
        self._save()

    def reset_registry(self):
        self.state = self._get_empty_state()
        self._save()
        print(f"Registry at {self.storage_path} has been reset.")

    def get_last_processed_time(self):
        return self.state["summary"]["last_data_point"]

    def _save(self):
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        with open(self.storage_path, 'w') as f:
            json.dump(self.state, f, indent=4)

if __name__ == "__main__":
    manager = TimestampManager()
    print("Timestamp Manager active with historical tracking.")
# EXPLANATION OF UPDATED LOGIC:
# 1. THE HISTORY LIST: Instead of overwriting, we use .append() on the 'history' key. 
#    This allows you to see every single batch you've ever processed, like a bank statement.
# 2. SEPARATION OF CONCERNS: The 'summary' key gives you the current status at a glance, 
#    while 'history' stores the granular details of every run.
# 3. RESET CAPABILITY: The 'reset_registry' method allows you to clear the data if 
#    you are restarting your project from scratch or testing a new data simulator.
# 4. DATA VOLUME TRACKING: I added 'record_count' to the history so you can see if 
#    some batches were larger than others, which is vital for debugging performance.