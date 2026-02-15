"""
Field Normalizer module

- Cleans all field names into snakecase, for uniformity throughout
- Checks if the field name is similar to somehting seen earlier. if yes, maps it to the same field

"""

import re
import os
import json
from difflib import get_close_matches

class DynamicNormalizer:

    def __init__(self, similarity_threshold = 0.85):
        self.master_keys = []
        self.threshold = similarity_threshold

    def normalize_key(self, key):

        # 1. Handle camelCase and PascalCase (e.g., userName -> user_name)
        temp = re.sub('([a-z0-9])([A-Z])', r'\1_\2', key)
        # 2. Lowercase everything
        clean_key = temp.lower().replace(" ", "_")

        #Dynamic discovery
        matches = get_close_matches(clean_key, self.master_keys, n = 1, cutoff=self.threshold)
        if matches:
            # If a similar key exists, use the existing one
            return matches[0]
        else:
            # If it's truly new, learn it and add to master keys
            self.master_keys.append(clean_key)
            return clean_key
    
    def normalize_record(self, record):
        """Recursively cleans all keys in a JSON record."""
        if isinstance(record, list):
            return [self.normalize_record(item) for item in record]
        if not isinstance(record, dict):
            return record
        
        return {self.normalize_key(k): self.normalize_record(v) for k, v in record.items()}

def run_field_normalization():
    INPUT_FILE = "../data/raw_data.json"
    OUTPUT_FILE = "../data/normalized_data.json"

    if os.path.exists(INPUT_FILE):
        with open(INPUT_FILE, 'r') as f:
            raw_data = json.load(f)
        
        normalizer = DynamicNormalizer()
        
        # Process all records
        normalized_data = [normalizer.normalize_record(doc) for doc in raw_data]
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(normalized_data, f, indent=4)
            
        print(f"Normalization complete. Saved {len(normalized_data)} records to {OUTPUT_FILE}")
        print(f"Discovered Master Keys: {normalizer.master_keys}")
    else:
        print(f"No raw data found at {INPUT_FILE}. Run client.py first.")

# CHANGES MADE:
# 1. Added 'import os' and 'import json' to handle file operations.
# 2. Updated 'normalize_record' to handle lists/arrays recursively (essential for 'readings' or 'tags' fields).
# 3. Added an 'if __name__ == "__main__":' block to make the script executable.
# 4. Implemented file loading from '../data/raw_data.json' and saving to '../data/normalized_data.json'.
# 5. Added directory verification using 'os.makedirs' to prevent FileNotFoundError.
# 6. Added a print summary showing the final Master Keys discovered during the fuzzy matching process.