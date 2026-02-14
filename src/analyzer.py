import re
import os
import json
from typing import Dict, Any, List
from collections import defaultdict

class DataAnalyzer:
    def __init__(self):
        self.total_records = 0
        self.field_counts = defaultdict(int)
        self.field_types = defaultdict(lambda: defaultdict(int))
        self.field_values = defaultdict(set)
        self.value_count_limit = 10000
        self.nested_fields = set()
        self.array_fields = set()
        self.pattern_matches = defaultdict(lambda: defaultdict(int))
        self.patterns = {
            'ip_address': re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'),
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'url': re.compile(r'^https?://'),
            'uuid': re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', re.I),
            'iso_timestamp': re.compile(r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}'),
        }

    def _get_type_name(self, value: Any) -> str:
        if value is None: return 'null'
        if isinstance(value, bool): return 'boolean'
        if isinstance(value, int): return 'integer'
        if isinstance(value, float): return 'float'
        if isinstance(value, str): return 'string'
        if isinstance(value, list): return 'array'
        if isinstance(value, dict): return 'object'
        return type(value).__name__

    def _detect_pattern(self, value: Any) -> str:
        if not isinstance(value, str): return 'none'
        for name, regex in self.patterns.items():
            if regex.match(value): return name
        return 'none'

    def _analyze_value(self, field_name: str, value: Any):
        self.field_counts[field_name] += 1
        type_name = self._get_type_name(value)
        self.field_types[field_name][type_name] += 1
        if isinstance(value, dict):
            self.nested_fields.add(field_name)
        elif isinstance(value, list):
            self.array_fields.add(field_name)
        else:
            if len(self.field_values[field_name]) < self.value_count_limit:
                self.field_values[field_name].add(str(value))
            if isinstance(value, str):
                pattern = self._detect_pattern(value)
                if pattern != 'none':
                    self.pattern_matches[field_name][pattern] += 1

    def analyze_records(self, records: List[Dict]):
        for record in records:
            self.total_records += 1
            for field_name, value in record.items():
                self._analyze_value(field_name, value)

    def save_analysis(self, output_file: str = "data/analyzed_data.json"):
        fields_summary = []
        for f in sorted(self.field_counts.keys()):
            count = self.field_counts[f]
            freq = count / self.total_records
            type_counts = self.field_types[f]
            dom_type, type_val = max(type_counts.items(), key=lambda x: x[1])
            stability = type_val / sum(type_counts.values())
            cardinality = len(self.field_values[f]) / count if count > 0 else 0
            patterns = self.pattern_matches[f]
            dom_pattern = max(patterns.items(), key=lambda x: x[1])[0] if patterns else 'none'
            
            fields_summary.append({
                'field_name': f,
                'frequency': freq,
                'dominant_type': dom_type,
                'type_stability': stability,
                'cardinality': cardinality,
                'is_nested': f in self.nested_fields,
                'is_array': f in self.array_fields,
                'dominant_pattern': dom_pattern
            })

        summary = {
            'total_records': self.total_records,
            'fields': fields_summary
        }
        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=4)
        print(f"Analysis saved to {output_file}")

if __name__ == "__main__":
    INPUT_FILE = "data/raw_data.json"
    
    if os.path.exists(INPUT_FILE):
        with open(INPUT_FILE, 'r') as f:
            data = json.load(f)
        
        analyzer = DataAnalyzer()
        analyzer.analyze_records(data)
        analyzer.save_analysis()
    else:
        print(f"No data found at {INPUT_FILE}. Run client.py first.")