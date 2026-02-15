import json
from dataclasses import dataclass
from typing import List, Dict, Any

WEIGHTS = {
    "sparsity": 1.5,
    "nested": 2.0,
    "lowCardinality": 1.0
}

THRESHOLDS = {
    "densityLimit": 0.6,
    "stabilityLimit": 0.99,
    "cardinalityLimit": 0.1,
}

MONGO_SCORE_THRESHOLD = 0.3 
MANDATORY_BOTH = {"username", "timestamp", "sys_ingested_time"}

@dataclass
class FieldStats:
    fieldName: str
    frequency: float
    dominantType: str
    typeStability: float
    cardinality: float
    isNested: bool
    isArray: bool
    dominantPattern: str

class SchemaClassifier:
    def __init__(self, weights: Dict[str, float], limits: Dict[str, float], threshold: float):
        self.weights = weights
        self.limits = limits
        self.threshold = threshold

    def classifyField(self, field: FieldStats) -> Dict[str, Any]:
        result = {
            "fieldName": field.fieldName,
            "decision": "SQL",
            "score": 0.0,
            "flags": [],
            "reason": "Default"
        }

        # Mandatory Check
        if field.fieldName in MANDATORY_BOTH:
            result["decision"] = "BOTH"
            result["reason"] = "Mandatory Field"
            return result

        # Hard Gate: Type Instability
        if field.typeStability < self.limits["stabilityLimit"]:
            result["decision"] = "MONGO"
            result["score"] = 1.0 # Max score for hard gate
            result["flags"].append("UNSTABLE_TYPE")
            result["reason"] = "Hard Gate: Unstable Types"
            return result

        # Calculate Penalties
        score = 0.0
        maxScore = sum(self.weights.values())
        
        # Sparsity Flag
        if field.frequency < self.limits["densityLimit"]:
            score += self.weights["sparsity"]
            result["flags"].append("SPARSITY")

        # UPDATED NESTED LOGIC:
        # We only penalize if it's an ARRAY or a literal 'object' type.
        # If it's a flattened path (isNested=True but dominantType is float/int/str), 
        # we don't apply the heavy 2.0 penalty.
        if field.isArray or field.dominantType in ['object', 'dict', 'array']:
            score += self.weights["nested"]
            result["flags"].append("COMPLEX_STRUCTURE")
        elif field.isNested:
            # Optional: Add a much smaller "Path Penalty" (e.g., 0.1) 
            # if you still want to slightly prefer top-level fields for SQL.
            pass

        # Low Cardinality Flag
        if field.cardinality < self.limits["cardinalityLimit"]:
            score += self.weights["lowCardinality"]
            result["flags"].append("LOW_CARDINALITY")

        # Final Score
        normalizedScore = score / maxScore
        result["score"] = round(normalizedScore, 3)

        if normalizedScore > self.threshold:
            result["decision"] = "MONGO"
            result["reason"] = "Score Threshold Exceeded"
        else:
            result["decision"] = "SQL"
            result["reason"] = "Safe for SQL"

        return result

def runPipeline():
    # Load Data
    try:
        with open('data/analyzed_data.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print("Error: analyzed_data.json not found")
        return

    classifier = SchemaClassifier(WEIGHTS, THRESHOLDS, MONGO_SCORE_THRESHOLD)
    output_records = []

    print(f"{'Field':<20} {'Score':<6} {'Decision':<10} {'Flags'}")
    print("-" * 60)

    for record in data['fields']:
        stats = FieldStats(
            fieldName=record['field_name'],
            frequency=record['frequency'],
            dominantType=record['dominant_type'],
            typeStability=record['type_stability'],
            cardinality=record['cardinality'],
            isNested=record['is_nested'],
            isArray=record['is_array'],
            dominantPattern=record['dominant_pattern']
        )
        
        res = classifier.classifyField(stats)
        output_records.append(res)
        
        flags_str = ", ".join(res["flags"])
        print(f"{res['fieldName']:<20} {res['score']:<6} {res['decision']:<10} {flags_str}")

    # Save Results
    with open('data/field_metadata.json', 'w', encoding='utf-8') as f:
        json.dump(output_records, f, indent=2)
    

def run_classification():
    runPipeline()