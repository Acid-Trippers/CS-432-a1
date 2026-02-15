# Assignment-1: Adaptive Data Ingestion System

## Overview

An autonomous data ingestion system that dynamically determines the optimal storage backend (SQL or MongoDB) for incoming JSON records based on data patterns, field behaviors, and structural complexity.

## Repository Structure

```
CS-432-a1/
├── data/
│   ├── raw_data.json              # Raw JSON records from the data stream
│   ├── normalized_data.json       # Cleaned and normalized records
│   ├── analyzed_data.json         # Records with extracted statistics and patterns
│   ├── decision_graph.png         # Visualization of classification decisions
│   └── timestamp_registry.json    # Historical ingestion metadata
├── external/
│   └── simulation_code.py         # Data stream generator (provided by instructor)
├── src/
│   ├── client.py                  # Streams and collects records from http://localhost:8000
│   ├── normalizer.py              # Phase 1: Field name normalization and cleaning
│   ├── analyzer.py                # Phase 2: Statistical analysis (frequency, types, patterns)
│   ├── classifier.py              # Phase 3: Classification logic (SQL vs MongoDB routing)
│   ├── metadata_store.py          # Persistence layer for classification metadata
│   ├── timestamp_manager.py       # Tracks ingestion runs and data timestamps
│   └── classification_visualiser.py  # Generates decision visualization
├── .gitignore
├── requirements.txt               # Project dependencies
└── README.md
```

## How It Works

The system processes JSON records through a four-stage pipeline:

1. **Data Collection** (`client.py`): Streams JSON records from the external data generator
2. **Normalization** (`normalizer.py`): Standardizes field names and detects schema variations
3. **Analysis** (`analyzer.py`): Extracts statistical metrics and identifies data patterns
4. **Classification** (`classifier.py`): Routes fields to SQL or MongoDB based on analysis results

The classification logic evaluates fields across multiple dimensions:
- **Frequency**: How often a field appears (sparsity penalties)
- **Type Stability**: Whether a field maintains consistent data types
- **Cardinality**: Uniqueness of values (low cardinality → better for SQL)
- **Nesting**: Complex nested objects favor MongoDB
- **Array Fields**: Array structures preferred in MongoDB

Mandatory fields (username, timestamp, sys_ingested_time) are stored in both backends.

## Setup Instructions

### Prerequisites

- Python 3.8+
- PostgreSQL or MySQL
- MongoDB

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Acid-Trippers/CS-432-a1.git
   cd CS-432-a1
   ```

2. **Install project dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Running the System

### Step 1: Start the Data Generator

The external data stream provides simulated JSON records via SSE (Server-Sent Events):

```bash
cd external
uvicorn simulation_code:app --reload --port 8000
```

The server will be available at `http://127.0.0.1:8000`

### Step 2: Run the Data Pipeline

```bash
# Collect records from the stream
python src/client.py

# Normalize field names
python src/normalizer.py

# Extract statistics and patterns
python src/analyzer.py

# Classify and route fields to appropriate databases
python src/classifier.py
```

Each script reads from the previous stage's output and writes processed data to the `data/` directory.

### Data Stream API

The external data generator provides these endpoints:

- **Single record**: `GET /`
- **Multiple records**: `GET /record/{count}`
  - Example: `GET /record/100` (retrieves 100 records as SSE stream)

### Example Data Collection

```bash
# Fetch records using curl with SSE parsing
curl http://localhost:8000/record/50 | grep "^data: " | sed 's/^data: //' | jq

# Or use the client.py script which handles SSE parsing automatically
python src/client.py
```



## License

This is a course assignment project for CS-432.
