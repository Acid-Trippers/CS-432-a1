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
│   └── sqlRecords.json            # Stores records to be sent to SQL
│   └── mongoRecords.Json          # Stores records to be sent to MongoDB
│   └── field_metadata.json        # Stores which field goes where and why
│   └── router_logger.txt          # Logging ingested records
│   └── drift_logger.txt           # Logging fields to be shifted
├── external/
│   └── simulation_code.py         # Data stream generator (provided by instructor)
├── src/
│   ├── client.py                  # Streams and collects records from http://localhost:8000
│   ├── normalizer.py              # Phase 1: Field name normalization and cleaning
│   ├── analyzer.py                # Phase 2: Statistical analysis (frequency, types, patterns)
│   ├── classifier.py              # Phase 3: Classification logic (SQL vs MongoDB routing)
│   ├── timestamp_manager.py       # Tracks ingestion runs and data timestamps
│   ├── router_logger.py           # Ingests data one record at a time, routes them to DB and logs records.
│   ├── main.py                    # Python script to activate the pipeline.
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
# In the repo directory, run
python src/main.py initialise
to get a batch of 1000 records to create metadata according to classification heuristics

# In the repo directory, run
python src/main.py router <number>
to get the next <number> records and store them in SQL Database or MongoDB

# In the repo directory, run
python src/main.py clearLogs
to clear all logs

# In the repo directory, run
python src/main.py clearRecords
to clear all records from both Databases
```


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
