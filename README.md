# Assignment-1: Adaptive Data Ingestion System

## Overview

An autonomous data ingestion system that dynamically determines the optimal storage backend (SQL or MongoDB) for incoming JSON records based on data patterns, field behaviors, and structural complexity.

## Repository Structure

```
CS-432-a1/
├── config/
│   └── config.json            # Database credentials and thresholds (e.g., 0.8 frequency)
├── data/
│   ├── metadata_store.json    # Stores routing decisions for persistence
│   └── logs/                  # Debug logs for ingestion issues
├── docker/
│   └── docker-compose.yml     # Spins up PostgreSQL & MongoDB containers
├── external/
│   ├── simulation_code.py     # Data stream generator (provided by instructor)
│   └── requirements.txt       # Dependencies for the generator
├── src/
│   ├── __init__.py
│   ├── client.py              # Client code to consume records from http://localhost:8000
│   ├── normalizer.py          # Phase 1: Data cleaning and field normalization
│   ├── analyzer.py            # Phase 2: Statistical analysis (frequency, type stability)
│   ├── classifier.py          # Phase 3: Decision logic (SQL vs MongoDB routing)
│   ├── storage.py             # Phase 4: Database insertion handlers
│   └── main.py                # Main orchestrator
├── .gitignore
├── requirements.txt           # Project dependencies (pymongo, psycopg2, etc.)
└── README.md
```

## Setup Instructions

### Prerequisites

- Python 3.8+
- PostgreSQL or MySQL
- MongoDB
- Docker (optional, for containerized databases)
- Git

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

3. **Configure database connections**
   ```bash
   # Edit config/config.json with your database credentials
   # Update host, port, username, password, and database names
   ```

4. **Start databases** (if using Docker)
   ```bash
   docker-compose -f docker/docker-compose.yml up -d
   ```

5. **Run the main system**
   ```bash
   python src/main.py
   ```

## Data Streaming App

### Running the Streaming Server

1. **Install streaming app dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start the data stream server**
   ```bash
   cd external
   uvicorn simulation_code:app --reload --port 8000
   ```

3. **Access the stream**
   ```
   http://127.0.0.1:8000
   ```

### API Endpoints

- **Single record**: `GET /`
- **Multiple records**: `GET /record/{count}`
  - Example: `GET /record/10` (retrieves 10 records)

### Example cURL Commands

```bash
# Fetch a single record
curl http://localhost:8000/

# Fetch 5 records and pipe to jq
curl -N http://127.0.0.1:8000/record/5 | grep "^data: " | sed 's/^data: //' | jq

# Fetch and save to file
curl http://localhost:8000/record/50 > sample_data.json
```



## License

This is a course assignment project for CS-432.
