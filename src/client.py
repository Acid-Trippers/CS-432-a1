import httpx
import json
import os
import subprocess
import time
import sys
from datetime import datetime

def wait_for_server(url: str, timeout: int = 15):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with httpx.Client() as client:
                response = client.get("http://127.0.0.1:8000/")
                if response.status_code == 200:
                    return True
        except (httpx.RequestError, httpx.ConnectError):
            time.sleep(1)
    return False

def collect_data(url: str, count: int, output_file: str = "data/raw_data.json"):
    data_dir = os.path.dirname(output_file)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    records = []
    print(f"Connecting to stream: {url}")
    
    try:
        with httpx.stream("GET", url, timeout=None) as response:
            for line in response.iter_lines():
                if line.startswith("data: "):
                    record = json.loads(line[6:])
                    
                    # Add Ingestion Time
                    record['sys_ingested_time'] = datetime.now().isoformat()
                    
                    records.append(record)
                    
                    if len(records) % 100 == 0:
                        print(f"Downloaded {len(records)}/{count} records...")
                    
                    if len(records) >= count:
                        break
    except Exception as e:
        print(f"Error collecting data: {e}")
    
    with open(output_file, 'w') as f:
        json.dump(records, f, indent=4)
    
    print(f"Collection complete. {len(records)} records saved to {output_file}")

def run_data_collection(record_count: int = 1000):
    if len(sys.argv) > 1:
        try:
            record_count = int(sys.argv[1])
        except ValueError:
            print(f"Invalid record count provided. Defaulting to {record_count}")

    STREAM_URL = f"http://127.0.0.1:8000/record/{record_count}"
    
    EXTERNAL_SERVER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../external/"))
    
    print(f">>> Starting Data Server from external path: {EXTERNAL_SERVER_DIR}")
    
    server_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "simulation_code:app", "--port", "8000"],
        cwd=EXTERNAL_SERVER_DIR,
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    
    print(">>> Waiting for server to become responsive...")
    if wait_for_server(STREAM_URL):
        try:
            collect_data(STREAM_URL, record_count)
        finally:
            print(">>> Shutting down Data Server...")
            server_proc.terminate()
            server_proc.wait()
            print(">>> Done.")
    else:
        print(">>> Error: Server failed to start or timed out.")
        server_proc.terminate()

if __name__ == "__main__":
    run_data_collection()