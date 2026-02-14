import httpx
import json
import os
import subprocess
import time
import sys

def wait_for_server(url: str, timeout: int = 15):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with httpx.Client() as client:
                response = client.get(url.replace(url.split('/')[-1], ""))
                if response.status_code == 200:
                    return True
        except (httpx.RequestError, httpx.ConnectError):
            time.sleep(1)
    return False

def collect_data(url: str, count: int, output_file: str = "data/raw_data.json"):
    if not os.path.exists("data"):
        os.makedirs("data")
        
    records = []
    print(f"Connecting to stream: {url}")
    
    try:
        with httpx.stream("GET", url, timeout=None) as response:
            for line in response.iter_lines():
                if line.startswith("data: "):
                    record = json.loads(line[6:])
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

if __name__ == "__main__":
    record_count = 1000
    if len(sys.argv) > 1:
        try:
            record_count = int(sys.argv[1])
        except ValueError:
            print(f"Invalid record count provided. Defaulting to {record_count}")

    STREAM_URL = f"http://127.0.0.1:8000/record/{record_count}"
    
    EXTERNAL_SERVER_DIR = "../external/" 
    
    print(f">>> Starting Data Server from external path: {EXTERNAL_SERVER_DIR}")
    server_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "data_stream_server:app", "--port", "8000", "--log-level", "error"],
        cwd=EXTERNAL_SERVER_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
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
        print(">>> Error: Server failed to start. Check if data_stream_server.py exists in external folder.")
        stdout, stderr = server_proc.communicate()
        if stderr:
            print(f"Server Error Log:\n{stderr}")
        server_proc.terminate()