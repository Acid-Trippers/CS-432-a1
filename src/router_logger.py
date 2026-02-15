import os
import sys
import json
import time
import subprocess
import httpx
from datetime import datetime

scriptDir = os.path.dirname(os.path.abspath(__file__))
dataDir = os.path.join(scriptDir, '..', 'data')
externalDir = os.path.join(scriptDir, '..', 'external')

classificationFile = os.path.join(dataDir, 'field_metadata.json')
sqlOutputFile = os.path.join(dataDir, 'sql_records.json')
mongoOutputFile = os.path.join(dataDir, 'mongo_records.json')
routerLogFile = os.path.join(dataDir, 'router_logger.txt')

def loadClassificationMap():
    if not os.path.exists(classificationFile):
        print(f"Error: Classification file not found at {classificationFile}")
        sys.exit(1)

    with open(classificationFile, 'r') as f:
        results = json.load(f)
    
    return {item['fieldName']: item['decision'] for item in results}

def waitForServer(url: str, timeout: int = 15):
    startTime = time.time()
    while time.time() - startTime < timeout:
        try:
            with httpx.Client() as client:
                response = client.get("http://127.0.0.1:8000/")
                if response.status_code == 200:
                    return True
        except (httpx.RequestError, httpx.ConnectError):
            time.sleep(1)
    return False

def processAndSplit(recordCount: int):
    schemaMap = loadClassificationMap()
    
    serverProc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "simulation_code:app", "--port", "8000"],
        cwd=externalDir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    url = f"http://127.0.0.1:8000/record/{recordCount}"
    sqlRecords = []
    mongoRecords = []

    try:
        if not waitForServer(url):
            print("Server failed to start.")
            return

        with open(routerLogFile, 'w') as logFile, httpx.stream("GET", url, timeout=None) as response:
            count = 0
            for line in response.iter_lines():
                if not line.startswith("data: "):
                    continue
                
                recordJson = json.loads(line[6:])
                
                ingestTime = datetime.now().isoformat()
                recordJson['sys_ingested_time'] = ingestTime
                
                count += 1
                
                sqlDoc = {}
                mongoDoc = {}

                logEntry = f"Record received at {ingestTime}\n"
                logEntry += f"{len(recordJson)} Fields\n"

                for field, value in recordJson.items():
                    decision = schemaMap.get(field, "MONGO")

                    logEntry += f"{field} : {decision}\n"

                    if decision == "SQL":
                        sqlDoc[field] = value
                    elif decision == "MONGO":
                        mongoDoc[field] = value
                    elif decision == "BOTH":
                        sqlDoc[field] = value
                        mongoDoc[field] = value
                
                logEntry += "\n"
                logFile.write(logEntry)
                
                if sqlDoc: sqlRecords.append(sqlDoc)
                if mongoDoc: mongoRecords.append(mongoDoc)

                if count >= recordCount:
                    break
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        serverProc.terminate()
        serverProc.wait()

    with open(sqlOutputFile, 'w') as f:
        json.dump(sqlRecords, f, indent=2)

    with open(mongoOutputFile, 'w') as f:
        json.dump(mongoRecords, f, indent=2)

    print(f"Saved {len(sqlRecords)} SQL records and {len(mongoRecords)} Mongo records.")
    print(f"Router logs saved to {routerLogFile}")

if __name__ == "__main__":
    count = 10
    if len(sys.argv) > 1:
        try:
            count = int(sys.argv[1])
        except ValueError:
            pass

    processAndSplit(count)