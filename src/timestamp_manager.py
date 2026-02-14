import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

# Configure logging
logger = logging.getLogger(__name__)

# sys_ingested_at
def get_current_server_time() -> str:
    """
    Args:
        None

    Returns:
        str: Current server time in ISO 8601 format (UTC)
    """
    return datetime.now(timezone.utc).isoformat()



def process_timestamps(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enriches the record with server ingestion timestamp (sys_ingested_at)
   
    Args:
        record: The incoming JSON dictionary (normalized or raw).
        
    Returns:
        The modified record dictionary.
    """

    record['sys_ingested_at'] = get_current_server_time()
    
    if 't_stamp' not in record:
        logger.warning(
                f"Record {record.get('username', 'unknown')} is missing 't_stamp' field."
                "Check normalizer.py configuration."
            )
        
    return record