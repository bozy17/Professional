import base64
import json
import logging
import time
import functions_framework

from google.cloud import bigquery_datatransfer_v1
from google.cloud.bigquery_datatransfer_v1 import TransferState

#Update Function with New Repository Owner v2 12.10.2025

# Initialize Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Client globally for warm starts (reuse across invocations)
client = bigquery_datatransfer_v1.DataTransferServiceClient()

# CONFIGURATION
# Suggestion: Move this to Environment Variables or Secret Manager for easier updates.
MANAGED_TRANSFERS = {
    "projects/as-finance-430214/locations/us/transferConfigs/69657df0-0000-22f1-a447-883d24f2b4d0": 3, # Account
    "projects/as-finance-430214/locations/us/transferConfigs/692f3f12-0000-2605-bcb1-883d24f366f8": 3, # Contact
    "projects/as-finance-430214/locations/us/transferConfigs/692d6a4e-0000-218e-b20f-24058882b178": 3, # Opp
    "projects/as-finance-430214/locations/us/transferConfigs/6916e26d-0000-26a7-b20e-f4f5e80d7988": 3, # Opp Line Item
    "projects/as-finance-430214/locations/us/transferConfigs/69312243-0000-2486-9d5e-94eb2c0ce348": 3, # Opp Split
    "projects/as-finance-430214/locations/us/transferConfigs/6968b171-0000-263a-9022-089e082cf438": 3, # Subscription
    "projects/as-finance-430214/locations/us/transferConfigs/69627e87-0000-274a-8c66-f4f5e80cf590": 3, # User
}

def parse_log_entry(cloud_event):
    """Decodes and parses the incoming Pub/Sub message."""
    try:
        log_data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        return json.loads(log_data)
    except Exception as e:
        logger.error(f"Failed to decode log entry: {e}")
        return None

def get_transfer_ids(log_entry):
    """Extracts critical IDs (project, location, config, run) from the log."""
    resource = log_entry.get("resource", {})
    labels = resource.get("labels", {})
    
    ids = {
        "project_id": labels.get("project_id"),
        "location": labels.get("location"),
        "config_id": labels.get("config_id"),
        "run_id": labels.get("run_id")
    }

    # Fallback: Check top-level labels if run_id is missing in resource labels
    if not ids["run_id"]:
        ids["run_id"] = log_entry.get("labels", {}).get("run_id")

    if not all(ids.values()):
        logger.warning(f"Log missing required DTS labels. Data found: {ids}")
        return None
        
    return ids

def wait_for_run_completion(run_path, max_attempts=5):
    """Polls the API to ensure the run has actually failed before retrying."""
    for attempt in range(max_attempts):
        try:
            target_run = client.get_transfer_run(request={"name": run_path})
            
            if target_run.state in [TransferState.FAILED, TransferState.CANCELLED]:
                return target_run
            
            logger.info(f"Run {run_path} state is {target_run.state.name}. Waiting... ({attempt + 1}/{max_attempts})")
            time.sleep(3) 
            
        except Exception as e:
            logger.error(f"API Error fetching run {run_path}: {e}")
            return None
            
    return None

def check_retry_limit(config_path, target_time, max_retries):
    """Counts how many runs share the same schedule time to enforce retry limits."""
    try:
        history_response = client.list_transfer_runs(
            request=bigquery_datatransfer_v1.ListTransferRunsRequest(
                parent=config_path,
                page_size=20 # Check last 20 runs
            )
        )
        
        attempt_count = 0
        for run in history_response:
            # Normalize time check
            run_time = run.schedule_time if run.schedule_time else run.run_time
            if run_time == target_time:
                attempt_count += 1
                
        return attempt_count
    except Exception as e:
        logger.error(f"Error checking run history for {config_path}: {e}")
        return float('inf') # Fail safe: assume limit reached if we can't check

@functions_framework.cloud_event
def retry_dts_failure_from_logs(cloud_event):
    # 1. Decode Log
    log_entry = parse_log_entry(cloud_event)
    if not log_entry: return

    # 2. Check Failure Signature
    text_payload = log_entry.get("jsonPayload", {}).get("message", "")
    if "Transfer run failed" not in text_payload:
        # Debug level allows you to silence these in prod if they are too noisy
        logger.debug(f"Skipping non-failure message: {text_payload}") 
        return

    # 3. Extract IDs
    ids = get_transfer_ids(log_entry)
    if not ids: return

    config_path = f"projects/{ids['project_id']}/locations/{ids['location']}/transferConfigs/{ids['config_id']}"
    run_path = f"{config_path}/runs/{ids['run_id']}"

    # 4. Check Allow List
    if config_path not in MANAGED_TRANSFERS:
        logger.info(f"Ignored: Config {ids['config_id']} not in managed list.")
        return

    max_retries = MANAGED_TRANSFERS[config_path]

    # 5. Confirm Failure State (API Check)
    logger.info(f"Validating failure for: {run_path}")
    target_run = wait_for_run_completion(run_path)
    
    if not target_run:
        logger.warning(f"Aborting: Could not confirm final failure state for {run_path}")
        return

    # 6. Check History & Retry
    target_time = target_run.schedule_time if target_run.schedule_time else target_run.run_time
    attempts = check_retry_limit(config_path, target_time, max_retries)
    
    logger.info(f"Retry check: {attempts} attempts found (Limit: {max_retries})")

    if attempts > max_retries:
        logger.warning(f"STOPPING: Retry limit reached for {ids['config_id']}")
        return

    # 7. Trigger Retry
    try:
        client.start_manual_transfer_runs(
            request=bigquery_datatransfer_v1.StartManualTransferRunsRequest(
                parent=config_path,
                requested_run_time=target_time 
            )
        )
        logger.info(f"SUCCESS: Retry triggered for {ids['config_id']} at {target_time}")
    except Exception as e:
        logger.error(f"Failed to trigger retry for {ids['config_id']}: {e}")
