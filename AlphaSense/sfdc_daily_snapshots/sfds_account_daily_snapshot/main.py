import functions_framework
from google.cloud import bigquery
from cloudevents.http import CloudEvent
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from datetime import datetime
import pytz

# Initialize clients
client = bigquery.Client()
db = firestore.Client(database="firestore-db")

# Define the path to your config document
doc_ref = db.collection('snapshot').document('WYE4sGMaP9RS07oU5z59')

target_table_id = "as-finance-430214.snapshot.account_snapshot"
source_table_id = "as-finance-430214.sfdc.Account"

def generate_dynamic_select_statement(client, target_table_id, source_table_id, snapshot_date_str):
    """
    Generates a SELECT statement that:
    1. Selects all current source columns (to capture new ones).
    2. Explicitly adds NULL for target columns missing in the source.
    """
    
    target_table = client.get_table(target_table_id)
    target_schema = {field.name: field.field_type for field in target_table.schema}
    
    try:
        source_table = client.get_table(source_table_id)
        source_schema = {field.name: field.field_type for field in source_table.schema}
    except Exception as e:
        print(f"Error getting source schema: {e}")
        # If source schema cannot be retrieved, we cannot proceed reliably
        raise

    select_clauses = []
    
    # 1. Start by selecting all columns from the source table.
    # This automatically includes any NEW columns that appeared in the source.
    source_column_names = list(source_schema.keys())
    select_clauses.extend([f"`{name}`" for name in source_column_names])
    
    # 2. Add columns that exist in the target but are MISSING in the source.
    for target_col_name, target_col_type in target_schema.items():
        
        # Skip columns already handled (present in source) or the snapshot date
        if target_col_name in source_schema or target_col_name == 'snapshot_date':
            continue

        # Column is missing in source but present in target: explicitly cast NULL
        print(f"NOTICE: Column '{target_col_name}' is in the target but missing in source. Selecting NULL.")
        select_clauses.append(f"CAST(NULL AS {target_col_type}) AS `{target_col_name}`")

    # 3. Add the snapshot date column
    select_clauses.append(f"DATE('{snapshot_date_str}') AS snapshot_date")
    
    select_list_str = ",\n    ".join(select_clauses)

    # 4. Construct the final SQL query
    sql = f"""
SELECT
    {select_list_str}
FROM
    `{source_table_id}`
"""
    return sql

# Create a transactional function.
# This ensures the read (get) and write (update) are atomic.
@firestore.transactional
def check_and_update_snapshot_date(transaction, doc_ref, new_date_str):
    """
    Atomically checks if the snapshot has run and updates the date.
    
    Args:
        transaction: The Firestore transaction object.
        doc_ref: The document reference to check.
        new_date_str: The current date string.

    Returns:
        bool: True if the query should run, False if it has already run.
    """
    snapshot = doc_ref.get(transaction=transaction)
    
    if not snapshot.exists:
        # Document doesn't exist, so we'll create it and run the query
        print(f"Config document not found. Creating and proceeding with snapshot for {new_date_str}.")
        transaction.set(doc_ref, {"sfdc_account_date_of_snap": new_date_str})
        return True

    last_run_date = snapshot.to_dict().get("sfdc_account_date_of_snap")

    if last_run_date == new_date_str:
        # It has already run today.
        print(f"Snapshot for {new_date_str} has already run. Skipping.")
        return False
    else:
        # It has not run today. Update the doc and proceed.
        print(f"Updating snapshot date from {last_run_date} to {new_date_str}. Proceeding with query.")
        transaction.update(doc_ref, {"sfdc_account_date_of_snap": new_date_str})
        return True


@functions_framework.cloud_event
def pubsub_to_bigquery_query(cloud_event):
    
    # Use 'America/New_York' to match your BigQuery query timezone
    eastern = pytz.timezone('America/New_York')
    current_datetime = datetime.now(eastern)
    string_date = current_datetime.strftime("%Y-%m-%d")

    try:
        # 1. Run the atomic transaction
        transaction = db.transaction()
        should_run_query = check_and_update_snapshot_date(transaction, doc_ref, string_date)

        # 2. Check the result
        if not should_run_query:
            return "Snapshot already completed for today."
        
        # 3. GENERATE DYNAMIC SQL HERE
        sql = generate_dynamic_select_statement(client, target_table_id, source_table_id, string_date)
        
        # 4. Configure the BigQuery Job
        job_config = bigquery.QueryJobConfig(
            destination = target_table_id,
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )

        # 5. Execute the BigQuery job
        print(f"Running BigQuery snapshot query for {string_date}...")
        query_job = client.query(sql, job_config=job_config)
        query_job.result()  # Wait for the job to complete

        log_message = f"Query job completed successfully. Job ID: {query_job.job_id}"
        print(log_message)
        return log_message

    except Exception as e:
        error_message = f"An error occurred: {e}"
        print(error_message)
        raise e
