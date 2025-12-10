This BigQuery cloud function snapshots the account table from the direct Salesforce data transfer.
The steps are as follows:
  1) The data transfer pulls the account object and completely replaces the 'as-finance-430214.sfdc.Account' table each morning - this is triggered at 9AM UTC
  2) Once the data transfer completes successfully, a Log Sink that is listening to this specific data transfer for a success log sends a note to the pub/sub topic 'projects/as-finance-430214/topics/sfdc_account`
  3) This pub/sub topic then publishes the message, which triggers the Eventarc Trigger in the cloud function, telling it to run
  4) Now, the cloud function runs:
       - First, an atomic transaction is created to check if the snapshot has already been initiated for the current day (this is set using Eastern Time)
       - If the snapshot hasn't run today, it compares the schemas from the snapshot table destination and the data transfer table to create a dynamic schema that allows for instances where a field has been created
           or deleted within Salesforce.
       - Then, the dynamically created SQL code runs, taking the current Account table and appending it as a snapshot to the existing account_snapshot table

This is currently being monitored for inaccuraces or data duplication via a Tableau dashboard 'https://us-east-1.online.tableau.com/#/site/alphasense/views/SFDCSnapshotCheck/SnapshotDashboard?:iid=1'.
There are also email notifications being sent to abossart@alpha-sense.com when the data transfer fails to complete successfully.  This way, while this is still being monitored, we are able to get the snapshot into the
dataset for consistency and analysis capabilities in the future.

The function target for this is the '‎pubsub_to_bigquery_query' function.  Leave the entrypoint blank
