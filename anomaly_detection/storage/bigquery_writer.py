import pandas as pd
from google.cloud import bigquery
import logging
from datetime import datetime, timezone

def write_anomalies_to_bigquery(anomalies_df: pd.DataFrame, project_id: str, dataset_id: str, table_name: str):
    """
    Writes the detected anomalies DataFrame to the specified BigQuery table.

    Args:
        anomalies_df: DataFrame containing the detected anomalies.
        project_id: The GCP project ID.
        dataset_id: The BigQuery dataset ID.
        table_name: The name of the fact table to write to (e.g., 'FactPriceAnomaly').
    """
    if anomalies_df.empty:
        logging.info("No anomalies to write to BigQuery.")
        return

    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.{table_name}"

        # --- FIX: GENERATE THE REQUIRED anomaly_id ---
        now_nanos = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
        anomalies_df = anomalies_df.reset_index(drop=True)
        anomalies_df['anomaly_id'] = [now_nanos + i for i in anomalies_df.index]

        # Prepare the DataFrame to match the FactPriceAnomaly schema
        df_to_load = pd.DataFrame({
            'anomaly_id': anomalies_df['anomaly_id'].astype('Int64'), # Added anomaly_id
            'price_fact_id': anomalies_df['price_fact_id'].astype(int),
            'model_id': anomalies_df['model_id'].astype(int),
            'anomaly_score': anomalies_df['anomaly_score'].astype(float),
            'anomaly_type': anomalies_df['anomaly_type'].astype(str),
            'created_at': pd.to_datetime(anomalies_df['created_at']),
            'status': 'PENDING_REVIEW',
            'reviewed_by_user_id': None # Added reviewed_by_user_id, set to NULL
        })
        
    

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND", # Append new anomalies to the table
        )

        logging.info(f"Writing {len(df_to_load)} anomalies to BigQuery table: {table_id}")
        job = client.load_table_from_dataframe(df_to_load, table_id, job_config=job_config)
        job.result() # Wait for the job to complete
        
        logging.info(f"Successfully loaded {job.output_rows} rows to {table_id}.")

    except Exception as e:
        logging.error(f"Failed to write anomalies to BigQuery: {e}")

