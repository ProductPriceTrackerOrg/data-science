import pandas as pd
from google.cloud import bigquery
import logging

def get_price_history(project_id: str, dataset_id: str, days: int = 90) -> pd.DataFrame:
    """
    Extracts the daily price history for all variants from the BigQuery warehouse.

    Args:
        project_id: The GCP project ID.
        dataset_id: The BigQuery dataset ID where the tables reside.
        days: The number of days of price history to retrieve.

    Returns:
        A pandas DataFrame with the price history.
    """
    logging.info(f"Connecting to BigQuery to extract price history for the last {days} days...")
    try:
        client = bigquery.Client(project=project_id)
        
        query = f"""
            SELECT
                fpp.price_fact_id,
                fpp.variant_id,
                fpp.current_price,
                dd.full_date
            FROM
                `{project_id}.{dataset_id}.FactProductPrice` AS fpp
            JOIN
                `{project_id}.{dataset_id}.DimDate` AS dd ON fpp.date_id = dd.date_id
            WHERE
                dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
            ORDER BY
                fpp.variant_id, dd.full_date DESC
        """
        
        df = client.query(query).to_dataframe()
        logging.info(f"Successfully extracted {len(df)} price records from BigQuery.")
        return df

    except Exception as e:
        logging.error(f"Failed to extract data from BigQuery: {e}")
        # Return an empty DataFrame on failure
        return pd.DataFrame()

