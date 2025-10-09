import yaml
import logging
import pandas as pd
from datetime import datetime

# Import our custom modules
from data.extraction import get_price_history
from data.preparation import prepare_price_data
from models.statistical import ModifiedZScoreDetector, MovingAverageDetector
from storage.bigquery_writer import write_anomalies_to_bigquery

# --- Configuration ---
CONFIG_PATH = 'config/detection_parameters.yaml'

def setup_logging():
    """Sets up basic logging for the script execution."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def main():
    """
    Main entry point to orchestrate the entire anomaly detection process.
    """
    start_time = datetime.now()
    logging.info("--- Starting Price Anomaly Detection Pipeline ---")

    # 1. Load configuration
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        return

    # 2. Extract and Prepare Data
    project_id = config['bigquery']['project_id']
    dataset_id = config['bigquery']['dataset_id']
    logging.info("Step 1 & 2: Extracting and preparing data from BigQuery...")
    raw_data = get_price_history(project_id, dataset_id, days=90)
    prepared_data = prepare_price_data(raw_data)
    
    if prepared_data.empty:
        logging.warning("No data available after preparation. Exiting.")
        return

    # 3. Initialize and run detectors
    logging.info("Step 3: Initializing anomaly detection models...")
    detectors = [
        ModifiedZScoreDetector(config),
        MovingAverageDetector(config)
    ]

    all_anomalies = []
    for detector in detectors:
        logging.info(f"Running detector: {detector.__class__.__name__}")
        results = detector.detect(prepared_data)
        if not results.empty:
            all_anomalies.append(results)

    # 4. Combine and Store Results
    if not all_anomalies:
        logging.info("No anomalies were detected by any model.")
    else:
        final_results_df = pd.concat(all_anomalies, ignore_index=True)
        # Drop duplicates in case both models flag the same price_fact_id
        final_results_df.drop_duplicates(subset=['price_fact_id'], inplace=True)
        
        logging.info(f"Total unique anomalies found: {len(final_results_df)}")
        logging.info("Step 4: Storing detected anomalies in BigQuery...")
        write_anomalies_to_bigquery(
            final_results_df,
            project_id,
            dataset_id,
            config['bigquery']['fact_price_anomaly_table']
        )

    end_time = datetime.now()
    logging.info(f"--- Pipeline Finished in {end_time - start_time} ---")


if __name__ == '__main__':
    setup_logging()
    main()

