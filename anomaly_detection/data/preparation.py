import pandas as pd
import logging

def prepare_price_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and prepares the raw price data for anomaly detection models.

    Args:
        df: The raw DataFrame from the extraction step.

    Returns:
        A cleaned and prepared DataFrame.
    """
    if df.empty:
        logging.warning("Input DataFrame for preparation is empty.")
        return df

    # Ensure correct data types
    df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce')
    df['full_date'] = pd.to_datetime(df['full_date'], errors='coerce')
    
    # Drop rows with missing essential data
    original_rows = len(df)
    df.dropna(subset=['variant_id', 'current_price', 'full_date'], inplace=True)
    
    if len(df) < original_rows:
        logging.info(f"Dropped {original_rows - len(df)} rows with missing values.")

    return df

