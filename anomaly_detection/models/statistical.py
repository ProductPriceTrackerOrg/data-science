import pandas as pd
from datetime import datetime, timezone
from .base import AnomalyDetectorBase

class ModifiedZScoreDetector(AnomalyDetectorBase):
    """
    Detects anomalies using the Modified Z-score, which is robust to outliers.
    It uses the median and Median Absolute Deviation (MAD) instead of the mean/std dev.
    """

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        print("Running Modified Z-score anomaly detection...")
        if data.empty:
            return pd.DataFrame()

        config = self.config['modified_z_score']
        threshold = config['threshold']
        min_data_points = config['min_data_points']
        model_id = config['model_id']
        
        anomaly_results = []

        for variant_id, group in data.groupby('variant_id'):
            group = group.sort_values('full_date', ascending=False).reset_index(drop=True)

            if len(group) < min_data_points:
                continue

            historical_prices = group['current_price'].iloc[1:]
            
            median_price = historical_prices.median()
            median_absolute_deviation = (historical_prices - median_price).abs().median()

            if median_absolute_deviation == 0:
                continue

            latest_price_record = group.iloc[0]
            latest_price = latest_price_record['current_price']
            
            mod_z_score = 0.6745 * (latest_price - median_price) / median_absolute_deviation

            if abs(mod_z_score) > threshold:
                anomaly_type = 'PRICE_SPIKE' if mod_z_score > 0 else 'PRICE_DROP'
                
                anomaly_record = latest_price_record.to_frame().T
                anomaly_record['model_id'] = model_id
                anomaly_record['anomaly_score'] = abs(mod_z_score)
                anomaly_record['anomaly_type'] = anomaly_type
                anomaly_record['created_at'] = datetime.now(timezone.utc)
                
                anomaly_results.append(anomaly_record)
        
        if not anomaly_results:
            print("No anomalies detected by Modified Z-score.")
            return pd.DataFrame()
        
        final_anomalies_df = pd.concat(anomaly_results, ignore_index=True)
        print(f"Modified Z-score detection complete. Found {len(final_anomalies_df)} anomalies.")
        return final_anomalies_df


class MovingAverageDetector(AnomalyDetectorBase):
    """
    Detects anomalies by comparing the latest price to its recent moving average.
    """
    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        print("Running Moving Average anomaly detection...")
        if data.empty:
            return pd.DataFrame()

        config = self.config['moving_average']
        window_size = config['window_size']
        threshold_percentage = config['threshold_percentage']
        model_id = config['model_id']

        min_data_points = window_size + 1
        anomaly_results = []

        for variant_id, group in data.groupby('variant_id'):
            group = group.sort_values('full_date', ascending=False).reset_index(drop=True)

            if len(group) < min_data_points:
                continue

            latest_price_record = group.iloc[0]
            latest_price = latest_price_record['current_price']
            
            historical_prices = group['current_price'].iloc[1:min_data_points]
            moving_average = historical_prices.mean()

            if moving_average == 0:
                continue

            percentage_change = ((latest_price - moving_average) / moving_average) * 100

            if abs(percentage_change) > threshold_percentage:
                anomaly_type = 'PRICE_SPIKE' if percentage_change > 0 else 'PRICE_DROP'
                
                anomaly_record = latest_price_record.to_frame().T
                anomaly_record['model_id'] = model_id
                anomaly_record['anomaly_score'] = abs(percentage_change)
                anomaly_record['anomaly_type'] = anomaly_type
                anomaly_record['created_at'] = datetime.now(timezone.utc)
                
                anomaly_results.append(anomaly_record)

        if not anomaly_results:
            print("No anomalies detected by Moving Average method.")
            return pd.DataFrame()

        final_anomalies_df = pd.concat(anomaly_results, ignore_index=True)
        print(f"Moving Average detection complete. Found {len(final_anomalies_df)} anomalies.")
        return final_anomalies_df

