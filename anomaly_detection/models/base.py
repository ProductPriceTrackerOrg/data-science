from abc import ABC, abstractmethod
import pandas as pd

class AnomalyDetectorBase(ABC):
    """
    Abstract base class for all anomaly detection models.
    It ensures that any new detector we create will have a consistent interface.
    """

    def __init__(self, config: dict):
        """
        Initializes the detector with a configuration dictionary.
        
        Args:
            config: A dictionary containing all parameters, typically loaded from a YAML file.
        """
        self.config = config

    @abstractmethod
    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        The core method to detect anomalies in the provided data.

        Args:
            data: A pandas DataFrame containing the price data to be analyzed.

        Returns:
            A pandas DataFrame containing only the rows that have been identified as anomalies,
            formatted to match the schema of the FactPriceAnomaly table.
        """
        pass

