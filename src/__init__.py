"""
Product Price Tracker - Data Science Package

This package provides tools for scraping, analyzing, and modeling product price data.
"""

from .scrape_data import PriceHistoryScraper

# Package metadata
__version__ = "1.0.0"
__author__ = "Product Price Tracker Team"
__description__ = "Data science tools for product price tracking and analysis"

# Main exports
__all__ = [
    'PriceHistoryScraper'
]

