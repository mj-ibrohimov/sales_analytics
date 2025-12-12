"""Data cleaning and transformation utilities"""
import pandas as pd
import numpy as np
import re


class DataCleaners:
    """Utility class for cleaning and transforming data"""
    
    MONTH_MAPPING = {
        'jan': '01', 'january': '01', 'feb': '02', 'february': '02',
        'mar': '03', 'march': '03', 'apr': '04', 'april': '04',
        'may': '05', 'jun': '06', 'june': '06', 'jul': '07', 'july': '07',
        'aug': '08', 'august': '08', 'sep': '09', 'september': '09',
        'oct': '10', 'october': '10', 'nov': '11', 'november': '11',
        'dec': '12', 'december': '12'
    }
    
    @staticmethod
    def normalize_years(series: pd.Series) -> pd.Series:
        """Normalize year values"""
        years = pd.to_numeric(series, errors='coerce')
        median_year = years.median()
        years = years.fillna(median_year).astype(int)
        years = years.replace(0, int(median_year))
        return years
    
    @staticmethod
    def clean_titles(series: pd.Series) -> pd.Series:
        """Clean book titles"""
        return (series
                .str.replace(r"'(\w+)'", r"\1", regex=True)
                .str.replace(r"''", r"'", regex=True)
                .str.replace(r'–', r'-', regex=True))
    
    @staticmethod
    def normalize_authors(series: pd.Series) -> pd.Series:
        """Normalize author names"""
        return (series
                .str.replace(r'\s+', ' ', regex=True)
                .str.strip())
    
    @staticmethod
    def clean_publishers(series: pd.Series) -> pd.Series:
        """Clean publisher names"""
        mode_publisher = series.mode()[0] if len(series.mode()) > 0 else "Unknown"
        return series.replace([" ", "", "NULL", None], mode_publisher)
    
    @staticmethod
    def standardize_phones(series: pd.Series) -> pd.Series:
        """Standardize phone number formats"""
        return (series
                .str.replace(r'[\)\.\s+]', '-', regex=True)
                .str.replace(r'\((\d+)--', r'\1-', regex=True))
    
    @staticmethod
    def process_pricing(df: pd.DataFrame) -> pd.DataFrame:
        """Process and convert prices to USD"""
        price_series = (df['unit_price']
                       .astype(str)
                       .str.replace(r'\s+', '', regex=True)
                       .str.replace(r'USD', '$', regex=True)
                       .str.replace(r'EUR', '€', regex=True)
                       .str.replace(r'\.$', '', regex=True)
                       .str.replace(r'(\d+)\$(\d+)¢', r'$\1.\2', regex=True)
                       .str.replace(r'(\d+)€(\d+)¢', r'€\1.\2', regex=True)
                       .str.replace(r'\¢', '.', regex=True)
                       .str.replace(r'(\d+\.?\d*)(\$)', r'\2\1', regex=True)
                       .str.replace(r'(\d+\.?\d*)(€)', r'\2\1', regex=True))
        
        is_eur = price_series.str.startswith("€")
        numeric_prices = pd.to_numeric(
            price_series.str.replace(r'[$\€]', '', regex=True),
            errors='coerce'
        )
        
        # Convert EUR to USD (1 EUR = 1.2 USD)
        numeric_prices = np.where(
            is_eur,
            (numeric_prices * 1.2).round(2),
            numeric_prices
        )
        
        df['price_per_item'] = numeric_prices
        return df
    
    @staticmethod
    def extract_dates(series: pd.Series) -> pd.Series:
        """Extract and normalize dates from various formats"""
        DATE_PATTERNS = {
            'iso': r'(\d{4}-\d{2}-\d{2})',
            'us_format': r'(\d{2}\/\d{2}\/\d{2})',
            'european': r'(\d+\.\d{2}\.\d{4})',
            'text_month': r'(\d+-\w+\-\d+)'
        }
        
        dates = series.copy()
        iso_dates = dates.str.extract(DATE_PATTERNS['iso'])[0]
        us_dates = dates.str.extract(DATE_PATTERNS['us_format'])[0]
        us_dates = us_dates.str.replace(r'(\d{2})\/(\d{2})\/(\d{2})', r'20\3-\1-\2', regex=True)
        eu_dates = dates.str.extract(DATE_PATTERNS['european'])[0]
        eu_dates = eu_dates.str.replace(r'(\d+)\.(\d{2})\.(\d{4})', r'\3-\2-\1', regex=True)
        text_dates = dates.str.extract(DATE_PATTERNS['text_month'])[0]
        text_dates = text_dates.apply(DataCleaners._parse_text_date)
        
        result = iso_dates.fillna(us_dates).fillna(eu_dates).fillna(text_dates)
        return pd.to_datetime(result, errors='coerce').dt.date
    
    @staticmethod
    def _parse_text_date(date_str: str) -> str:
        """Parse date string with text month"""
        if pd.isna(date_str):
            return None
        
        match = re.search(r'(\d+)-(\w+)-(\d{4})', str(date_str))
        if match:
            day, month, year = match.groups()
            month_num = DataCleaners.MONTH_MAPPING.get(month.lower()[:3], month)
            return f'{year}-{month_num}-{day.zfill(2)}'
        return None


