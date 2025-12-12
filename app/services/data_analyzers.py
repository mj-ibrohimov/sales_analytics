"""Data analysis and metric calculation utilities"""
import pandas as pd
from typing import Dict


class DataAnalyzers:
    """Utility class for analyzing data and calculating metrics"""
    
    @staticmethod
    def count_author_combinations(df: pd.DataFrame) -> int:
        """Count unique author combinations"""
        author_sets = df['authors'].str.split(", ").apply(
            lambda x: tuple(sorted(x)) if isinstance(x, list) else tuple()
        )
        return author_sets.nunique()
    
    @staticmethod
    def identify_customer_duplicates(df: pd.DataFrame) -> pd.DataFrame:
        """Identify duplicate customers based on various field combinations"""
        df = df.copy()
        df['linked_customer_ids'] = ''
        
        combinations = [
            ['name', 'address', 'phone'],
            ['name', 'address', 'email'],
            ['name', 'phone', 'email'],
            ['address', 'phone', 'email']
        ]
        
        for combo in combinations:
            grouped = df.groupby(combo)['id'].apply(list)
            for ids in grouped:
                if len(ids) > 1:
                    for idx in df.index:
                        if df.loc[idx, 'id'] in ids:
                            current_ids = df.loc[idx, 'linked_customer_ids']
                            other_ids = [str(i) for i in ids if i != df.loc[idx, 'id']]
                            if current_ids:
                                all_ids = set(current_ids.split(',')) | set(other_ids)
                                df.loc[idx, 'linked_customer_ids'] = ','.join(sorted(all_ids))
                            else:
                                df.loc[idx, 'linked_customer_ids'] = ','.join(sorted(other_ids))
        
        return df
    
    @staticmethod
    def count_unique_customers(df: pd.DataFrame) -> int:
        """Count truly unique customers"""
        df_unique = df.drop_duplicates(subset=['name', 'address', 'phone'], keep='first')
        df_unique = df_unique.drop_duplicates(subset=['name', 'address', 'email'], keep='first')
        df_unique = df_unique.drop_duplicates(subset=['name', 'phone', 'email'], keep='first')
        df_unique = df_unique.drop_duplicates(subset=['address', 'phone', 'email'], keep='first')
        return len(df_unique)


