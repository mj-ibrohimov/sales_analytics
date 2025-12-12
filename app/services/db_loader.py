"""Database loading utilities"""
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from app.core.database import DatabaseManager


class DatabaseLoader:
    """Utility class for loading data into database"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self._engine = None
    
    @property
    def engine(self):
        """Lazy load SQLAlchemy engine"""
        if self._engine is None:
            load_dotenv()
            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')
            db_name = os.getenv("DB_NAME")
            db_host = os.getenv("DB_HOST")
            
            self._engine = create_engine(
                f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}'
            )
        return self._engine
    
    def store_dataframe(self, df: pd.DataFrame, table_name: str, source: str):
        """Store dataframe in database if not already present"""
        existing = self.db.execute_query(
            f"SELECT COUNT(*) as cnt FROM {table_name} WHERE source = %s",
            (source,)
        )
        
        if existing[0]['cnt'] == 0:
            df.to_sql(table_name, self.engine, if_exists='append', index=False)


