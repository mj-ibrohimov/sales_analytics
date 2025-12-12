"""Database connection and session management"""
import os
from contextlib import contextmanager
from typing import Generator
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv

load_dotenv()


class DatabaseManager:
    """Manages database connections using connection pooling"""
    
    def __init__(self):
        self.pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize connection pool"""
        try:
            self.pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                port=5432
            )
        except Exception as e:
            print(f"Error creating connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self) -> Generator:
        """Get a connection from the pool"""
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)
    
    def execute_query(self, query: str, params: tuple = None) -> list:
        """Execute a query and return results as list of dicts"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                if cur.description:
                    return [dict(row) for row in cur.fetchall()]
                conn.commit()
                return []
    
    def execute_many(self, query: str, params_list: list):
        """Execute query with multiple parameter sets"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, params_list)
                conn.commit()


# Global database manager instance
_db_manager = None


def get_db_manager() -> DatabaseManager:
    """Get database manager singleton"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def get_db_session():
    """Dependency for FastAPI to get database session"""
    return get_db_manager()


def init_database():
    """Initialize database tables"""
    manager = get_db_manager()
    
    tables_sql = [
        """
        CREATE TABLE IF NOT EXISTS analytics_metrics (
            metric_key VARCHAR(100),
            metric_value TEXT,
            data_source VARCHAR(10),
            PRIMARY KEY (metric_key, data_source)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS book_catalog (
            book_id BIGINT,
            book_title TEXT,
            authors TEXT,
            category TEXT,
            publisher_name TEXT,
            publication_year INTEGER,
            source VARCHAR(10),
            PRIMARY KEY (book_id, source)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS customer_profiles (
            customer_id BIGINT,
            customer_name TEXT,
            delivery_address TEXT,
            contact_phone TEXT,
            email_address TEXT,
            linked_customer_ids TEXT,
            source VARCHAR(10),
            PRIMARY KEY (customer_id, source)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS transaction_records (
            customer_id BIGINT,
            book_id BIGINT,
            items_quantity INTEGER,
            price_per_item DECIMAL(10, 2),
            total_amount DECIMAL(10, 2),
            transaction_date DATE,
            delivery_method TEXT,
            source VARCHAR(10),
            currency_code VARCHAR(3)
        )
        """
    ]
    
    for sql in tables_sql:
        manager.execute_query(sql)


