"""Data processing service for ETL operations"""
import pandas as pd
import yaml
import re
from typing import Dict
from app.core.database import DatabaseManager
from app.services.data_cleaners import DataCleaners
from app.services.data_analyzers import DataAnalyzers
from app.services.chart_generator import ChartGenerator
from app.services.db_loader import DatabaseLoader


class DataProcessingService:
    """Service for processing and analyzing data"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.data_sources = ['DATA1', 'DATA2', 'DATA3']
        self.cleaners = DataCleaners()
        self.analyzers = DataAnalyzers()
        self.chart_gen = ChartGenerator()
        self.db_loader = DatabaseLoader(db_manager)
    
    def ensure_data_processed(self):
        """Ensure all data sources are processed"""
        for source in self.data_sources:
            if not self._is_processed(source):
                self._process_data_source(source)
    
    def _is_processed(self, source: str) -> bool:
        """Check if data source is already processed"""
        result = self.db.execute_query(
            "SELECT COUNT(*) as cnt FROM analytics_metrics WHERE data_source = %s",
            (source,)
        )
        return result[0]['cnt'] > 0 if result else False
    
    def _process_data_source(self, source: str):
        """Process a single data source"""
        self._process_books(source)
        self._process_customers(source)
        self._process_transactions(source)
    
    def _process_books(self, source: str):
        """Process books YAML file"""
        file_path = f"DATAs/{source}/books.yaml"
        
        with open(file_path, "r") as f:
            content = re.sub(r':(\w+):', r'\1:', f.read())
            yaml_data = yaml.safe_load(content)
        
        df = pd.DataFrame(yaml_data)
        
        # Clean and transform
        df['publication_year'] = self.cleaners.normalize_years(df['year'])
        df['book_title'] = self.cleaners.clean_titles(df['title'])
        df['authors'] = self.cleaners.normalize_authors(df['author'])
        df['publisher_name'] = self.cleaners.clean_publishers(df['publisher'])
        df['source'] = source
        
        # Calculate unique author sets
        author_sets_count = self.analyzers.count_author_combinations(df)
        
        # Prepare for database
        df_db = df[[
            'id', 'book_title', 'authors', 'genre', 
            'publisher_name', 'publication_year', 'source'
        ]].rename(columns={
            'id': 'book_id',
            'genre': 'category'
        })
        
        # Store in database
        self.db_loader.store_dataframe(df_db, 'book_catalog', source)
        
        # Store metric
        self.db.execute_query(
            """INSERT INTO analytics_metrics (metric_key, metric_value, data_source)
               VALUES (%s, %s, %s)
               ON CONFLICT (metric_key, data_source) DO UPDATE SET metric_value = EXCLUDED.metric_value""",
            ('unique_author_sets', str(author_sets_count), source)
        )
    
    def _process_customers(self, source: str):
        """Process customers CSV file"""
        df = pd.read_csv(f"DATAs/{source}/users.csv")
        
        df['contact_phone'] = self.cleaners.standardize_phones(df['phone'])
        
        # Finding duplicate customers
        df = self.analyzers.identify_customer_duplicates(df)
        
        df['source'] = source
        
        # Count unique customers
        unique_count = self.analyzers.count_unique_customers(df)
        
        # Prepare for database
        df_db = df[[
            'id', 'name', 'address', 'contact_phone', 
            'email', 'linked_customer_ids', 'source'
        ]].rename(columns={
            'id': 'customer_id',
            'name': 'customer_name',
            'address': 'delivery_address',
            'email': 'email_address'
        })
        
        # Store in database
        self.db_loader.store_dataframe(df_db, 'customer_profiles', source)
        
        # Store metric
        self.db.execute_query(
            """INSERT INTO analytics_metrics (metric_key, metric_value, data_source)
               VALUES (%s, %s, %s)
               ON CONFLICT (metric_key, data_source) DO UPDATE SET metric_value = EXCLUDED.metric_value""",
            ('unique_customers', str(unique_count), source)
        )
    
    def _process_transactions(self, source: str):
        """Process orders parquet file"""
        df = pd.read_parquet(f"DATAs/{source}/orders.parquet", engine="pyarrow")
        
        df = self.cleaners.process_pricing(df)
        
        df['total_amount'] = (df['price_per_item'] * df['quantity']).round(2)
        
        df['transaction_date'] = self.cleaners.extract_dates(df['timestamp'])
        
        df['delivery_method'] = self._get_shipping_addresses(df, source)
        
        df['source'] = source
        df['currency_code'] = 'USD'
        
        self.chart_gen.create_revenue_chart(df, source)
        
        df_db = df[[
            'user_id', 'book_id', 'quantity', 'price_per_item',
            'total_amount', 'transaction_date', 'delivery_method', 
            'source', 'currency_code'
        ]].rename(columns={
            'user_id': 'customer_id',
            'quantity': 'items_quantity'
        })
        
        # Store in database
        self.db_loader.store_dataframe(df_db, 'transaction_records', source)
    
    def _get_shipping_addresses(self, df: pd.DataFrame, source: str) -> pd.Series:
        """Get shipping addresses from customer data"""
        customers = self.db.execute_query(
            "SELECT customer_id, delivery_address FROM customer_profiles WHERE source = %s",
            (source,)
        )
        customer_dict = {row['customer_id']: row['delivery_address'] for row in customers}
        return df['user_id'].map(customer_dict)
    
    def get_dashboard_metrics(self) -> Dict[str, Dict]:
        """Get all dashboard metrics for all sources"""
        all_metrics = {}
        
        for source in self.data_sources:
            metrics = self._get_source_metrics(source)
            all_metrics[source] = metrics
        
        return all_metrics
    
    def _get_source_metrics(self, source: str) -> Dict:
        """Get metrics for a specific source"""
        # Top 5 revenue days
        top_days = self.db.execute_query("""
            SELECT TO_CHAR(transaction_date, 'YYYY-MM-dd') as date, 
                   SUM(total_amount) as revenue,
                   source
            FROM transaction_records
            WHERE source = %s
              AND transaction_date IS NOT NULL
            GROUP BY transaction_date, source
            ORDER BY revenue DESC NULLS LAST
            LIMIT 5
        """, (source,))
        
        # Unique users count
        unique_users = self.db.execute_query(
            "SELECT metric_value FROM analytics_metrics WHERE metric_key = 'unique_customers' AND data_source = %s",
            (source,)
        )
        users_count = int(unique_users[0]['metric_value']) if unique_users else 0
        
        # Unique author sets
        author_sets = self.db.execute_query(
            "SELECT metric_value FROM analytics_metrics WHERE metric_key = 'unique_author_sets' AND data_source = %s",
            (source,)
        )
        author_sets_count = int(author_sets[0]['metric_value']) if author_sets else 0
        
        # Most popular author
        popular_author = self.db.execute_query("""
            SELECT b.authors as author_name,
                   COUNT(t.items_quantity) as books_sold,
                   t.source
            FROM transaction_records t
            JOIN book_catalog b ON t.book_id = b.book_id AND t.source = b.source
            WHERE t.source = %s
            GROUP BY b.authors, t.source
            ORDER BY books_sold DESC
            LIMIT 1
        """, (source,))
        
        # Top customer
        top_customer = self.db.execute_query("""
            SELECT c.customer_id,
                   c.customer_name,
                   SUM(t.total_amount) as total_spent,
                   c.linked_customer_ids,
                   t.source
            FROM transaction_records t
            JOIN customer_profiles c ON t.customer_id = c.customer_id AND t.source = c.source
            WHERE t.source = %s
            GROUP BY c.customer_id, c.customer_name, c.linked_customer_ids, t.source
            ORDER BY total_spent DESC
            LIMIT 1
        """, (source,))
        
        # Convert Decimal to float for JSON serialization
        top_days_processed = []
        for day in top_days:
            day_dict = dict(day)
            if day_dict.get('date') is None:
                continue
            day_dict['revenue'] = float(day_dict['revenue'])
            top_days_processed.append(day_dict)
        
        popular_author_dict = {}
        if popular_author:
            pa = dict(popular_author[0])
            popular_author_dict = {
                'author_name': pa.get('author_name', ''),
                'books_sold': int(pa.get('books_sold', 0))
            }
        
        top_customer_dict = {}
        if top_customer:
            tc = dict(top_customer[0])
            # Build array of customer IDs
            customer_ids = [str(tc.get('customer_id', ''))]
            if tc.get('linked_customer_ids'):
                linked_ids = [id.strip() for id in str(tc['linked_customer_ids']).split(',') if id.strip()]
                customer_ids.extend(linked_ids)
            
            top_customer_dict = {
                'customer_id': int(tc.get('customer_id', 0)),
                'customer_name': tc.get('customer_name', ''),
                'total_spent': float(tc.get('total_spent', 0)),
                'linked_customer_ids': tc.get('linked_customer_ids', ''),
                'customer_ids_array': customer_ids
            }
        
        return {
            'top_revenue_days': top_days_processed,
            'unique_users': users_count,
            'unique_author_sets': author_sets_count,
            'popular_author': popular_author_dict,
            'top_customer': top_customer_dict
        }
