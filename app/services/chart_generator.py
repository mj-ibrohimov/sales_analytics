"""Chart generation utilities"""
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


class ChartGenerator:
    """Utility class for generating charts"""
    
    @staticmethod
    def create_revenue_chart(df: pd.DataFrame, source: str):
        """Create revenue chart"""
        os.makedirs('static/images', exist_ok=True)
        
        daily_revenue = df.groupby('transaction_date')['total_amount'].sum()
        
        plt.figure(figsize=(12, 6))
        plt.plot(daily_revenue.index, daily_revenue.values, linewidth=2, color='#2563eb')
        plt.title(f'Daily Revenue Trend - {source}', fontsize=16, fontweight='bold')
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Revenue (USD)', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'static/images/revenue_chart_{source}.png', dpi=300, bbox_inches='tight')
        plt.close()


