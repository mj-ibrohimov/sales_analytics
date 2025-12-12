from pydantic import BaseModel
from typing import List, Optional
from datetime import date


class RevenueDay(BaseModel):
    date: str
    revenue: float
    source: str


class AuthorMetrics(BaseModel):
    author_name: str
    books_sold: int
    source: str


class CustomerMetrics(BaseModel):
    customer_id: int
    customer_name: str
    total_spent: float
    linked_ids: List[int]
    source: str


class DashboardMetrics(BaseModel):
    top_revenue_days: List[RevenueDay]
    unique_users_count: int
    unique_author_sets_count: int
    most_popular_author: AuthorMetrics
    top_customer: CustomerMetrics
    source: str


