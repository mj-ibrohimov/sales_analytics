"""API routes"""
from fastapi import APIRouter, Depends
from app.core.database import get_db_session

router = APIRouter()


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


