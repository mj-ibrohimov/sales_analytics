from fastapi import FastAPI, Depends, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os
from pathlib import Path
from contextlib import asynccontextmanager

from app.core.database import get_db_session, init_database
from app.services.data_processor import DataProcessingService
from app.api.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup"""
    init_database()
    yield


app = FastAPI(
    title="Data Analytics Dashboard",
    description="Business Intelligence Dashboard for Sales Analytics",
    version="1.0.0",
    lifespan=lifespan
)

static_dir = Path("static")
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

app.include_router(router)


@app.get("/", response_class=HTMLResponse)
async def dashboard_homepage(request: Request, db=Depends(get_db_session)):
    """Main dashboard endpoint"""
    processor = DataProcessingService(db)
    processor.ensure_data_processed()
    
    dashboard_data = processor.get_dashboard_metrics()
    
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "metrics": dashboard_data}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=True
    )

