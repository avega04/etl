from sqlalchemy import Column, DateTime, String, Integer, JSON, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from config.database import Base
import uuid

class BaseModel(Base):
    """Base model for all tables"""
    __abstract__ = True
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class RawDataModel(Base):
    """Base model for all raw data tables"""
    __abstract__ = True

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    created_at = Column(DateTime(timezone=True), server_default=text('now()'))
    updated_at = Column(DateTime(timezone=True), onupdate=text('now()'))
    raw_data = Column(JSON)
    source_id = Column(String(255), index=True)
    etl_batch_id = Column(String(36), index=True)
    status = Column(String(50), server_default='pending')
    error_message = Column(String(1000))
    retry_count = Column(Integer, server_default='0') 