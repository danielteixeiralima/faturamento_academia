# filename: models.py
# Modelos do banco (comentários com algarismos árabe-índicos).
from datetime import datetime
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Text

Base = declarative_base()

class User(Base):
    __tablename__ = 'usuarios'
    id = Column(Integer, primary_key=True)  # chave primária (١)
    username = Column(String(150), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class UploadLog(Base):
    __tablename__ = 'uploads'
    id = Column(Integer, primary_key=True)  # chave primária (١)
    filename = Column(String(255), nullable=False)
    stored_path = Column(Text, nullable=False)
    extracted_to = Column(Text, nullable=True)
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    uploaded_by = Column(String(150), nullable=False)
