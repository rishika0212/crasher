from typing import Optional
from sqlmodel import Field, SQLModel

class User(SQLModel, table=True):
    __tablename__ = "users"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True, nullable=False)
    email: str = Field(nullable=False)
    full_name: str = Field(nullable=False)
    is_active: bool = Field(default=True)
