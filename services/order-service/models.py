from typing import Optional
from sqlmodel import Field, SQLModel

class Order(SQLModel, table=True):
    __tablename__ = "orders"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(nullable=False)
    product: str = Field(nullable=False)
    quantity: int = Field(nullable=False, default=1)
    price: float = Field(nullable=False)
    status: str = Field(default="pending")
