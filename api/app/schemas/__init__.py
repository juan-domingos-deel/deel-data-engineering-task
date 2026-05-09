from datetime import date, datetime
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field


T = TypeVar("T")


class Meta(BaseModel):
    count: int
    generated_at: datetime


class Envelope(BaseModel, Generic[T]):
    data: list[T]
    meta: Meta


class OrdersByDateStatusRow(BaseModel):
    delivery_date: date | None
    status: str
    order_count: int


class OrdersByDateRow(BaseModel):
    delivery_date: date | None
    order_count: int


class OpenItemsByProductRow(BaseModel):
    product_id: int
    product_name: str
    open_item_count: int


class CustomersRow(BaseModel):
    customer_id: int
    customer_name: str
    order_count: int


class ErrorBody(BaseModel):
    type: str
    message: str
    request_id: str


class ErrorEnvelope(BaseModel):
    error: ErrorBody
    model_config = ConfigDict(json_schema_extra={"example": {
        "error": {"type": "validation_error", "message": "limit must be ≤ 100", "request_id": "..."}
    }})
