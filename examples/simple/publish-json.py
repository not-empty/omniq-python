from dataclasses import dataclass
from typing import List, Optional

# importing the lib
from omniq.client import OmniqClient


# Nested structure
@dataclass
class Customer:
    id: str
    email: str
    vip: bool


# Main payload
@dataclass
class OrderCreated:
    order_id: str
    customer: Customer
    amount: int
    currency: str
    items: List[str]
    processed: bool
    retry_count: int
    tags: Optional[List[str]] = None


# creating OmniQ passing redis information
omniq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# creating structured payload
payload = OrderCreated(
    order_id="ORD-2026-0001",
    customer=Customer(
        id="CUST-99",
        email="leo@example.com",
        vip=True,
    ),
    amount=1500,
    currency="USD",
    items=["keyboard", "mouse"],
    processed=False,
    retry_count=0,
    tags=["priority", "online"],
)

# publish using publish_json
job_id = omniq.publish_json(
    queue="demo",
    payload=payload,
    max_attempts=5,
    timeout_ms=60_000,
)

print("OK", job_id)
