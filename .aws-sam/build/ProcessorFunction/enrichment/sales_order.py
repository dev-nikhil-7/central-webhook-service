import asyncio

from enrichment.base import BaseAdapter
from logger import StructuredLogger
from models import EnrichedPayload, WebhookMessage

log = StructuredLogger("enrichment-sales-order")


async def fetch_customer(customer_id: str) -> dict:
    return {
        "id":     customer_id,
        "name":   "Acme Corp",
        "email":  "orders@acme.com",
        "region": "EMEA",
    }


async def fetch_products(line_items: list) -> list:
    return [
        {
            "product_id": item.get("product_id"),
            "name":       f"Product {item.get('product_id')}",
            "qty":        item.get("qty", 1),
        }
        for item in line_items
    ]


class SalesOrderAdapter(BaseAdapter):

    async def enrich(self, message: WebhookMessage) -> EnrichedPayload:
        customer_id = message.payload.get("customer_id", "")
        line_items = message.payload.get("line_items", [])
        partial = False
        customer = {}
        products = []

        try:
            # Both calls run in parallel
            customer, products = await asyncio.gather(
                fetch_customer(customer_id),
                fetch_products(line_items),
            )
        except Exception as e:
            log.warn("partial", "Enrichment partially failed",
                     message_id=message.message_id,
                     error=str(e))
            partial = True

        return EnrichedPayload(
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            consumer_id=message.consumer_id,
            object_type=message.object_type,
            object_id=message.object_id,
            event_type=message.event_type,
            priority=message.priority,
            payload=message.payload,
            enriched={
                "customer": customer,
                "products": products,
                "partial":  partial,
            },
        )
