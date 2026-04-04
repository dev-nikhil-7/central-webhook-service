import asyncio

from enrichment.base import BaseAdapter
from models import EnrichedPayload, WebhookMessage


async def fetch_warehouse(warehouse_id: str) -> dict:
    return {
        "id":       warehouse_id,
        "name":     f"Warehouse {warehouse_id}",
        "location": "UK"
    }


async def fetch_sku(sku_id: str) -> dict:
    return {
        "id":       sku_id,
        "name":     f"SKU {sku_id}",
        "category": "electronics"
    }


class InventoryAdapter(BaseAdapter):

    async def enrich(self, message: WebhookMessage) -> EnrichedPayload:
        warehouse_id = message.payload.get("warehouse_id", "")
        sku_id = message.payload.get("sku_id", "")

        warehouse, sku = await asyncio.gather(
            fetch_warehouse(warehouse_id),
            fetch_sku(sku_id),
        )

        return EnrichedPayload(
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            consumer_id=message.consumer_id,
            object_type=message.object_type,
            object_id=message.object_id,
            event_type=message.event_type,
            priority=message.priority,
            payload=message.payload,
            enriched={"warehouse": warehouse, "sku": sku, "partial": False},
        )
