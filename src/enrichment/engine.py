from enrichment.base import BaseAdapter
from enrichment.inventory import InventoryAdapter
from enrichment.sales_order import SalesOrderAdapter
from logger import StructuredLogger
from models import EnrichedPayload, WebhookMessage

log = StructuredLogger("enrichment")

# Registry: object_type → adapter clas
REGISTRY = {
    "sales_order":      SalesOrderAdapter,
    "inventory_update": InventoryAdapter,
}


async def enrich(message: WebhookMessage) -> EnrichedPayload:
    adapter_class = REGISTRY.get(message.object_type, BaseAdapter)
    adapter = adapter_class()

    log.info("enriching", f"Using {adapter_class.__name__}",
             object_type=message.object_type,
             message_id=message.message_id)

    return await adapter.enrich(message)
