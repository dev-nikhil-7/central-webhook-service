import json
import time
import urllib.request
import urllib.error
import db_client as db
from config import SALESFORCE_URL
from logger import StructuredLogger
from models import WebhookMessage, EnrichedPayload

log = StructuredLogger("processor")


def enrich_message(message: WebhookMessage) -> EnrichedPayload:

    if message.object_type == "sales_order":
        customer_id = message.payload.get("customer_id", "")
        line_items = message.payload.get("line_items", [])

        enriched = {
            "customer": {
                "id":     customer_id,
                "name":   "Acme Corp",
                "email":  "orders@acme.com",
                "region": "EMEA",
            },
            "products": [
                {
                    "product_id": item.get("product_id"),
                    "name":       f"Product {item.get('product_id')}",
                    "qty":        item.get("qty", 1),
                }
                for item in line_items
            ],
            "partial": False,
        }

    elif message.object_type == "inventory_update":
        enriched = {
            "warehouse": {
                "id":       message.payload.get("warehouse_id"),
                "name":     "Main Warehouse",
                "location": "UK",
            },
            "sku": {
                "id":       message.payload.get("sku_id"),
                "name":     f"SKU {message.payload.get('sku_id')}",
                "category": "electronics",
            },
            "partial": False,
        }

    else:
        enriched = {"note": "no enrichment for this type"}

    return EnrichedPayload(
        message_id=message.message_id,
        correlation_id=message.correlation_id,
        consumer_id=message.consumer_id,
        object_type=message.object_type,
        object_id=message.object_id,
        event_type=message.event_type,
        priority=message.priority,
        payload=message.payload,
        enriched=enriched,
    )


def deliver_message(payload: EnrichedPayload) -> bool:
    log.info("delivering", "Posting to Salesforce",
             message_id=payload.message_id,
             url=SALESFORCE_URL)
    try:
        data = json.dumps(payload.model_dump()).encode("utf-8")
        req = urllib.request.Request(
            SALESFORCE_URL,
            data=data,
            headers={
                "Content-Type": "application/json",
                "X-Message-ID": payload.message_id,
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info("response", f"Got {resp.status}",
                     message_id=payload.message_id)
            return resp.status in (200, 201, 202)

    except Exception as e:
        log.error("delivery_error", str(e),
                  message_id=payload.message_id)
        return False


def process_one(message: WebhookMessage) -> bool:
    start = time.monotonic()

    log.info("processing", "Processing message",
             message_id=message.message_id,
             priority=message.priority)

    if db.is_already_delivered(message.message_id):
        log.info("skip", "Already delivered",
                 message_id=message.message_id)
        return True

    # Enrich
    try:
        enriched = enrich_message(message)
        log.info("enriched", "Enrichment complete",
                 message_id=message.message_id)
    except Exception as e:
        log.error("enrich_error", str(e),
                  message_id=message.message_id)
        return False

    # Deliver
    if not deliver_message(enriched):
        log.error("deliver_failed", "Delivery failed",
                  message_id=message.message_id)
        return False

    # Mark delivered
    latency = int((time.monotonic() - start) * 1000)
    db.update_message_status(
        message.message_id, "delivered", latency_ms=latency
    )

    log.info("done", "Message delivered",
             message_id=message.message_id,
             latency_ms=latency)

    return True


def lambda_handler(event: dict, context) -> dict:
    records = event.get("Records", [])

    log.info("batch", f"Received {len(records)} records")

    # Sort by priority — P1 processed before P2/P3
    records.sort(
        key=lambda r: json.loads(r["body"]).get("priority", 2)
    )

    failed = []

    for record in records:
        try:
            message = WebhookMessage.model_validate_json(record["body"])
            success = process_one(message)

            if not success:
                failed.append({"itemIdentifier": record["messageId"]})

        except Exception as e:
            log.error("record_error", str(e))
            failed.append({"itemIdentifier": record["messageId"]})

    return {"batchItemFailures": failed}
