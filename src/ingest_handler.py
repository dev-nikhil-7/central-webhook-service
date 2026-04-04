import hashlib
import json

import db_client as db
import queue_client as queue
from config import VALID_PRIORITIES, VALID_OBJECT_TYPES
from logger import StructuredLogger
from models import WebhookMessage

log = StructuredLogger("ingest")


def lambda_handler(event: dict, context) -> dict:
    try:

        body_raw = event.get("body", "{}")
        body = json.loads(body_raw) if isinstance(body_raw, str) else body_raw

        consumer_id = (
            event
            .get("requestContext", {})
            .get("authorizer", {})
            .get("consumer_id", "")
        )
        correlation_id = (
            event
            .get("requestContext", {})
            .get("requestId", "unknown")
        )

        if not consumer_id:
            log.warn("no_consumer", "No consumer_id in authoriser context")
            return _resp({"error": "INTERNAL_ERROR"}, 500)

        # Validate
        object_type = body.get("object_type", "")
        object_id = body.get("object_id", "")
        event_type = body.get("event_type", "")
        priority = body.get("priority", 2)

        if object_type not in VALID_OBJECT_TYPES:
            return _resp({"error": "INVALID_OBJECT_TYPE"}, 400)

        if priority not in VALID_PRIORITIES:
            return _resp({"error": "INVALID_PRIORITY"}, 400)

        if not object_id or not event_type:
            return _resp({"error": "MISSING_FIELDS"}, 400)

        log.info("received", "Webhook received",
                 consumer_id=consumer_id,
                 object_type=object_type,
                 event_type=event_type,
                 priority=priority,
                 correlation_id=correlation_id)

        # Idempotency check
        raw_key = f"{consumer_id}:{object_id}:{event_type}"
        idem_key = body.get("idempotency_key") or \
            __import__('hashlib').sha256(raw_key.encode()).hexdigest()

        if db.is_duplicate(idem_key):
            log.warn("duplicate", "Duplicate rejected",
                     consumer_id=consumer_id)
            return _resp({"error": "DUPLICATE"}, 409)

        # Build message
        message = WebhookMessage(
            consumer_id=consumer_id,
            correlation_id=correlation_id,
            object_type=object_type,
            object_id=object_id,
            event_type=event_type,
            priority=priority,
            payload=body.get("payload", {}),
        )

        # Prevents duplicate if Lambda is retried between these two steps
        db.write_idempotency_key(idem_key, message.message_id)
        db.write_message_record(
            message.message_id, consumer_id, status="queued")

        # Enqueue
        queue.enqueue(message)

        log.info("queued", "Message queued",
                 message_id=message.message_id,
                 priority=priority,
                 correlation_id=correlation_id)

        return _resp({
            "message_id":     message.message_id,
            "correlation_id": correlation_id,
            "status":         "queued",
        }, 202)

    except json.JSONDecodeError:
        return _resp({"error": "INVALID_JSON"}, 400)

    except Exception as e:
        log.error("error", str(e))
        return _resp({"error": "INTERNAL_ERROR"}, 500)


def _resp(body: dict, status_code: int) -> dict:
    return {
        "statusCode": status_code,
        "headers":    {"Content-Type": "application/json"},
        "body":       json.dumps(body),
    }
