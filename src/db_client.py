import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from config import REGION, CONSUMERS_TABLE, IDEMPOTENCY_TABLE, MESSAGES_TABLE
from logger import StructuredLogger

log = StructuredLogger("db")

_dynamo = None


def get_dynamo():
    global _dynamo
    if _dynamo is None:
        _dynamo = boto3.resource("dynamodb", region_name=REGION)
    return _dynamo


# Used by Authoriser Lambda

def lookup_consumer(api_key_hash: str) -> dict | None:
    try:
        table = get_dynamo().Table(CONSUMERS_TABLE)
        resp = table.get_item(Key={"api_key_hash": api_key_hash})
        item = resp.get("Item")

        if not item:
            return None

        if item.get("status") != "active":
            log.warn("inactive", "Key found but not active")
            return None

        return item

    except ClientError as e:
        log.error("dynamo_error", str(e))
        return None


# Used by ingest Lambda

def is_duplicate(idempotency_key: str) -> bool:
    # Check if this event was already received. True = reject with 409
    try:
        table = get_dynamo().Table(IDEMPOTENCY_TABLE)
        resp = table.get_item(Key={"idempotency_key": idempotency_key})
        return "Item" in resp
    except ClientError:
        return False


def write_idempotency_key(idempotency_key: str, message_id: str):
    table = get_dynamo().Table(IDEMPOTENCY_TABLE)
    table.put_item(Item={
        "idempotency_key": idempotency_key,
        "message_id":      message_id,
        "created_at":      datetime.now(timezone.utc).isoformat(),
        "ttl":             int(time.time()) + (7 * 86400),
    })


def write_message_record(message_id: str, consumer_id: str, status: str = "queued"):
    table = get_dynamo().Table(MESSAGES_TABLE)
    table.put_item(Item={
        "message_id":  message_id,
        "consumer_id": consumer_id,
        "status":      status,
        "created_at":  datetime.now(timezone.utc).isoformat(),
    })


# Used by processor Lambda

def is_already_delivered(message_id: str) -> bool:
    try:
        table = get_dynamo().Table(MESSAGES_TABLE)
        resp = table.get_item(Key={"message_id": message_id})
        item = resp.get("Item")
        return item is not None and item.get("status") == "delivered"
    except ClientError:
        return False


def update_message_status(message_id: str, status: str, **extra):
    table = get_dynamo().Table(MESSAGES_TABLE)

    expr = "SET #s = :s, updated_at = :u"
    names = {"#s": "status"}
    values = {":s": status, ":u": datetime.now(timezone.utc).isoformat()}

    for k, v in extra.items():
        expr += f", {k} = :{k}"
        values[f":{k}"] = str(v)

    table.update_item(
        Key={"message_id": message_id},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )
