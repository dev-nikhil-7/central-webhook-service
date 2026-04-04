import json

import boto3

from config import REGION
from logger import StructuredLogger
from models import WebhookMessage

log = StructuredLogger("queue")

_sqs = None


def get_sqs():
    global _sqs
    if _sqs is None:
        _sqs = boto3.client("sqs", region_name=REGION)
    return _sqs


def get_queue_url(consumer_id: str, priority: int) -> str:
    return get_sqs().get_queue_url(
        QueueName=f"{consumer_id}-p{priority}.fifo"
    )["QueueUrl"]


def enqueue(message: WebhookMessage) -> str:
    """Write message to the correct priority queue."""
    sqs = get_sqs()
    queue_url = get_queue_url(message.consumer_id, message.priority)

    resp = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message.model_dump_json(),
        MessageGroupId=message.consumer_id,
        MessageDeduplicationId=message.message_id,
    )

    log.info("enqueued", "Message written to SQS",
             message_id=message.message_id,
             priority=message.priority)

    return resp["MessageId"]
