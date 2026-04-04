from models import EnrichedPayload, WebhookMessage


class BaseAdapter:

    async def enrich(self, message: WebhookMessage) -> EnrichedPayload:
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
                "note": "no enrichment available for this object_type"
            },
        )
