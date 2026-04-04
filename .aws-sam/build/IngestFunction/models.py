import uuid
from datetime import datetime, timezone


class WebhookMessage:
    def __init__(self, consumer_id, object_type, object_id,
                 event_type, priority, payload,
                 correlation_id="unknown", message_id=None):
        self.message_id = message_id or f"msg_{uuid.uuid4().hex[:12]}"
        self.correlation_id = correlation_id
        self.consumer_id = consumer_id
        self.object_type = object_type
        self.object_id = object_id
        self.event_type = event_type
        self.priority = priority
        self.payload = payload
        self.enqueued_at = datetime.now(timezone.utc).isoformat()

    def model_dump_json(self):
        import json
        return json.dumps({
            "message_id":     self.message_id,
            "correlation_id": self.correlation_id,
            "consumer_id":    self.consumer_id,
            "object_type":    self.object_type,
            "object_id":      self.object_id,
            "event_type":     self.event_type,
            "priority":       self.priority,
            "payload":        self.payload,
            "enqueued_at":    self.enqueued_at,
        })

    def model_dump(self):
        import json
        return json.loads(self.model_dump_json())

    @classmethod
    def model_validate_json(cls, data: str):
        import json
        d = json.loads(data)
        return cls(**{k: v for k, v in d.items()
                      if k in cls.__init__.__code__.co_varnames})


class EnrichedPayload:
    def __init__(self, message_id, correlation_id, consumer_id,
                 object_type, object_id, event_type,
                 priority, payload, enriched):
        self.message_id = message_id
        self.correlation_id = correlation_id
        self.consumer_id = consumer_id
        self.object_type = object_type
        self.object_id = object_id
        self.event_type = event_type
        self.priority = priority
        self.payload = payload
        self.enriched = enriched

    def model_dump(self):
        return {
            "message_id":     self.message_id,
            "correlation_id": self.correlation_id,
            "consumer_id":    self.consumer_id,
            "object_type":    self.object_type,
            "object_id":      self.object_id,
            "event_type":     self.event_type,
            "priority":       self.priority,
            "payload":        self.payload,
            "enriched":       self.enriched,
        }
