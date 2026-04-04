# Central Webhook Service

Central webhook ingestion and delivery service. Receives notifications
from multiple source systems, queues them with priority ordering,
enriches the payload, and delivers to Salesforce.

**Architecture decisions and design rationale - [DESIGN.md](DESIGN.md)**

## Prerequisites

```
Python 3.14
AWS CLI    - aws configure
SAM CLI    - pip install aws-sam-cli
```

---

## Deploy to AWS

```bash
# Clone the repo
git clone https://github.com/dev-nikhil-7/central-webhook-service.git
cd central-webhook-service

# Build and deploy
sam build
sam deploy --guided
```

After deploy, SAM prints your API endpoint:

```
Outputs:
  ApiEndpoint: https://oaf8rg5ie3.execute-api.us-east-1.amazonaws.com/dev/webhooks
```

---

## Register a test consumer

Before sending webhooks, seed a consumer record:

```bash
python scripts/register_consumer.py
```

This writes a consumer record to DynamoDB and prints the API key to use.

---

## Send a webhook

```bash
curl -X POST https://oaf8rg5ie3.execute-api.us-east-1.amazonaws.com/dev/webhooks \
  -H "X-API-Key: demo-order-mgmt-key" \
  -H "Content-Type: application/json" \
  -d '{
    "object_type": "sales_order",
    "object_id":   "ORD-001",
    "event_type":  "order.status_changed",
    "priority":    1,
    "payload": {
      "status":      "shipped",
      "customer_id": "CUST-42",
      "line_items":  [{"product_id": "PROD-001", "qty": 2}]
    }
  }'
```

Expected response:

```json
{
  "message_id": "msg_123",
  "correlation_id": "abc-123",
  "status": "queued"
}
```

## Observability

**Live structured logs in CloudWatch:**

Every log line is structured JSON:

```json
{
  "timestamp": "2026-04-04T10:30:01Z",
  "level": "INFO",
  "service": "processor",
  "stage": "delivered",
  "message_id": "msg_a1b2c3",
  "consumer_id": "order-mgmt",
  "correlation_id": "abc-123",
  "latency_ms": 142
}
```

## API Schema

Full OpenAPI 3.0 schema - [openapi.yaml](openapi.yaml)

---

## Architecture - [DESIGN.md](DESIGN.md)
