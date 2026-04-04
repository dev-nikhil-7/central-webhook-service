# scripts/register_consumer.py
import boto3
import hashlib

# Replace with your actual region and account ID
REGION = "us-east-1"
ACCOUNT_ID = "766099558744"

dynamo = boto3.resource("dynamodb", region_name=REGION)
table = dynamo.Table("webhook-consumers-dev")

raw_key = "demo-order-mgmt-key"
hashed_key = hashlib.sha256(raw_key.encode()).hexdigest()

table.put_item(Item={
    "api_key_hash":  hashed_key,
    "consumer_id":   "order-mgmt",
    "name":          "Order Management System",
    "status":        "active",
    "queue_urls": {
        "p1":  f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/order-mgmt-p1.fifo",
        "p2":  f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/order-mgmt-p2.fifo",
        "p3":  f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/order-mgmt-p3.fifo",
        "dlq": f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/order-mgmt-dlq.fifo",
    },
    "registered_at": "2026-04-04T10:00:00Z",
})

print(f"Consumer registered successfully")
print(f"Use this API key in your requests: {raw_key}")
