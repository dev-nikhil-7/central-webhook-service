import os

# AWS region
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# DynamoDB table names
CONSUMERS_TABLE = os.environ.get(
    "DYNAMO_CONSUMERS_TABLE",   "webhook-consumers")
IDEMPOTENCY_TABLE = os.environ.get(
    "DYNAMO_IDEMPOTENCY_TABLE", "webhook-idempotency")
MESSAGES_TABLE = os.environ.get("DYNAMO_MESSAGES_TABLE",    "webhook-messages")

# Validation
VALID_PRIORITIES = {1, 2, 3}
VALID_OBJECT_TYPES = {"sales_order", "inventory_update"}

# Delivery
SALESFORCE_URL = os.environ.get("SALESFORCE_URL", "https://webhook.site/test")
MAX_RETRIES = 5
RETRY_DELAYS = [30, 120, 600, 1800]  # seconds between retries
