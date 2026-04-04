
import hashlib

import db_client as db
from logger import StructuredLogger

log = StructuredLogger("authoriser")


def lambda_handler(event: dict, context) -> dict:
    raw_key = event.get("authorizationToken", "")
    method_arn = event.get("methodArn", "*")

    # No key — deny immediately
    if not raw_key:
        log.warn("no_key", "Request has no API key")
        return _policy("Deny", method_arn)

    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    # Look up in DynamoDB
    consumer = db.lookup_consumer(key_hash)

    if not consumer:
        log.warn("invalid_key", "Key not found",
                 prefix=raw_key[:6] + "...")
        return _policy("Deny", method_arn)

    log.info("allowed", "Authorised",
             consumer_id=consumer["consumer_id"])

    return _policy("Allow", method_arn, consumer)


def _policy(effect: str, method_arn: str, consumer: dict = None) -> dict:
    policy = {
        "principalId": "consumer",
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [{
                "Action":   "execute-api:Invoke",
                "Effect":   effect,
                "Resource": method_arn,
            }],
        },
    }
    if effect == "Allow" and consumer:
        policy["context"] = {
            "consumer_id": consumer.get("consumer_id", "")
        }
    return policy
