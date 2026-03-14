import json
from datetime import datetime, timezone


def lambda_handler(event, context):
    print("Event:", json.dumps(event))

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "message": "Hello from Lambda!",
            "path": event.get("path"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }),
    }
