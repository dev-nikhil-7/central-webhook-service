import json
from datetime import datetime, timezone


class StructuredLogger:

    def __init__(self, service: str):
        self.service = service

    def _log(self, level: str, stage: str, message: str, **kwargs):
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     level,
            "service":   self.service,
            "stage":     stage,
            "message":   message,
        }
        record.update(kwargs)
        print(json.dumps(record), flush=True)

    def info(self, stage: str, message: str, **kwargs):
        self._log("INFO", stage, message, **kwargs)

    def warn(self, stage: str, message: str, **kwargs):
        self._log("WARN", stage, message, **kwargs)

    def error(self, stage: str, message: str, **kwargs):
        self._log("ERROR", stage, message, **kwargs)
