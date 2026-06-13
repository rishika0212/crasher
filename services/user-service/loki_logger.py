import os
import logging
import requests
import queue
import threading
import time

class LokiQueueHandler(logging.Handler):
    def __init__(self, loki_url, service_name):
        super().__init__()
        self.loki_url = loki_url
        self.service_name = service_name
        self.log_queue = queue.Queue()
        self.thread = threading.Thread(target=self._post_logs, daemon=True)
        self.thread.start()

    def emit(self, record):
        try:
            msg = self.format(record)
            level = record.levelname
            timestamp_ns = str(int(time.time() * 1e9))
            self.log_queue.put((timestamp_ns, level, msg))
        except Exception:
            self.handleError(record)

    def _post_logs(self):
        while True:
            batch = []
            try:
                item = self.log_queue.get(timeout=1.0)
                batch.append(item)
                while len(batch) < 100:
                    try:
                        batch.append(self.log_queue.get_nowait())
                    except queue.Empty:
                        break
            except queue.Empty:
                continue

            if not batch:
                continue

            streams = {}
            for timestamp_ns, level, msg in batch:
                key = (self.service_name, level)
                if key not in streams:
                    streams[key] = []
                streams[key].append([timestamp_ns, msg])

            payload = {
                "streams": [
                    {
                        "stream": {
                            "service": service_name,
                            "level": level
                        },
                        "values": values
                    }
                    for (service_name, level), values in streams.items()
                ]
            }

            try:
                requests.post(
                    self.loki_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=2.0
                )
            except Exception:
                pass

def setup_loki_logger(service_name: str):
    loki_url = os.getenv("LOKI_URL")
    if not loki_url:
        return
    
    logger = logging.getLogger()
    handler = LokiQueueHandler(loki_url, service_name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info(f"Loki logging initialized for service: {service_name}")
