import time
import logging
import threading
from typing import List, Dict, Callable

logger = logging.getLogger("chaos-scheduler")

class ChaosScheduler:
    def __init__(self, trigger_func: Callable[[str, str], None]):
        self.trigger_func = trigger_func
        self.jobs: List[Dict] = []
        self.lock = threading.Lock()
        self.active = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def add_job(self, scenario: str, target: str, interval_seconds: int):
        with self.lock:
            # Check if job already exists
            for job in self.jobs:
                if job["scenario"] == scenario and job["target"] == target:
                    job["interval"] = interval_seconds
                    job["last_run"] = 0 # reset last run to trigger soon
                    logger.info(f"Updated scheduled chaos: {scenario} on {target} every {interval_seconds}s")
                    return
            
            # Add new job
            self.jobs.append({
                "scenario": scenario,
                "target": target,
                "interval": interval_seconds,
                "last_run": time.time() # don't trigger immediately, wait for next tick
            })
            logger.info(f"Scheduled new chaos: {scenario} on {target} every {interval_seconds}s")

    def remove_job(self, scenario: str, target: str):
        with self.lock:
            original_len = len(self.jobs)
            self.jobs = [j for j in self.jobs if not (j["scenario"] == scenario and j["target"] == target)]
            if len(self.jobs) < original_len:
                logger.info(f"Removed scheduled chaos: {scenario} on {target}")
                return True
            return False

    def clear_jobs(self):
        with self.lock:
            self.jobs.clear()
            logger.info("Cleared all scheduled chaos jobs.")

    def get_jobs(self):
        with self.lock:
            return list(self.jobs)

    def _run_loop(self):
        logger.info("Chaos scheduler background thread active.")
        while self.active:
            now = time.time()
            jobs_to_run = []
            
            with self.lock:
                for job in self.jobs:
                    if now - job["last_run"] >= job["interval"]:
                        jobs_to_run.append(job)
                        job["last_run"] = now
            
            for job in jobs_to_run:
                try:
                    logger.info(f"Scheduler triggering: {job['scenario']} on {job['target']}")
                    self.trigger_func(job["scenario"], job["target"])
                except Exception as e:
                    logger.error(f"Scheduler failed to run job {job['scenario']}: {e}")
                    
            time.sleep(1.0)
