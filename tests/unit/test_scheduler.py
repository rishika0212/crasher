"""Unit tests for the chaos-engine ChaosScheduler."""

import time

import pytest

from _harness import import_isolated


@pytest.fixture
def Scheduler():
    mod = import_isolated("chaos-engine", module="scheduler")
    return mod.ChaosScheduler


def make(Scheduler):
    """Build a scheduler whose trigger records calls instead of firing chaos."""
    calls = []
    sched = Scheduler(trigger_func=lambda scenario, target: calls.append((scenario, target)))
    sched.active = False  # stop the background loop; we drive timing in the test
    return sched, calls


def test_add_job_registers_a_job(Scheduler):
    sched, _ = make(Scheduler)
    sched.add_job("cpu_spike", "order-service", 30)
    jobs = sched.get_jobs()
    assert len(jobs) == 1
    assert jobs[0]["scenario"] == "cpu_spike"
    assert jobs[0]["target"] == "order-service"
    assert jobs[0]["interval"] == 30


def test_add_job_is_idempotent_and_updates_interval(Scheduler):
    sched, _ = make(Scheduler)
    sched.add_job("cpu_spike", "order-service", 30)
    sched.add_job("cpu_spike", "order-service", 10)
    jobs = sched.get_jobs()
    assert len(jobs) == 1, "re-adding the same scenario+target must not duplicate"
    assert jobs[0]["interval"] == 10
    assert jobs[0]["last_run"] == 0, "interval change should reset last_run to trigger soon"


def test_distinct_targets_create_distinct_jobs(Scheduler):
    sched, _ = make(Scheduler)
    sched.add_job("cpu_spike", "order-service", 30)
    sched.add_job("cpu_spike", "user-service-1", 30)
    assert len(sched.get_jobs()) == 2


def test_remove_job(Scheduler):
    sched, _ = make(Scheduler)
    sched.add_job("cpu_spike", "order-service", 30)
    assert sched.remove_job("cpu_spike", "order-service") is True
    assert sched.get_jobs() == []
    assert sched.remove_job("cpu_spike", "order-service") is False


def test_clear_jobs(Scheduler):
    sched, _ = make(Scheduler)
    sched.add_job("cpu_spike", "a", 30)
    sched.add_job("network_delay", "b", 30)
    sched.clear_jobs()
    assert sched.get_jobs() == []


def test_get_jobs_returns_a_copy(Scheduler):
    sched, _ = make(Scheduler)
    sched.add_job("cpu_spike", "a", 30)
    snapshot = sched.get_jobs()
    snapshot.clear()
    assert len(sched.get_jobs()) == 1, "external mutation must not affect internal state"


def test_run_loop_fires_due_jobs():
    """The background loop should fire a job once its interval elapses."""
    mod = import_isolated("chaos-engine", module="scheduler")
    calls = []
    sched = mod.ChaosScheduler(trigger_func=lambda s, t: calls.append((s, t)))
    try:
        # last_run defaults to now, so a 0s interval is immediately due on the next tick.
        sched.add_job("cpu_spike", "order-service", 0)
        deadline = time.time() + 4
        while not calls and time.time() < deadline:
            time.sleep(0.1)
        assert calls, "scheduler did not fire a due job within the timeout"
        assert calls[0] == ("cpu_spike", "order-service")
    finally:
        sched.active = False
