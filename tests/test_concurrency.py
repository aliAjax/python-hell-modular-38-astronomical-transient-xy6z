import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class ConcurrencyFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repository = SQLiteRepository(Path(self.tmp.name) / "concurrency.db")
        self.service = DomainService(self.repository, RuleEngine())
        self.analyst = Actor("analyst-1", "analyst")
        self.coordinator = Actor("coordinator-1", "coordinator")
        self.supervisor = Actor("supervisor-1", "supervisor")
        source = self.service.create(
            self.analyst, "source", {"name": "Survey", "survey_name": "S"}
        )
        self.source_id = source["id"]
        self.telescope = self.service.create(
            self.coordinator,
            "telescope",
            {"name": "North 2m", "aperture_m": 2.0, "site_name": "NAO"},
        )
        self.telescope2 = self.service.create(
            self.coordinator,
            "telescope",
            {"name": "South 4m", "aperture_m": 4.0, "site_name": "ESO"},
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _candidate(self, event_id, magnitude=18.0, transient_type="unknown"):
        return self.service.create(
            self.analyst,
            "candidate",
            {
                "source_id": self.source_id,
                "event_id": event_id,
                "ra": 10,
                "dec": 20,
                "magnitude": magnitude,
                "transient_type": transient_type,
                "observed_at": "2026-09-27T00:00:00Z",
            },
        )

    def _observation(self, candidate_id, team_id, start, end, telescope_id=None):
        return self.service.create(
            self.coordinator,
            "observation",
            {
                "candidate_id": candidate_id,
                "telescope_id": telescope_id or self.telescope["id"],
                "team_id": team_id,
                "start_at": start,
                "end_at": end,
                "mode": "imaging",
            },
        )


class MeasurementMergeConcurrencyTest(ConcurrencyFixture):
    def test_concurrent_merges_keep_both_measurements_and_one_wins(self):
        candidate = self._candidate("AT-MERGE")

        barrier = threading.Barrier(2)

        def merge(ts, magnitude):
            barrier.wait()
            return self.service.transition(
                self.analyst,
                candidate["id"],
                "merge_measurement",
                {
                    "measurement": {
                        "observed_at": ts,
                        "ra": 10.01,
                        "dec": 20.01,
                        "magnitude": magnitude,
                    }
                },
                expected_version=candidate["version"],
            )

        outcomes = {}

        def run(name, ts, magnitude):
            try:
                outcomes[name] = ("ok", merge(ts, magnitude))
            except ConflictError as exc:
                outcomes[name] = ("conflict", str(exc))

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(run, "first", "2026-09-27T01:00:00Z", 17.5),
                pool.submit(run, "second", "2026-09-27T02:00:00Z", 16.0),
            ]
            for future in futures:
                future.result()

        statuses = {name: value[0] for name, value in outcomes.items()}
        self.assertEqual(sorted(statuses.values()), ["conflict", "ok"])
        winner = next(value[1] for value in outcomes.values() if value[0] == "ok")
        # Winner advanced the version exactly once.
        self.assertEqual(winner["version"], 2)

        stored = self.service.get(candidate["id"])
        # Only the winner's write landed; the loser changed nothing.
        self.assertEqual(len(stored["data"]["measurements"]), 2)
        submissions = {
            "first": "2026-09-27T01:00:00Z",
            "second": "2026-09-27T02:00:00Z",
        }
        winner_name = next(
            name for name, value in outcomes.items() if value[0] == "ok"
        )
        loser_name = next(
            name for name, value in outcomes.items() if value[0] == "conflict"
        )
        merged_timestamps = {m["observed_at"] for m in stored["data"]["measurements"]}
        self.assertIn(submissions[winner_name], merged_timestamps)
        self.assertNotIn(submissions[loser_name], merged_timestamps)

        # The loser retries against the current version and its measurement is
        # retained too - nobody overwrites the other's value.
        fresh = self.service.get(candidate["id"])
        retry_ts = "2026-09-27T03:00:00Z"
        retried = self.service.transition(
            Actor("analyst-2", "analyst"),
            fresh["id"],
            "merge_measurement",
            {
                "measurement": {
                    "observed_at": retry_ts,
                    "ra": 10.02,
                    "dec": 20.02,
                    "magnitude": 15.5,
                }
            },
            expected_version=fresh["version"],
        )
        self.assertEqual(len(retried["data"]["measurements"]), 3)
        self.assertEqual(retried["version"], 3)
        self.assertEqual(
            {m["observed_at"] for m in retried["data"]["measurements"]},
            {
                "2026-09-27T00:00:00Z",
                submissions[winner_name],
                retry_ts,
            },
        )

        actions = [entry["action"] for entry in self.service.audit_log(candidate["id"])]
        self.assertEqual(actions.count("merge_measurement"), 2)


class SchedulingConcurrencyTest(ConcurrencyFixture):
    def test_same_telescope_window_is_admitted_once(self):
        candidate_a = self._candidate("AT-A", magnitude=18.0)
        candidate_b = self._candidate("AT-B", magnitude=19.0)
        obs_a = self._observation(
            candidate_a["id"], "team-a", "2026-09-28T10:00:00Z", "2026-09-28T11:00:00Z"
        )
        obs_b = self._observation(
            candidate_b["id"], "team-b", "2026-09-28T10:30:00Z", "2026-09-28T11:30:00Z"
        )

        barrier = threading.Barrier(2)
        outcomes = {}

        def schedule(name, obs_id):
            barrier.wait()
            try:
                self.service.transition(
                    self.coordinator,
                    obs_id,
                    "schedule",
                    {"operator_id": "op-1"},
                )
                outcomes[name] = "ok"
            except ConflictError as exc:
                outcomes[name] = "conflict:" + str(exc)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(schedule, "a", obs_a["id"]),
                pool.submit(schedule, "b", obs_b["id"]),
            ]
            for future in futures:
                future.result()

        self.assertIn("ok", outcomes.values())
        self.assertTrue(
            any(value.startswith("conflict") for value in outcomes.values()),
            outcomes,
        )
        statuses = sorted(
            entity["status"] for entity in self.service.list("observation")
        )
        self.assertEqual(statuses, ["requested", "scheduled"])
        # The failed request remains retryable on the same telescope at a free
        # window.
        loser_id = obs_b["id"] if outcomes["a"] == "ok" else obs_a["id"]
        loser = self.service.get(loser_id)
        self.assertEqual(loser["status"], "requested")
        self.assertFalse(loser["data"].get("holds_slot"))

    def test_same_team_window_is_admitted_once_even_on_different_telescopes(self):
        candidate_a = self._candidate("AT-C")
        candidate_b = self._candidate("AT-D")
        obs_a = self._observation(
            candidate_a["id"],
            "team-shared",
            "2026-09-28T12:00:00Z",
            "2026-09-28T13:00:00Z",
            telescope_id=self.telescope["id"],
        )
        obs_b = self._observation(
            candidate_b["id"],
            "team-shared",
            "2026-09-28T12:30:00Z",
            "2026-09-28T13:30:00Z",
            telescope_id=self.telescope2["id"],
        )

        barrier = threading.Barrier(2)
        outcomes = {}

        def schedule(name, obs_id):
            barrier.wait()
            try:
                self.service.transition(
                    self.coordinator, obs_id, "schedule", {"operator_id": "op-1"}
                )
                outcomes[name] = "ok"
            except ConflictError:
                outcomes[name] = "conflict"

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(schedule, "a", obs_a["id"]),
                pool.submit(schedule, "b", obs_b["id"]),
            ]
            for future in futures:
                future.result()

        self.assertEqual(sorted(outcomes.values()), ["conflict", "ok"])
        self.assertEqual(
            sorted(entity["status"] for entity in self.service.list("observation")),
            ["requested", "scheduled"],
        )


class PriorityInvalidationTest(ConcurrencyFixture):
    def _setup_with_scheduled_observation(self, magnitude=17.0, transient_type="unknown"):
        candidate = self._candidate("AT-PRIO", magnitude=magnitude, transient_type=transient_type)
        observation = self._observation(
            candidate["id"],
            "team-a",
            "2026-09-28T10:00:00Z",
            "2026-09-28T11:00:00Z",
        )
        scheduled = self.service.transition(
            self.coordinator, observation["id"], "schedule", {"operator_id": "op-1"}
        )
        return candidate, scheduled

    def test_priority_change_invalidates_open_observations_but_keeps_slot(self):
        candidate, scheduled = self._setup_with_scheduled_observation()

        # A competing request in the same window: while the scheduled slot is
        # under review it must remain occupied, so this cannot be admitted.
        other = self._candidate("AT-WAIT", magnitude=12.0, transient_type="grb")
        waiting = self._observation(
            other["id"],
            "team-wait",
            "2026-09-28T10:15:00Z",
            "2026-09-28T10:45:00Z",
        )

        fresh = self.service.get(candidate["id"])
        merged = self.service.transition(
            self.analyst,
            fresh["id"],
            "merge_measurement",
            {
                "measurement": {
                    "observed_at": "2026-09-27T01:00:00Z",
                    "ra": 10,
                    "dec": 20,
                    "magnitude": 15.0,
                }
            },
            expected_version=fresh["version"],
        )
        self.assertNotEqual(
            merged["data"]["priority_score"], fresh["data"]["priority_score"]
        )

        invalidated = self.service.get(scheduled["id"])
        self.assertEqual(invalidated["status"], "review_pending")
        self.assertTrue(invalidated["data"]["holds_slot"])

        # The waiting request cannot steal the window during review.
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator, waiting["id"], "schedule", {"operator_id": "op-2"}
            )

        audit = self.service.audit_log(scheduled["id"])
        invalidate_entries = [entry for entry in audit if entry["action"] == "invalidate"]
        self.assertEqual(len(invalidate_entries), 1)
        self.assertEqual(invalidate_entries[0]["to_status"], "review_pending")
        self.assertTrue(invalidate_entries[0]["detail"]["slot_retained"])

    def test_requested_observation_invalidated_without_holding_slot(self):
        candidate = self._candidate("AT-PRIO2")
        observation = self._observation(
            candidate["id"],
            "team-a",
            "2026-09-29T10:00:00Z",
            "2026-09-29T11:00:00Z",
        )
        fresh = self.service.get(candidate["id"])
        self.service.transition(
            self.analyst,
            fresh["id"],
            "reclassify",
            {"new_type": "grb", "reason": "spectrum confirms"},
            expected_version=fresh["version"],
        )
        pending = self.service.get(observation["id"])
        self.assertEqual(pending["status"], "review_pending")
        self.assertFalse(pending["data"]["holds_slot"])

    def test_confirm_review_releases_slot_and_higher_priority_wins(self):
        candidate, scheduled = self._setup_with_scheduled_observation(magnitude=18.0)

        # Two requests queued for the same window: brighter candidate (higher
        # priority) vs fainter one.
        bright = self._candidate("AT-BRIGHT", magnitude=13.0, transient_type="grb")
        faint = self._candidate("AT-FAINT", magnitude=20.0)
        bright_obs = self._observation(
            bright["id"],
            "team-bright",
            "2026-09-28T10:00:00Z",
            "2026-09-28T11:00:00Z",
        )
        faint_obs = self._observation(
            faint["id"],
            "team-faint",
            "2026-09-28T10:00:00Z",
            "2026-09-28T11:00:00Z",
        )

        # Priority of the scheduled candidate changes and sends it to review.
        fresh = self.service.get(candidate["id"])
        self.service.transition(
            self.analyst,
            fresh["id"],
            "merge_measurement",
            {
                "measurement": {
                    "observed_at": "2026-09-27T01:00:00Z",
                    "ra": 10,
                    "dec": 20,
                    "magnitude": 19.0,
                }
            },
            expected_version=fresh["version"],
        )
        self.assertEqual(self.service.get(scheduled["id"])["status"], "review_pending")

        # Reviewer confirms: slot released, queue re-competes at latest
        # priority in the same transaction.
        confirmed = self.service.transition(
            self.supervisor,
            scheduled["id"],
            "confirm_review",
            {},
        )
        self.assertEqual(confirmed["status"], "requested")
        self.assertFalse(confirmed["data"]["holds_slot"])

        self.assertEqual(self.service.get(bright_obs["id"])["status"], "scheduled")
        self.assertEqual(self.service.get(faint_obs["id"])["status"], "requested")
        self.assertEqual(self.service.get(scheduled["id"])["status"], "requested")

        # Audit shows the automatic admit and the deferred losers.
        self.assertTrue(
            any(
                entry["action"] == "schedule"
                and entry["detail"].get("trigger") == "priority_competition"
                for entry in self.service.audit_log(bright_obs["id"])
            )
        )
        self.assertTrue(
            any(
                entry["action"] == "schedule_deferred"
                for entry in self.service.audit_log(faint_obs["id"])
            )
        )

    def test_withdraw_releases_slot_and_recompetes(self):
        candidate, scheduled = self._setup_with_scheduled_observation(magnitude=18.0)
        other = self._candidate("AT-OTHER", magnitude=14.0)
        waiting = self._observation(
            other["id"],
            "team-other",
            "2026-09-28T10:00:00Z",
            "2026-09-28T11:00:00Z",
        )

        # Send the scheduled observation into review first.
        pending = self.service.get(scheduled["id"])
        self.service.transition(
            self.supervisor,
            pending["id"],
            "invalidate",
            {"reason": "check"},
        )
        withdrawn = self.service.transition(
            self.supervisor,
            pending["id"],
            "withdraw",
            {"reason": "weather"},
        )
        self.assertEqual(withdrawn["status"], "withdrawn")
        self.assertFalse(withdrawn["data"]["holds_slot"])
        self.assertEqual(self.service.get(waiting["id"])["status"], "scheduled")

    def test_completed_observation_is_not_invalidated(self):
        candidate = self._candidate("AT-DONE", magnitude=17.0)
        observation = self._observation(
            candidate["id"],
            "team-a",
            "2026-09-30T10:00:00Z",
            "2026-09-30T11:00:00Z",
        )
        self.service.transition(
            self.coordinator, observation["id"], "schedule", {"operator_id": "op-1"}
        )
        self.service.transition(
            Actor("operator-1", "operator"),
            observation["id"],
            "complete",
            {"quality": "good"},
        )
        fresh = self.service.get(candidate["id"])
        self.service.transition(
            self.analyst,
            fresh["id"],
            "merge_measurement",
            {
                "measurement": {
                    "observed_at": "2026-09-27T01:00:00Z",
                    "ra": 10,
                    "dec": 20,
                    "magnitude": 15.0,
                }
            },
            expected_version=fresh["version"],
        )
        self.assertEqual(self.service.get(observation["id"])["status"], "completed")


class TransactionAtomicityTest(ConcurrencyFixture):
    def test_failed_schedule_rolls_back_everything(self):
        candidate = self._candidate("AT-ATOM")
        observation = self._observation(
            candidate["id"],
            "team-a",
            "2026-10-01T10:00:00Z",
            "2026-10-01T11:00:00Z",
        )
        original = self.service.get(observation["id"])

        # Force the audit write (the last step of the transaction) to fail.
        from src.repository import UnitOfWork

        real_audit = UnitOfWork.audit
        state = {"count": 0}

        def flaky_audit(self, entry, now=None):
            state["count"] += 1
            if state["count"] == 1:
                raise RuntimeError("disk full")
            return real_audit(self, entry, now)

        UnitOfWork.audit = flaky_audit
        try:
            with self.assertRaises(RuntimeError):
                self.service.transition(
                    self.coordinator,
                    observation["id"],
                    "schedule",
                    {"operator_id": "op-1"},
                )
        finally:
            UnitOfWork.audit = real_audit

        after = self.service.get(observation["id"])
        self.assertEqual(after["status"], "requested")
        self.assertEqual(after["version"], original["version"])
        actions = [entry["action"] for entry in self.service.audit_log(observation["id"])]
        # Only the create audit exists; the failed schedule left no trace.
        self.assertEqual(actions, ["create"])


if __name__ == "__main__":
    unittest.main()
