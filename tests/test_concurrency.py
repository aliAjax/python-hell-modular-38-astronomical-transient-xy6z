import threading
import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "concurrency.db"),
            RuleEngine(),
        )
        self.analyst = Actor("analyst-1", "analyst")
        self.coordinator = Actor("coordinator-1", "coordinator")

    def tearDown(self):
        self.tmp.cleanup()

    def _source(self):
        return self.service.create(
            self.analyst, "source", {"name": "Survey", "survey_name": "S"}
        )

    def _candidate(self, event_id="AT-1", magnitude=18.0, transient_type="unknown"):
        source = self._source()
        return self.service.create(
            self.analyst,
            "candidate",
            {
                "source_id": source["id"],
                "event_id": event_id,
                "ra": 10,
                "dec": 20,
                "magnitude": magnitude,
                "transient_type": transient_type,
                "observed_at": "2026-09-27T00:00:00Z",
            },
        )

    def _telescope(self):
        return self.service.create(
            self.coordinator,
            "telescope",
            {"name": "T1", "aperture_m": 2.0, "site_name": "NAO"},
        )

    def _observation(
        self,
        candidate_id,
        telescope_id,
        team_id="team-1",
        start="2026-09-28T10:00:00Z",
        end="2026-09-28T11:00:00Z",
    ):
        return self.service.create(
            self.coordinator,
            "observation",
            {
                "candidate_id": candidate_id,
                "telescope_id": telescope_id,
                "team_id": team_id,
                "start_at": start,
                "end_at": end,
                "mode": "imaging",
            },
        )

    def test_concurrent_merges_one_wins_other_gets_version_conflict(self):
        candidate = self._candidate("concurrent-merge")
        version = candidate["version"]
        barrier = threading.Barrier(2)
        results = {}

        def merge(tag, observed_at, magnitude):
            barrier.wait()
            try:
                updated = self.service.transition(
                    self.analyst,
                    candidate["id"],
                    "merge_measurement",
                    {
                        "measurement": {
                            "observed_at": observed_at,
                            "ra": 11,
                            "dec": 21,
                            "magnitude": magnitude,
                        }
                    },
                    expected_version=version,
                )
                results[tag] = ("ok", updated)
            except ConflictError as exc:
                results[tag] = ("conflict", str(exc))

        t1 = threading.Thread(
            target=merge, args=("a", "2026-09-27T01:00:00Z", 17.0)
        )
        t2 = threading.Thread(
            target=merge, args=("b", "2026-09-27T02:00:00Z", 16.0)
        )
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        statuses = sorted(result[0] for result in results.values())
        self.assertEqual(statuses, ["conflict", "ok"])
        winner = results["a"][1] if results["a"][0] == "ok" else results["b"][1]
        measurements = winner["data"]["measurements"]
        # Original measurement retained plus the winner's; loser's never written.
        self.assertEqual(len(measurements), 2)
        self.assertEqual(winner["version"], version + 1)
        conflict_msg = (
            results["a"][1] if results["a"][0] == "conflict" else results["b"][1]
        )
        self.assertIn("version", conflict_msg)
        loser_tag = "b" if results["a"][0] == "ok" else "a"
        loser_time = (
            "2026-09-27T02:00:00Z" if loser_tag == "b" else "2026-09-27T01:00:00Z"
        )
        self.assertNotIn(loser_time, [m["observed_at"] for m in measurements])

    def test_priority_change_invalidates_requested_observations(self):
        candidate = self._candidate("invalidate")
        telescope = self._telescope()
        obs = self._observation(candidate["id"], telescope["id"])
        self.assertEqual(obs["status"], "requested")

        self.service.transition(
            self.analyst,
            candidate["id"],
            "merge_measurement",
            {
                "measurement": {
                    "observed_at": "2026-09-27T01:00:00Z",
                    "ra": 11,
                    "dec": 21,
                    "magnitude": 10.0,
                }
            },
        )
        obs = self.service.get(obs["id"])
        self.assertEqual(obs["status"], "pending_review")
        audit = self.service.audit_log(obs["id"])
        self.assertTrue(
            any(
                e["action"] == "priority_review" and e["to_status"] == "pending_review"
                for e in audit
            )
        )

    def test_pending_review_holds_slot_and_confirm_requeues(self):
        candidate = self._candidate("hold-slot")
        telescope = self._telescope()
        obs = self._observation(candidate["id"], telescope["id"])
        # Change priority -> obs pending_review, holding the slot.
        self.service.transition(
            self.analyst,
            candidate["id"],
            "merge_measurement",
            {
                "measurement": {
                    "observed_at": "2026-09-27T01:00:00Z",
                    "ra": 11,
                    "dec": 21,
                    "magnitude": 10.0,
                }
            },
        )
        # Another observation for the same slot cannot be scheduled.
        other_candidate = self._candidate("hold-slot-2")
        other = self._observation(other_candidate["id"], telescope["id"])
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator, other["id"], "schedule", {"operator_id": "op-1"}
            )
        # The failed application is retained in a retryable state.
        other = self.service.get(other["id"])
        self.assertEqual(other["status"], "requested")
        # Review confirm releases the hold and re-queues.
        obs = self.service.get(obs["id"])
        self.assertEqual(obs["status"], "pending_review")
        obs = self.service.transition(self.analyst, obs["id"], "confirm", {})
        self.assertEqual(obs["status"], "requested")
        # With the hold released, the confirmed observation can be scheduled.
        obs = self.service.transition(
            self.coordinator, obs["id"], "schedule", {"operator_id": "op-1"}
        )
        self.assertEqual(obs["status"], "scheduled")

    def test_withdraw_releases_slot(self):
        candidate = self._candidate("release")
        telescope = self._telescope()
        obs = self._observation(candidate["id"], telescope["id"])
        self.service.transition(
            self.analyst,
            candidate["id"],
            "merge_measurement",
            {
                "measurement": {
                    "observed_at": "2026-09-27T01:00:00Z",
                    "ra": 11,
                    "dec": 21,
                    "magnitude": 10.0,
                }
            },
        )
        obs = self.service.get(obs["id"])
        self.assertEqual(obs["status"], "pending_review")
        # Withdraw releases the slot.
        obs = self.service.transition(
            self.coordinator, obs["id"], "withdraw", {"reason": "no longer needed"}
        )
        self.assertEqual(obs["status"], "withdrawn")
        # Now another observation can take the slot.
        other_candidate = self._candidate("release-2")
        other = self._observation(other_candidate["id"], telescope["id"])
        other = self.service.transition(
            self.coordinator, other["id"], "schedule", {"operator_id": "op-1"}
        )
        self.assertEqual(other["status"], "scheduled")

    def test_waiting_requests_compete_by_priority(self):
        telescope = self._telescope()
        # High-priority candidate (bright GRB) vs low-priority candidate (faint unknown).
        high = self._candidate("high", magnitude=10.0, transient_type="grb")
        low = self._candidate("low", magnitude=20.0, transient_type="unknown")
        obs_high = self._observation(high["id"], telescope["id"], team_id="team-high")
        obs_low = self._observation(low["id"], telescope["id"], team_id="team-low")
        self.assertGreater(
            high["data"]["priority_score"], low["data"]["priority_score"]
        )

        # Low-priority cannot jump the queue while a higher-priority request waits.
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator, obs_low["id"], "schedule", {"operator_id": "op-1"}
            )
        obs_low = self.service.get(obs_low["id"])
        self.assertEqual(obs_low["status"], "requested")
        # High-priority goes first.
        obs_high = self.service.transition(
            self.coordinator, obs_high["id"], "schedule", {"operator_id": "op-1"}
        )
        self.assertEqual(obs_high["status"], "scheduled")
        # Slot now firmly taken; low-priority still cannot be scheduled.
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator, obs_low["id"], "schedule", {"operator_id": "op-1"}
            )
        # Withdraw the high-priority observation to release the slot.
        self.service.transition(
            self.coordinator, obs_high["id"], "withdraw", {"reason": "free slot"}
        )
        # Low-priority can now be scheduled.
        obs_low = self.service.transition(
            self.coordinator, obs_low["id"], "schedule", {"operator_id": "op-1"}
        )
        self.assertEqual(obs_low["status"], "scheduled")

    def test_team_conflict_blocks_second_observation(self):
        telescope = self._telescope()
        c1 = self._candidate("team-1")
        c2 = self._candidate("team-2")
        o1 = self._observation(c1["id"], telescope["id"], team_id="team-same")
        o2 = self._observation(c2["id"], telescope["id"], team_id="team-same")
        self.service.transition(
            self.coordinator, o1["id"], "schedule", {"operator_id": "op-1"}
        )
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator, o2["id"], "schedule", {"operator_id": "op-1"}
            )
        o2 = self.service.get(o2["id"])
        self.assertEqual(o2["status"], "requested")

    def test_reclassify_recalculates_priority_and_invalidates(self):
        candidate = self._candidate("reclassify", magnitude=18.0, transient_type="unknown")
        telescope = self._telescope()
        obs = self._observation(candidate["id"], telescope["id"])
        self.assertEqual(obs["status"], "requested")
        before = candidate["data"]["priority_score"]

        self.service.transition(
            self.analyst, candidate["id"], "triage", {"reason": "review"}
        )
        self.service.transition(
            self.analyst,
            candidate["id"],
            "reclassify",
            {"new_type": "grb", "reason": "afterglow detected"},
        )
        candidate = self.service.get(candidate["id"])
        after = candidate["data"]["priority_score"]
        self.assertGreater(after, before)
        obs = self.service.get(obs["id"])
        self.assertEqual(obs["status"], "pending_review")


if __name__ == "__main__":
    unittest.main()
