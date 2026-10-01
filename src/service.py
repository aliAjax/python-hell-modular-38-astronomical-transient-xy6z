from uuid import uuid4

from .audit import AuditTrail
from .domain import Actor, ConflictError, NotFoundError, PermissionDenied
from .rules import RuleEngine, SLOT_HOLDING_STATUSES, _slot_conflicts


# Actor used for automatic, system-driven side effects (priority cascades,
# slot re-allocation after a release).
DISPATCHER = Actor(user_id="system-scheduler", role="coordinator")

# Observations that must be invalidated the moment their candidate's priority
# changes. Completed/withdrawn observations are historical and never move.
PRIORITY_SENSITIVE_STATUSES = ("requested", "scheduled")


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    # -- helpers -----------------------------------------------------------

    def _normalize(self, kind):
        return self.rules.normalize_kind(kind)

    def _locked_lookup(self, uow):
        def lookup(kind, field, value):
            return uow.find(self._normalize(kind), field, value)

        return lookup

    @staticmethod
    def _audit_entry(uow, entity_id, actor, action, from_status, to_status, detail=None):
        uow.audit(
            {
                "entity_id": entity_id,
                "actor_id": actor.user_id,
                "actor_role": actor.role,
                "action": action,
                "from_status": from_status,
                "to_status": to_status,
                "detail": detail or {},
            }
        )

    @staticmethod
    def _candidate_priority(uow, candidate_id):
        candidate = uow.get(candidate_id)
        if not candidate:
            return None
        return candidate["data"].get("priority_score")

    def _invalidate_for_priority(self, uow, candidate, old_priority, new_priority, actor):
        """Send every open observation of a re-prioritized candidate to review.

        Already-scheduled observations become review_pending but keep holding
        the telescope and team window (holds_slot stays true) until a reviewer
        confirms or withdraws them. Requested observations simply re-queue.
        """
        invalidated = []
        for observation in uow.list(kind="observation"):
            if observation["status"] not in PRIORITY_SENSITIVE_STATUSES:
                continue
            if observation["data"].get("candidate_id") != candidate["id"]:
                continue
            data = dict(observation["data"])
            data["holds_slot"] = observation["status"] == "scheduled"
            data["pending_reason"] = "candidate priority changed"
            uow.update(observation["id"], observation["version"], "review_pending", data)
            self._audit_entry(
                uow,
                observation["id"],
                actor,
                "invalidate",
                observation["status"],
                "review_pending",
                {
                    "candidate_id": candidate["id"],
                    "old_priority": old_priority,
                    "new_priority": new_priority,
                    "slot_retained": data["holds_slot"],
                },
            )
            invalidated.append(observation["id"])
        return invalidated

    def _admit_waiting(self, uow, telescope_id=None, actor=None):
        """Greedily admit requested observations by latest candidate priority.

        Runs inside the same write lock that released a slot, so the freed
        window cannot be grabbed twice. Lower-priority requests that still
        clash stay in ``requested`` and remain retryable; each deferral is
        audited. Highest priority wins a telescope/team window first.
        """
        actor = actor or DISPATCHER
        waiting = [
            entity
            for entity in uow.list(kind="observation", status="requested")
            if telescope_id is None or entity["data"].get("telescope_id") == telescope_id
        ]

        def sort_key(entity):
            priority = self._candidate_priority(uow, entity["data"].get("candidate_id"))
            return (
                # Negate so higher priority sorts first; unknown priority last.
                -(priority if priority is not None else float("-inf")),
                entity["created_at"],
                entity["id"],
            )

        admitted = []
        deferred = []
        lookup = self._locked_lookup(uow)
        for observation in sorted(waiting, key=sort_key):
            candidate_priority = self._candidate_priority(
                uow, observation["data"].get("candidate_id")
            )
            conflict = _slot_conflicts(lookup, observation)
            if conflict is not None:
                deferred.append(observation["id"])
                # The request is not modified, so it stays retryable, but the
                # fact that it lost this competition round is recorded.
                self._audit_entry(
                    uow,
                    observation["id"],
                    actor,
                    "schedule_deferred",
                    "requested",
                    "requested",
                    {
                        "reason": str(conflict),
                        "priority_score": candidate_priority,
                        "telescope_id": observation["data"].get("telescope_id"),
                        "team_id": observation["data"].get("team_id"),
                    },
                )
                continue
            data = dict(observation["data"])
            data["holds_slot"] = True
            data["scheduled_by"] = actor.user_id
            uow.update(observation["id"], observation["version"], "scheduled", data)
            self._audit_entry(
                uow,
                observation["id"],
                actor,
                "schedule",
                "requested",
                "scheduled",
                {
                    "trigger": "priority_competition",
                    "priority_score": candidate_priority,
                },
            )
            admitted.append(observation["id"])
        return admitted, deferred

    # -- queries -----------------------------------------------------------

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self._normalize(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    # -- commands ----------------------------------------------------------

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self._normalize(kind)
        payload = dict(data or {})
        # Cheap pre-checks outside the lock for fast feedback.
        self.rules.validate_create(actor, kind, payload, None, run_custom=False)

        # Idempotent replay without a lock when no contention is expected.
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity

        with self.repository.unit_of_work() as uow:
            # Re-check idempotency inside the lock: two concurrent retries with
            # the same key must resolve to a single created entity.
            if idempotency_key:
                existing = uow.get_idempotency(actor.user_id, idempotency_key)
                if existing:
                    entity = uow.get(existing)
                    if entity:
                        return entity
            # Duplicate keys, references etc. are checked against locked state.
            validated = self.rules.validate_create(
                actor, kind, payload, self._locked_lookup(uow)
            )
            if validated:
                payload.update(validated)
            entity_id = str(payload.pop("id", "") or uuid4())
            if uow.get(entity_id):
                raise ConflictError("entity already exists: " + entity_id)
            status = self.rules.initial_status(kind)
            uow.insert(entity_id, kind, status, payload, actor.user_id)
            created = uow.get(entity_id)
            self._audit_entry(
                uow, entity_id, actor, "create", None, status, {"kind": kind}
            )
            if idempotency_key:
                uow.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return created

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        body = dict(data or {})
        # Load for pre-validation only; the authoritative load happens under the
        # write lock so stale snapshots cannot overwrite a concurrent writer.
        snapshot = self.repository.get_entity(entity_id)
        if not snapshot:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else snapshot["version"]
        self.rules.validate_transition(
            actor, snapshot, action, body, None, run_custom=False
        )

        with self.repository.unit_of_work() as uow:
            entity = uow.get(entity_id)
            if not entity:
                raise NotFoundError("entity not found: " + entity_id)
            # Optimistic concurrency: a concurrent merge/reclassification moves
            # the version, so the stale submitter is told the version changed
            # and nothing of theirs overwrites the other writer's values.
            next_status, patch = self.rules.validate_transition(
                actor,
                entity,
                action,
                body,
                self._locked_lookup(uow),
            )
            merged = dict(entity["data"])
            merged.update(patch)

            kind = self._normalize(entity["kind"])
            from_status = entity["status"]
            old_priority = entity["data"].get("priority_score")

            uow.update(entity_id, expected, next_status, merged)
            updated = uow.get(entity_id)
            self._audit_entry(
                uow,
                entity_id,
                actor,
                action,
                from_status,
                updated["status"],
                {"patch": patch, "version": expected + 1},
            )

            result = {"updated": updated, "invalidated": [], "admitted": [], "deferred": []}

            # A successful candidate merge or reclassification may have moved
            # the priority; all open observations are immediately invalidated.
            if kind == "candidate":
                new_priority = merged.get("priority_score")
                if new_priority is not None and new_priority != old_priority:
                    result["invalidated"] = self._invalidate_for_priority(
                        uow, updated, old_priority, new_priority, actor
                    )

            # Confirming a review releases the held slot and withdraw frees a
            # scheduled slot; the waiting queue re-competes at latest priority.
            if kind == "observation" and (
                (action == "confirm_review")
                or (action == "withdraw" and from_status in SLOT_HOLDING_STATUSES)
            ):
                admitted, deferred = self._admit_waiting(uow, actor=actor)
                result["admitted"] = admitted
                result["deferred"] = deferred

        return result["updated"]

    def dispatch(self, actor, telescope_id=None):
        """Explicitly re-run the priority competition for waiting requests."""
        if actor.role not in ("coordinator", "admin"):
            raise PermissionDenied("role %s is not allowed here" % actor.role)
        if telescope_id is not None and not self.repository.get_entity(telescope_id):
            raise NotFoundError("telescope not found: " + telescope_id)
        with self.repository.unit_of_work() as uow:
            admitted, deferred = self._admit_waiting(
                uow, telescope_id=telescope_id, actor=actor
            )
        return {"admitted": admitted, "deferred": deferred}
