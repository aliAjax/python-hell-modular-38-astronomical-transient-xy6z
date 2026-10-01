from .domain import ConflictError, InvalidTransition, PermissionDenied, ValidationError


def _find_one(lookup, kind, field, value):
    if lookup is None:
        return None
    rows = lookup(kind, field, value) or []
    return rows[0] if rows else None


def calculate_priority(magnitude, transient_type):
    """Return a deterministic observing priority from 0 to 100."""
    try:
        mag = float(magnitude)
    except (TypeError, ValueError):
        raise ValidationError("magnitude must be numeric")
    if mag < -30 or mag > 40:
        raise ValidationError("magnitude is outside the supported range")
    type_weights = {
        "grb": 30,
        "supernova": 22,
        "tde": 20,
        "variable": 8,
        "unknown": 12,
    }
    score = max(0.0, 100.0 - mag * 4.0) + type_weights.get(transient_type, 5)
    return round(min(100.0, score), 2)


def measurements_overlap(first_start, first_end, second_start, second_end):
    return str(first_start) < str(second_end) and str(second_start) < str(first_end)


def _validate_source(actor, data, lookup):
    if len(str(data.get("name", "")).strip()) < 2:
        raise ValidationError("source name is required")
    return {}


def _validate_candidate(actor, data, lookup):
    source_id = data.get("source_id")
    if not _find_one(lookup, "source", "id", source_id):
        raise ValidationError("source does not exist")
    try:
        ra = float(data.get("ra"))
        dec = float(data.get("dec"))
    except (TypeError, ValueError):
        raise ValidationError("ra and dec must be numeric")
    if ra < 0 or ra >= 360 or dec < -90 or dec > 90:
        raise ValidationError("coordinates are outside valid ranges")
    event_key = "%s:%s" % (source_id, data.get("event_id"))
    duplicate = _find_one(lookup, "candidate", "event_key", event_key)
    if duplicate:
        raise ConflictError("candidate already exists for event key " + event_key)
    return {
        "event_key": event_key,
        "measurements": [
            {
                "observed_at": data.get("observed_at"),
                "ra": ra,
                "dec": dec,
                "magnitude": float(data["magnitude"]),
            }
        ],
        "priority_score": calculate_priority(data.get("magnitude"), data.get("transient_type")),
    }


def _validate_telescope(actor, data, lookup):
    if not (0 < float(data.get("aperture_m", 0))):
        raise ValidationError("aperture_m must be positive")
    return {}


def _validate_observation(actor, data, lookup):
    if not _find_one(lookup, "candidate", "id", data.get("candidate_id")):
        raise ValidationError("candidate does not exist")
    if not _find_one(lookup, "telescope", "id", data.get("telescope_id")):
        raise ValidationError("telescope does not exist")
    if str(data.get("start_at")) >= str(data.get("end_at")):
        raise ValidationError("observation end must be after start")
    return {"scheduled_team": data.get("team_id")}


def _validate_merge_measurement(actor, entity, data, lookup):
    measurement = data.get("measurement")
    if not isinstance(measurement, dict):
        raise ValidationError("measurement must be an object")
    try:
        ra = float(measurement.get("ra"))
        dec = float(measurement.get("dec"))
        magnitude = float(measurement.get("magnitude"))
    except (TypeError, ValueError):
        raise ValidationError("measurement coordinates and magnitude must be numeric")
    if ra < 0 or ra >= 360 or dec < -90 or dec > 90:
        raise ValidationError("measurement coordinates are outside valid ranges")
    observed_at = str(measurement.get("observed_at", "")).strip()
    if not observed_at:
        raise ValidationError("measurement observed_at is required")
    existing = list(entity["data"].get("measurements") or [])
    if any(item.get("observed_at") == observed_at for item in existing):
        raise ConflictError("measurement timestamp already merged")
    merged = existing + [{"observed_at": observed_at, "ra": ra, "dec": dec, "magnitude": magnitude}]
    latest = sorted(merged, key=lambda item: item["observed_at"])[-1]
    return {
        "measurements": merged,
        "merged_measurement_count": len(merged),
        "latest_magnitude": latest["magnitude"],
        "priority_score": calculate_priority(latest["magnitude"], entity["data"].get("transient_type")),
    }


def _validate_reclassify(actor, entity, data, lookup):
    new_type = data.get("new_type")
    if new_type not in {"grb", "supernova", "tde", "variable", "unknown"}:
        raise ValidationError("unsupported transient type")
    priority = entity["data"].get("priority_score")
    measurements = entity["data"].get("measurements") or []
    if measurements:
        latest = sorted(measurements, key=lambda item: item["observed_at"])[-1]
        priority = calculate_priority(latest["magnitude"], new_type)
    return {
        "transient_type": new_type,
        "previous_type": entity["data"].get("transient_type"),
        "priority_score": priority,
    }


def _validate_correct(actor, entity, data, lookup):
    if not str(data.get("reason", "")).strip():
        raise ValidationError("correction reason is required")
    return {"corrected_by": actor.user_id}


def _validate_confirm(actor, entity, data, lookup):
    # Review confirmation releases the slot hold and returns the observation to
    # the waiting pool so it re-competes by the latest priority.
    return {"confirmed_by": actor.user_id}


def _slot_overlaps(start, end, other):
    return measurements_overlap(
        start, end, other["data"].get("start_at"), other["data"].get("end_at")
    )


def _validate_schedule(actor, entity, data, lookup):
    obs = entity["data"]
    telescope_id = obs.get("telescope_id")
    team_id = obs.get("team_id")
    start = obs.get("start_at")
    end = obs.get("end_at")
    candidate = _find_one(lookup, "candidate", "id", obs.get("candidate_id"))
    my_priority = candidate["data"].get("priority_score") if candidate else None

    telescope_observations = lookup("observation", "telescope_id", telescope_id) if lookup else []
    team_observations = lookup("observation", "team_id", team_id) if lookup else []

    # A slot is firmly occupied by scheduled observations and held by
    # pending_review observations while their review is in progress.
    for other in telescope_observations:
        if other["id"] == entity["id"]:
            continue
        if other["status"] in ("scheduled", "pending_review") and _slot_overlaps(start, end, other):
            raise ConflictError("telescope is already reserved in this window")
    for other in team_observations:
        if other["id"] == entity["id"]:
            continue
        if other["status"] in ("scheduled", "pending_review") and _slot_overlaps(start, end, other):
            raise ConflictError("observation team is already committed in this window")

    # Waiting applications re-compete by the latest candidate priority: a
    # higher-priority requested observation for the same resource slot goes first.
    competitors = {}
    for other in list(telescope_observations) + list(team_observations):
        if other["id"] == entity["id"]:
            continue
        competitors[other["id"]] = other
    for other in competitors.values():
        if other["status"] != "requested" or not _slot_overlaps(start, end, other):
            continue
        other_candidate = _find_one(lookup, "candidate", "id", other["data"].get("candidate_id"))
        other_priority = other_candidate["data"].get("priority_score") if other_candidate else None
        if (
            other_priority is not None
            and my_priority is not None
            and float(other_priority) > float(my_priority)
        ):
            raise ConflictError("a higher-priority observation is already waiting for this window")

    return {"scheduled_by": actor.user_id}


class RuleEngine:
    ALIASES = {
        "sources": "source",
        "candidates": "candidate",
        "telescopes": "telescope",
        "observations": "observation",
    }
    INITIAL_STATUS = {
        "source": "registered",
        "candidate": "detected",
        "telescope": "available",
        "observation": "requested",
    }
    TRANSITIONS = {
        "source": {
            "activate": (("registered",), "active"),
            "retire": (("active",), "retired"),
        },
        "candidate": {
            "merge_measurement": (("detected", "triaged"), "triaged"),
            "triage": (("detected",), "triaged"),
            "reclassify": (("triaged",), "triaged"),
            "correct": (("detected", "triaged", "classified"), "triaged"),
            "withdraw": (("detected", "triaged"), "withdrawn"),
            "classify": (("triaged",), "classified"),
        },
        "telescope": {
            "restrict": (("available",), "restricted"),
            "restore": (("restricted",), "available"),
        },
        "observation": {
            "schedule": (("requested",), "scheduled"),
            "confirm": (("pending_review",), "requested"),
            "complete": (("scheduled",), "completed"),
            "withdraw": (("requested", "pending_review", "scheduled"), "withdrawn"),
            "correct": (("requested", "scheduled"), "requested"),
        },
    }
    CREATE_REQUIRED = {
        "source": ("name", "survey_name"),
        "candidate": ("source_id", "event_id", "ra", "dec", "magnitude", "transient_type", "observed_at"),
        "telescope": ("name", "aperture_m", "site_name"),
        "observation": ("candidate_id", "telescope_id", "team_id", "start_at", "end_at", "mode"),
    }
    ACTION_REQUIRED = {
        ("source", "retire"): ("reason",),
        ("candidate", "merge_measurement"): ("measurement",),
        ("candidate", "triage"): ("reason",),
        ("candidate", "reclassify"): ("new_type", "reason"),
        ("candidate", "correct"): ("reason",),
        ("candidate", "withdraw"): ("reason",),
        ("candidate", "classify"): ("classification",),
        ("telescope", "restrict"): ("reason",),
        ("observation", "schedule"): ("operator_id",),
        ("observation", "withdraw"): ("reason",),
        ("observation", "correct"): ("reason",),
    }
    CREATE_ROLES = {
        "source": ("analyst", "admin"),
        "candidate": ("analyst", "operator", "admin"),
        "telescope": ("coordinator", "admin"),
        "observation": ("analyst", "coordinator", "admin"),
    }
    ROLE_ACTIONS = {
        "activate": ("coordinator", "admin"),
        "retire": ("coordinator", "admin"),
        "merge_measurement": ("analyst", "operator", "admin"),
        "triage": ("analyst", "operator", "admin"),
        "reclassify": ("analyst", "supervisor", "admin"),
        "correct": ("analyst", "supervisor", "admin"),
        "withdraw": ("analyst", "supervisor", "admin"),
        "classify": ("analyst", "supervisor", "admin"),
        "restrict": ("coordinator", "admin"),
        "restore": ("coordinator", "admin"),
        "schedule": ("coordinator", "admin"),
        "confirm": ("analyst", "coordinator", "admin"),
        "complete": ("operator", "coordinator", "admin"),
        ("observation", "withdraw"): ("coordinator", "admin"),
    }
    CUSTOM_CREATE = {
        "source": _validate_source,
        "candidate": _validate_candidate,
        "telescope": _validate_telescope,
        "observation": _validate_observation,
    }
    CUSTOM_TRANSITIONS = {
        ("candidate", "merge_measurement"): _validate_merge_measurement,
        ("candidate", "reclassify"): _validate_reclassify,
        ("candidate", "correct"): _validate_correct,
        ("observation", "schedule"): _validate_schedule,
        ("observation", "confirm"): _validate_confirm,
        ("observation", "correct"): _validate_correct,
    }

    def normalize_kind(self, kind):
        return self.ALIASES.get(kind, kind)

    def initial_status(self, kind):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        return self.INITIAL_STATUS[kind]

    @staticmethod
    def _ensure_role(actor, allowed):
        if actor.role not in allowed:
            raise PermissionDenied("role %s is not allowed here" % actor.role)

    @staticmethod
    def _require(data, fields):
        for field in fields:
            value = data.get(field)
            if value is None or value == "" or value == [] or value == {}:
                raise ValidationError("missing required field: " + field)

    def validate_create(self, actor, kind, data, lookup=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        self._ensure_role(actor, self.CREATE_ROLES.get(kind, ("admin",)))
        self._require(data, self.CREATE_REQUIRED.get(kind, ()))
        custom = self.CUSTOM_CREATE.get(kind)
        return custom(actor, data, lookup) if custom else {}

    def validate_transition(self, actor, entity, action, data, lookup=None):
        kind = self.normalize_kind(entity["kind"])
        transition = self.TRANSITIONS.get(kind, {}).get(action)
        if not transition:
            raise InvalidTransition("unknown action %s for %s" % (action, kind))
        allowed_statuses, next_status = transition
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition("cannot %s from status %s" % (action, entity["status"]))
        allowed_roles = self.ROLE_ACTIONS.get((kind, action), self.ROLE_ACTIONS.get(action, ("admin",)))
        self._ensure_role(actor, allowed_roles)
        self._require(data, self.ACTION_REQUIRED.get((kind, action), ()))
        custom = self.CUSTOM_TRANSITIONS.get((kind, action))
        extra = custom(actor, entity, data, lookup) if custom else {}
        patch = dict(data)
        if extra:
            patch.update(extra)
        return next_status, patch
