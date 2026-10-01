import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class UnitOfWork:
    """A write transaction holding a BEGIN IMMEDIATE lock.

    All reads performed through this unit of work observe the locked database
    state, so uniqueness and scheduling checks inside it are atomic with the
    write: a competing transaction blocks at BEGIN IMMEDIATE and then sees the
    committed rows instead of double booking a telescope or team window.
    """

    def __init__(self, path):
        self.path = path
        self.connection = None

    def __enter__(self):
        self.connection = sqlite3.connect(self.path, timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA busy_timeout = 30000")
        self.connection.execute("BEGIN IMMEDIATE")
        return self

    def __exit__(self, exc_type, exc, tb):
        connection = self.connection
        self.connection = None
        try:
            if exc_type is None:
                connection.commit()
            else:
                connection.rollback()
        finally:
            connection.close()
        return False

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def get(self, entity_id):
        row = self.connection.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        return self._entity_from_row(row) if row else None

    def list(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self.connection.execute(
            "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
        ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find(self, kind, field, value):
        rows = self.list(kind=kind)
        return [
            entity
            for entity in rows
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def insert(self, entity_id, kind, status, data, actor_id, now=None):
        now = now or utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        self.connection.execute(
            "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
            "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
            (entity_id, kind, status, payload, actor_id, now, now),
        )

    def update(self, entity_id, expected_version, status, data, now=None):
        """Conditional update; raises NotFound/Conflict exactly as the caller sees it.

        The conflict raised here aborts the surrounding unit of work, so every
        side effect of a stale submission (cascade invalidations, audits, slot
        admits) rolls back together and the retryable request is untouched.
        """
        now = now or utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        row = self.connection.execute(
            "SELECT version FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        if not row:
            raise NotFoundError("entity not found: " + entity_id)
        current_version = int(row["version"])
        if expected_version is not None and current_version != int(expected_version):
            raise ConflictError(
                "version conflict: expected %s, found %s"
                % (expected_version, current_version)
            )
        cursor = self.connection.execute(
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
            "WHERE id = ? AND version = ?",
            (status, payload, now, entity_id, current_version),
        )
        if cursor.rowcount != 1:
            raise ConflictError("entity was modified concurrently: " + entity_id)
        return current_version + 1

    def audit(self, entry, now=None):
        self.connection.execute(
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, "
            "to_status, detail, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entry["entity_id"],
                entry["actor_id"],
                entry["actor_role"],
                entry["action"],
                entry.get("from_status"),
                entry["to_status"],
                json.dumps(entry.get("detail") or {}, ensure_ascii=False, sort_keys=True),
                now or utcnow(),
            ),
        )

    def get_idempotency(self, actor_id, idem_key):
        row = self.connection.execute(
            "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
            (actor_id, idem_key),
        ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id, now=None):
        self.connection.execute(
            "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
            "VALUES (?, ?, ?, ?)",
            (actor_id, idem_key, entity_id, now or utcnow()),
        )


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
            """)

    def unit_of_work(self):
        return UnitOfWork(self.path)

    @staticmethod
    def _entity_from_row(row):
        return UnitOfWork._entity_from_row(row)

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        with self.unit_of_work() as uow:
            uow.update(entity_id, expected_version, status, data)
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self.unit_of_work() as uow:
            uow.audit(
                {
                    "entity_id": entity_id,
                    "actor_id": actor_id,
                    "actor_role": actor_role,
                    "action": action,
                    "from_status": from_status,
                    "to_status": to_status,
                    "detail": detail,
                }
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
