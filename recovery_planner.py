"""Metadata recovery planner for a distributed file system metadata service.

After a metadata service restart, some inode locks, directory-entry leases
and rename operations are only half-finished.  This module computes, from
abstract inputs (client sessions, locks/leases, the operation log, rename
dependencies, persisted parent-directory state and lease timeouts), which
operations can be replayed, which must be undone and which must wait.

The tool operates purely on abstract logs/state snapshots; it never touches
a real file system.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple

PHASE_PREPARE = "prepare"
PHASE_COMMIT = "commit"
PHASE_ABORT = "abort"

ACTION_REPLAY = "replay"
ACTION_UNDO = "undo"

REASON_LEASE_VALID = "client_lease_still_valid"
REASON_PARENT_MISMATCH = "parent_state_mismatch"
REASON_DEPENDENCY_CYCLE = "dependency_cycle"
REASON_WAITING_LEASE_EXPIRY = "waiting_for_lease_expiry"


@dataclass(frozen=True)
class ClientSession:
    client_id: str
    session_id: str
    epoch: int
    connected: bool


@dataclass(frozen=True)
class Lock:
    """An inode lock or directory-entry lease held by a client."""

    lock_id: str
    resource: str
    client_id: str
    session_id: str
    kind: str  # "inode_lock" or "dentry_lease"
    expires_at: float


@dataclass(frozen=True)
class LogRecord:
    seq: int
    op_id: str
    phase: str  # prepare | commit | abort
    op: str  # lock | unlock | rename | create | unlink
    client_id: str
    session_id: str
    resources: Tuple[str, ...]
    ts: float = 0.0


@dataclass(frozen=True)
class RenameDependency:
    """A rename is only completable when both parent directories are in the
    persisted state the rename was prepared against."""

    op_id: str
    src_parent: str
    dst_parent: str
    src_parent_version: int
    dst_parent_version: int


@dataclass
class RecoveryInput:
    sessions: List[ClientSession] = field(default_factory=list)
    locks: List[Lock] = field(default_factory=list)
    log: List[LogRecord] = field(default_factory=list)
    rename_deps: List[RenameDependency] = field(default_factory=list)
    parent_states: Dict[str, int] = field(default_factory=dict)
    now: float = 0.0


@dataclass(frozen=True)
class RecoveryAction:
    op_id: str
    action: str  # replay | undo
    op: str
    resources: Tuple[str, ...]


@dataclass(frozen=True)
class OrphanedLock:
    lock_id: str
    resource: str
    kind: str
    former_owner: str
    reclaimable: bool


@dataclass(frozen=True)
class BlockedItem:
    item: str
    reason: str
    detail: str


@dataclass
class RecoveryPlan:
    recovery_order: List[RecoveryAction] = field(default_factory=list)
    orphaned_locks: List[OrphanedLock] = field(default_factory=list)
    blocked: List[BlockedItem] = field(default_factory=list)


class RecoveryPlanner:
    def __init__(self, data: RecoveryInput):
        self.data = data
        self._sessions: Dict[Tuple[str, str], ClientSession] = {
            (s.client_id, s.session_id): s for s in data.sessions
        }
        self._rename_deps: Dict[str, RenameDependency] = {
            d.op_id: d for d in data.rename_deps
        }

    def _lease_expired(self, client_id: str, resource: str) -> bool:
        """True when every lease the client holds on the resource expired."""
        leases = [
            l
            for l in self.data.locks
            if l.client_id == client_id and l.resource == resource
        ]
        if not leases:
            return True  # no lease at all -> nothing protects the client
        return all(l.expires_at <= self.data.now for l in leases)

    def _client_live(self, client_id: str, resources: Tuple[str, ...]) -> bool:
        """A client counts as live for an operation only while it still holds
        a valid lease on every resource the operation touches.  Once the
        lease is lost the client may not commit any further."""
        return all(
            not self._lease_expired(client_id, resource) for resource in resources
        )

    def _client_live_at(
        self, client_id: str, resources: Tuple[str, ...], ts: float
    ) -> bool:
        for resource in resources:
            leases = [
                l
                for l in self.data.locks
                if l.client_id == client_id and l.resource == resource
            ]
            if not leases:
                return False
            if not any(l.expires_at >= ts for l in leases):
                return False
        return True

    def _dedupe_log(self) -> List[LogRecord]:
        """Collapse duplicate log records.

        Duplicates (same op_id + phase, e.g. retried journal writes) keep the
        earliest sequence number; surviving records come back ordered by seq.
        """
        best: Dict[Tuple[str, str], LogRecord] = {}
        for rec in self.data.log:
            key = (rec.op_id, rec.phase)
            current = best.get(key)
            if current is None or rec.seq < current.seq:
                best[key] = rec
        return sorted(best.values(), key=lambda r: r.seq)

    def _commit_valid(self, rec: LogRecord) -> bool:
        """A commit from a client whose lease had already expired at commit
        time is rejected: a stale client must not be able to commit."""
        return self._client_live_at(rec.client_id, rec.resources, rec.ts)

    def _rename_parents_consistent(self, op_id: str) -> Tuple[bool, str]:
        dep = self._rename_deps.get(op_id)
        if dep is None:
            return True, ""
        problems = []
        actual_src = self.data.parent_states.get(dep.src_parent)
        actual_dst = self.data.parent_states.get(dep.dst_parent)
        if actual_src != dep.src_parent_version:
            problems.append(
                "src parent %s version %s != expected %s"
                % (dep.src_parent, actual_src, dep.src_parent_version)
            )
        if actual_dst != dep.dst_parent_version:
            problems.append(
                "dst parent %s version %s != expected %s"
                % (dep.dst_parent, actual_dst, dep.dst_parent_version)
            )
        if problems:
            return False, "; ".join(problems)
        return True, ""

    def plan(self) -> RecoveryPlan:
        records = self._dedupe_log()

        ops: Dict[str, List[LogRecord]] = {}
        first_seq: Dict[str, int] = {}
        for rec in records:
            ops.setdefault(rec.op_id, []).append(rec)
            first_seq.setdefault(rec.op_id, rec.seq)

        actions: Dict[str, RecoveryAction] = {}
        blocked: List[BlockedItem] = []

        for op_id in sorted(ops, key=lambda o: first_seq[o]):
            recs = ops[op_id]
            prepare = next((r for r in recs if r.phase == PHASE_PREPARE), None)
            commit = next((r for r in recs if r.phase == PHASE_COMMIT), None)
            abort = next((r for r in recs if r.phase == PHASE_ABORT), None)

            if abort is not None and commit is None:
                continue  # already rolled back before the crash

            if commit is not None and self._commit_valid(commit):
                if commit.op == "rename":
                    ok, detail = self._rename_parents_consistent(op_id)
                    if not ok:
                        blocked.append(
                            BlockedItem(op_id, REASON_PARENT_MISMATCH, detail)
                        )
                        continue
                actions[op_id] = RecoveryAction(
                    op_id, ACTION_REPLAY, commit.op, commit.resources
                )
                continue

            # No usable commit: the operation was interrupted mid-flight.
            if prepare is None:
                continue
            if self._client_live(prepare.client_id, prepare.resources):
                blocked.append(
                    BlockedItem(
                        op_id,
                        REASON_LEASE_VALID,
                        "client %s still holds a valid lease; must wait"
                        % prepare.client_id,
                    )
                )
            else:
                actions[op_id] = RecoveryAction(
                    op_id, ACTION_UNDO, prepare.op, prepare.resources
                )

        ordered, cyclic = self._order_by_dependencies(actions, first_seq)
        for op_id in sorted(cyclic, key=lambda o: first_seq[o]):
            action = actions[op_id]
            blocked.append(
                BlockedItem(
                    op_id,
                    REASON_DEPENDENCY_CYCLE,
                    "resource dependency cycle involving %s"
                    % ", ".join(sorted(action.resources)),
                )
            )

        orphaned, waiting = self._orphaned_locks(records)
        for lock in waiting:
            blocked.append(
                BlockedItem(
                    lock.lock_id,
                    REASON_WAITING_LEASE_EXPIRY,
                    "lock on %s held by %s; lease valid until %.3f"
                    % (lock.resource, lock.client_id, lock.expires_at),
                )
            )

        return RecoveryPlan(
            recovery_order=ordered, orphaned_locks=orphaned, blocked=blocked
        )

    def _order_by_dependencies(
        self, actions: Dict[str, RecoveryAction], first_seq: Dict[str, int]
    ) -> Tuple[List[RecoveryAction], Set[str]]:
        """Topologically order recovery actions by resource dependency.

        Two actions that share a resource must run in log order; a rename
        additionally depends on every other action touching either parent
        directory, so cross-directory rename swaps surface as cycles.
        Returns (ordered actions, op_ids stuck in cycles).
        """
        op_ids = sorted(actions, key=lambda o: first_seq[o])
        deps: Dict[str, Set[str]] = {o: set() for o in op_ids}

        by_resource: Dict[str, List[str]] = {}
        for op_id in op_ids:
            for resource in actions[op_id].resources:
                by_resource.setdefault(resource, []).append(op_id)
        for holders in by_resource.values():
            holders.sort(key=lambda o: first_seq[o])
            for earlier, later in zip(holders, holders[1:]):
                deps[later].add(earlier)

        for op_id in op_ids:
            dep = self._rename_deps.get(op_id)
            if dep is None:
                continue
            for other in op_ids:
                if other == op_id:
                    continue
                if dep.src_parent in actions[other].resources or (
                    dep.dst_parent in actions[other].resources
                ):
                    deps[op_id].add(other)

        indegree = {o: len(d) for o, d in deps.items()}
        dependents: Dict[str, List[str]] = {o: [] for o in op_ids}
        for op_id, parents in deps.items():
            for parent in parents:
                dependents[parent].append(op_id)

        heap = [(first_seq[o], o) for o in op_ids if indegree[o] == 0]
        heapq.heapify(heap)
        ordered: List[RecoveryAction] = []
        done: Set[str] = set()
        while heap:
            _, op_id = heapq.heappop(heap)
            ordered.append(actions[op_id])
            done.add(op_id)
            for child in dependents[op_id]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    heapq.heappush(heap, (first_seq[child], child))

        cyclic = set(op_ids) - done
        return ordered, cyclic

    def _orphaned_locks(
        self, records: List[LogRecord]
    ) -> Tuple[List[OrphanedLock], List[Lock]]:
        committed_unlocks: Set[str] = set()
        for rec in records:
            if rec.phase == PHASE_COMMIT and rec.op == "unlock":
                committed_unlocks.update(rec.resources)

        orphaned: List[OrphanedLock] = []
        waiting: List[Lock] = []
        for lock in sorted(self.data.locks, key=lambda l: l.lock_id):
            if lock.resource in committed_unlocks:
                continue  # already released durably
            if lock.expires_at <= self.data.now:
                orphaned.append(
                    OrphanedLock(
                        lock_id=lock.lock_id,
                        resource=lock.resource,
                        kind=lock.kind,
                        former_owner=lock.client_id,
                        reclaimable=True,
                    )
                )
            else:
                session = self._sessions.get((lock.client_id, lock.session_id))
                if session is None or not session.connected:
                    # Owner is gone but the lease has not timed out yet:
                    # reclaiming now could let a stale client keep writing.
                    waiting.append(lock)
        return orphaned, waiting


def plan_recovery(data: RecoveryInput) -> RecoveryPlan:
    return RecoveryPlanner(data).plan()


# ---------------------------------------------------------------------------
# JSON command-line interface
# ---------------------------------------------------------------------------


def _input_from_dict(raw) -> RecoveryInput:
    return RecoveryInput(
        sessions=[ClientSession(**s) for s in raw.get("sessions", [])],
        locks=[Lock(**l) for l in raw.get("locks", [])],
        log=[
            LogRecord(resources=tuple(r.pop("resources", ())), **r)
            for r in (dict(e) for e in raw.get("log", []))
        ],
        rename_deps=[RenameDependency(**d) for d in raw.get("rename_deps", [])],
        parent_states=dict(raw.get("parent_states", {})),
        now=float(raw.get("now", 0.0)),
    )


def _plan_to_dict(plan: RecoveryPlan):
    return {
        "recovery_order": [
            {
                "op_id": a.op_id,
                "action": a.action,
                "op": a.op,
                "resources": list(a.resources),
            }
            for a in plan.recovery_order
        ],
        "orphaned_locks": [
            {
                "lock_id": o.lock_id,
                "resource": o.resource,
                "kind": o.kind,
                "former_owner": o.former_owner,
                "reclaimable": o.reclaimable,
            }
            for o in plan.orphaned_locks
        ],
        "blocked": [
            {"item": b.item, "reason": b.reason, "detail": b.detail}
            for b in plan.blocked
        ],
    }


def main(argv=None) -> int:
    import argparse
    import json
    import sys

    parser = argparse.ArgumentParser(
        description=(
            "Plan metadata recovery after a restart.  Reads an abstract JSON "
            "snapshot (sessions, locks, log, rename_deps, parent_states, "
            "now) and prints the recovery order, orphaned locks and blocked "
            "items.  No real file system is touched."
        )
    )
    parser.add_argument(
        "snapshot",
        nargs="?",
        help="JSON snapshot file (defaults to stdin)",
    )
    args = parser.parse_args(argv)

    if args.snapshot:
        with open(args.snapshot, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    else:
        raw = json.load(sys.stdin)

    plan = plan_recovery(_input_from_dict(raw))
    json.dump(_plan_to_dict(plan), sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
