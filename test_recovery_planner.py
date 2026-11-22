import unittest

from recovery_planner import (
    ACTION_REPLAY,
    ACTION_UNDO,
    REASON_DEPENDENCY_CYCLE,
    REASON_LEASE_VALID,
    REASON_PARENT_MISMATCH,
    REASON_WAITING_LEASE_EXPIRY,
    ClientSession,
    Lock,
    LogRecord,
    RecoveryInput,
    RenameDependency,
    plan_recovery,
)

NOW = 1000.0


def session(client="c1", sess="s1", connected=False):
    return ClientSession(client_id=client, session_id=sess, epoch=1,
                         connected=connected)


def live_session(client="c1", sess="s1"):
    return session(client, sess, connected=True)


def lock(lock_id, resource, client="c1", sess="s1", expires=500.0,
         kind="inode_lock"):
    return Lock(lock_id=lock_id, resource=resource, client_id=client,
                session_id=sess, kind=kind, expires_at=expires)


def rec(seq, op_id, phase, op, client="c1", sess="s1", resources=(), ts=0.0):
    return LogRecord(seq=seq, op_id=op_id, phase=phase, op=op,
                     client_id=client, session_id=sess,
                     resources=tuple(resources), ts=ts)


class InterruptedRenameTest(unittest.TestCase):
    def test_committed_rename_with_consistent_parents_replays(self):
        data = RecoveryInput(
            sessions=[live_session()],
            locks=[lock("L1", "/a", expires=2000.0),
                   lock("L2", "/b", expires=2000.0)],
            log=[
                rec(1, "op-rn", "prepare", "rename", resources=("/a", "/b")),
                rec(2, "op-rn", "commit", "rename", resources=("/a", "/b"),
                    ts=100.0),
            ],
            rename_deps=[RenameDependency("op-rn", "/a", "/b", 3, 7)],
            parent_states={"/a": 3, "/b": 7},
            now=NOW,
        )
        plan = plan_recovery(data)
        self.assertEqual(len(plan.recovery_order), 1)
        action = plan.recovery_order[0]
        self.assertEqual(action.op_id, "op-rn")
        self.assertEqual(action.action, ACTION_REPLAY)
        self.assertEqual(plan.blocked, [])

    def test_prepared_only_rename_from_dead_client_is_undone(self):
        data = RecoveryInput(
            sessions=[session()],
            locks=[lock("L1", "/a", expires=900.0),  # expired before NOW
                   lock("L2", "/b", expires=900.0)],
            log=[rec(1, "op-rn", "prepare", "rename", resources=("/a", "/b"))],
            rename_deps=[RenameDependency("op-rn", "/a", "/b", 3, 7)],
            parent_states={"/a": 3, "/b": 7},
            now=NOW,
        )
        plan = plan_recovery(data)
        self.assertEqual(len(plan.recovery_order), 1)
        self.assertEqual(plan.recovery_order[0].action, ACTION_UNDO)
        self.assertEqual(plan.recovery_order[0].op_id, "op-rn")


class LeaseExpiryTest(unittest.TestCase):
    def test_prepared_op_with_valid_lease_must_wait(self):
        data = RecoveryInput(
            sessions=[live_session()],
            locks=[lock("L1", "/i1", expires=2000.0)],  # still valid at NOW
            log=[rec(1, "op-w", "prepare", "lock", resources=("/i1",))],
            now=NOW,
        )
        plan = plan_recovery(data)
        self.assertEqual(plan.recovery_order, [])
        waiting_ops = [b for b in plan.blocked
                       if b.reason == REASON_LEASE_VALID]
        self.assertEqual(len(waiting_ops), 1)
        self.assertEqual(waiting_ops[0].item, "op-w")

    def test_stale_client_commit_is_rejected(self):
        # Lease expired at t=500 but the commit is stamped t=900: the old
        # client lost its lease and must not be allowed to commit.
        data = RecoveryInput(
            sessions=[session()],
            locks=[lock("L1", "/i1", expires=500.0)],
            log=[
                rec(1, "op-x", "prepare", "create", resources=("/i1",),
                    ts=100.0),
                rec(2, "op-x", "commit", "create", resources=("/i1",),
                    ts=900.0),
            ],
            now=NOW,
        )
        plan = plan_recovery(data)
        self.assertEqual(len(plan.recovery_order), 1)
        self.assertEqual(plan.recovery_order[0].action, ACTION_UNDO)

    def test_orphaned_lock_reclaimable_only_after_expiry(self):
        data = RecoveryInput(
            sessions=[session("c1", "s1"), session("c2", "s2")],
            locks=[
                lock("L-expired", "/i1", client="c1", expires=900.0),
                lock("L-live", "/i2", client="c2", sess="s2",
                     expires=2000.0),
            ],
            log=[],
            now=NOW,
        )
        plan = plan_recovery(data)
        self.assertEqual([o.lock_id for o in plan.orphaned_locks],
                         ["L-expired"])
        self.assertTrue(plan.orphaned_locks[0].reclaimable)
        waiting = [b for b in plan.blocked
                   if b.reason == REASON_WAITING_LEASE_EXPIRY]
        self.assertEqual([b.item for b in waiting], ["L-live"])

    def test_connected_session_lock_is_neither_orphaned_nor_waiting(self):
        data = RecoveryInput(
            sessions=[session("c1", "s1", connected=True)],
            locks=[lock("L1", "/i1", expires=2000.0)],
            log=[],
            now=NOW,
        )
        plan = plan_recovery(data)
        self.assertEqual(plan.orphaned_locks, [])
        self.assertEqual(plan.blocked, [])


class ParentStateMismatchTest(unittest.TestCase):
    def test_committed_rename_blocked_on_parent_mismatch(self):
        data = RecoveryInput(
            sessions=[live_session()],
            locks=[lock("L1", "/a", expires=2000.0),
                   lock("L2", "/b", expires=2000.0)],
            log=[
                rec(1, "op-rn", "prepare", "rename", resources=("/a", "/b")),
                rec(2, "op-rn", "commit", "rename", resources=("/a", "/b"),
                    ts=100.0),
            ],
            rename_deps=[RenameDependency("op-rn", "/a", "/b", 3, 7)],
            parent_states={"/a": 3, "/b": 8},  # dst parent diverged
            now=NOW,
        )
        plan = plan_recovery(data)
        self.assertEqual(plan.recovery_order, [])
        mismatched = [b for b in plan.blocked
                      if b.reason == REASON_PARENT_MISMATCH]
        self.assertEqual(len(mismatched), 1)
        self.assertIn("/b", mismatched[0].detail)

    def test_missing_parent_state_blocks_rename(self):
        data = RecoveryInput(
            sessions=[live_session()],
            locks=[lock("L1", "/a", expires=2000.0),
                   lock("L2", "/b", expires=2000.0)],
            log=[
                rec(1, "op-rn", "prepare", "rename", resources=("/a", "/b")),
                rec(2, "op-rn", "commit", "rename", resources=("/a", "/b"),
                    ts=100.0),
            ],
            rename_deps=[RenameDependency("op-rn", "/a", "/b", 3, 7)],
            parent_states={"/a": 3},  # /b never persisted
            now=NOW,
        )
        plan = plan_recovery(data)
        self.assertEqual(plan.recovery_order, [])
        mismatched = [b for b in plan.blocked
                      if b.reason == REASON_PARENT_MISMATCH]
        self.assertEqual(len(mismatched), 1)


class DuplicateLogTest(unittest.TestCase):
    def test_duplicate_records_replay_once(self):
        data = RecoveryInput(
            sessions=[session()],
            locks=[lock("L1", "/i1", expires=2000.0)],
            log=[
                rec(1, "op-1", "prepare", "create", resources=("/i1",)),
                rec(2, "op-1", "commit", "create", resources=("/i1",),
                    ts=100.0),
                rec(3, "op-1", "commit", "create", resources=("/i1",),
                    ts=100.0),  # duplicate journal write
                rec(4, "op-1", "prepare", "create", resources=("/i1",)),
            ],
            now=NOW,
        )
        plan = plan_recovery(data)
        self.assertEqual(len(plan.recovery_order), 1)
        self.assertEqual(plan.recovery_order[0].action, ACTION_REPLAY)

    def test_duplicate_prepare_from_dead_client_undone_once(self):
        data = RecoveryInput(
            sessions=[session()],
            locks=[lock("L1", "/i1", expires=100.0)],
            log=[
                rec(1, "op-1", "prepare", "unlink", resources=("/i1",)),
                rec(2, "op-1", "prepare", "unlink", resources=("/i1",)),
                rec(3, "op-1", "prepare", "unlink", resources=("/i1",)),
            ],
            now=NOW,
        )
        plan = plan_recovery(data)
        self.assertEqual(len(plan.recovery_order), 1)
        self.assertEqual(plan.recovery_order[0].action, ACTION_UNDO)

    def test_aborted_op_is_not_recovered(self):
        data = RecoveryInput(
            sessions=[session()],
            locks=[lock("L1", "/i1", expires=100.0)],
            log=[
                rec(1, "op-1", "prepare", "unlink", resources=("/i1",)),
                rec(2, "op-1", "abort", "unlink", resources=("/i1",)),
            ],
            now=NOW,
        )
        plan = plan_recovery(data)
        self.assertEqual(plan.recovery_order, [])
        self.assertEqual(plan.blocked, [])


class DependencyOrderTest(unittest.TestCase):
    def test_shared_resource_ops_run_in_log_order(self):
        data = RecoveryInput(
            sessions=[session()],
            locks=[lock("L1", "/i1", expires=2000.0),
                   lock("L2", "/i2", expires=2000.0)],
            log=[
                rec(1, "op-b", "prepare", "create", resources=("/i2",)),
                rec(2, "op-b", "commit", "create", resources=("/i2",),
                    ts=10.0),
                rec(3, "op-a", "prepare", "create", resources=("/i1",)),
                rec(4, "op-a", "commit", "create", resources=("/i1",),
                    ts=20.0),
                rec(5, "op-c", "prepare", "unlink", resources=("/i1",)),
                rec(6, "op-c", "commit", "unlink", resources=("/i1",),
                    ts=30.0),
            ],
            now=NOW,
        )
        plan = plan_recovery(data)
        order = [a.op_id for a in plan.recovery_order]
        self.assertLess(order.index("op-a"), order.index("op-c"))
        self.assertEqual(order, ["op-b", "op-a", "op-c"])

    def test_rename_waits_for_earlier_parent_dir_ops(self):
        data = RecoveryInput(
            sessions=[session()],
            locks=[lock("L1", "/a", expires=2000.0),
                   lock("L2", "/b", expires=2000.0)],
            log=[
                rec(1, "op-mkdir", "prepare", "create", resources=("/b",)),
                rec(2, "op-mkdir", "commit", "create", resources=("/b",),
                    ts=10.0),
                rec(3, "op-rn", "prepare", "rename", resources=("/a", "/b")),
                rec(4, "op-rn", "commit", "rename", resources=("/a", "/b"),
                    ts=20.0),
            ],
            rename_deps=[RenameDependency("op-rn", "/a", "/b", 1, 1)],
            parent_states={"/a": 1, "/b": 1},
            now=NOW,
        )
        plan = plan_recovery(data)
        order = [a.op_id for a in plan.recovery_order]
        self.assertEqual(order, ["op-mkdir", "op-rn"])

    def test_dependency_cycle_is_blocked(self):
        # Two renames swapping entries between /a and /b: each depends on
        # the other's parent directories, forming a cycle.
        data = RecoveryInput(
            sessions=[session()],
            locks=[lock("L1", "/a", expires=2000.0),
                   lock("L2", "/b", expires=2000.0)],
            log=[
                rec(1, "op-rn1", "prepare", "rename", resources=("/a",)),
                rec(2, "op-rn1", "commit", "rename", resources=("/a",),
                    ts=10.0),
                rec(3, "op-rn2", "prepare", "rename", resources=("/b",)),
                rec(4, "op-rn2", "commit", "rename", resources=("/b",),
                    ts=20.0),
            ],
            rename_deps=[
                RenameDependency("op-rn1", "/a", "/b", 1, 1),
                RenameDependency("op-rn2", "/b", "/a", 1, 1),
            ],
            parent_states={"/a": 1, "/b": 1},
            now=NOW,
        )
        plan = plan_recovery(data)
        cyclic = {b.item for b in plan.blocked
                  if b.reason == REASON_DEPENDENCY_CYCLE}
        self.assertEqual(cyclic, {"op-rn1", "op-rn2"})
        self.assertEqual(plan.recovery_order, [])


if __name__ == "__main__":
    unittest.main()
