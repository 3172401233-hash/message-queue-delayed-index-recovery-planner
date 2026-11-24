from __future__ import annotations

import unittest

from delayed_index import (
    DeliveryRecord,
    IndexEntry,
    Lease,
    RecoveryInput,
    SimulatedQueue,
    Verdict,
    recover,
)


def entry(message_id, not_before, queue_offset, position):
    return IndexEntry(
        message_id=message_id,
        not_before=not_before,
        queue_offset=queue_offset,
        position=position,
    )


def plan_input(
    entries,
    delivered=(),
    lease=None,
    scan_cursor=0,
    scan_cursor_token=None,
    index_commit_point=0,
    now=100,
):
    return RecoveryInput(
        entries=tuple(entries),
        delivered=tuple(delivered),
        lease=lease,
        scan_cursor=scan_cursor,
        scan_cursor_token=scan_cursor_token,
        index_commit_point=index_commit_point,
        now=now,
    )


class IndexCrashRecoveryTest(unittest.TestCase):
    def test_classifies_delete_rebuild_and_wait_after_index_crash(self):
        lease = Lease(owner="worker-a", fencing_token=1, expires_at=200)
        entries = [
            entry("m1", not_before=0, queue_offset=10, position=1),
            entry("m2", not_before=50, queue_offset=11, position=2),
            entry("m3", not_before=60, queue_offset=12, position=3),
            entry("m5", not_before=80, queue_offset=14, position=5),
            entry("m4", not_before=200, queue_offset=13, position=4),
        ]
        delivered = [DeliveryRecord(message_id="m1", queue_offset=10)]

        plan = recover(
            plan_input(
                entries,
                delivered=delivered,
                lease=lease,
                scan_cursor=3,
                scan_cursor_token=1,
                index_commit_point=1,
            )
        )

        self.assertTrue(plan.lease_valid)
        self.assertEqual(plan.safe_cursor, 3)
        self.assertEqual(plan.effective_commit_point, 1)

        self.assertEqual(plan.decided("m1").verdict, Verdict.DELETE)
        self.assertEqual(plan.decided("m2").verdict, Verdict.REBUILD)
        self.assertEqual(plan.decided("m3").verdict, Verdict.REBUILD)
        self.assertEqual(plan.decided("m5").verdict, Verdict.REBUILD)
        self.assertEqual(plan.decided("m4").verdict, Verdict.WAIT)

        self.assertTrue(plan.decided("m2").duplicate_risk)
        self.assertTrue(plan.decided("m3").duplicate_risk)
        self.assertFalse(plan.decided("m5").duplicate_risk)

        order = [decision.entry.message_id for decision in plan.recovery_order]
        self.assertEqual(order, ["m1", "m2", "m3", "m5", "m4"])

        self.assertEqual(
            [item.message_id for item in plan.remaining_delayed],
            ["m2", "m3", "m5", "m4"],
        )
        self.assertEqual(plan.ids_for(Verdict.DELETE), ("m1",))
        self.assertEqual(
            set(plan.duplicate_risk_ids), {"m2", "m3"}
        )


class DuplicateDeliveryTest(unittest.TestCase):
    def test_rebuild_is_idempotent_against_the_simulated_queue(self):
        queue = SimulatedQueue()
        crashed_enqueue = queue.enqueue("m2", queue_offset=11)

        lease = Lease(owner="worker-a", fencing_token=1, expires_at=200)
        entries = [entry("m2", not_before=50, queue_offset=11, position=2)]
        plan = recover(
            plan_input(
                entries,
                delivered=(),
                lease=lease,
                scan_cursor=3,
                scan_cursor_token=1,
                index_commit_point=1,
            )
        )
        decision = plan.decided("m2")
        self.assertEqual(decision.verdict, Verdict.REBUILD)
        self.assertTrue(decision.duplicate_risk)

        retried = queue.enqueue("m2")
        self.assertTrue(retried.duplicated)
        self.assertEqual(retried.queue_offset, crashed_enqueue.queue_offset)

        queue.ack("m2")
        after_ack = recover(
            plan_input(
                entries,
                delivered=queue.delivery_records(),
                lease=lease,
                scan_cursor=3,
                scan_cursor_token=1,
                index_commit_point=1,
            )
        )
        self.assertEqual(after_ack.decided("m2").verdict, Verdict.DELETE)
        self.assertFalse(after_ack.decided("m2").duplicate_risk)
        self.assertEqual(after_ack.duplicate_risk_ids, ())

    def test_offset_mismatch_is_rebuilt_with_duplicate_risk(self):
        entries = [entry("m1", not_before=0, queue_offset=10, position=1)]
        delivered = [DeliveryRecord(message_id="m1", queue_offset=99)]
        plan = recover(
            plan_input(
                entries,
                delivered=delivered,
                lease=Lease(owner="worker-a", fencing_token=1, expires_at=200),
                scan_cursor=1,
                scan_cursor_token=1,
            )
        )
        decision = plan.decided("m1")
        self.assertEqual(decision.verdict, Verdict.REBUILD)
        self.assertTrue(decision.duplicate_risk)
        self.assertTrue(plan.note_contains("m1"))


class CursorRollbackTest(unittest.TestCase):
    def test_expired_lease_and_takeover_cannot_advance_the_cursor(self):
        entries = [
            entry("m2", not_before=50, queue_offset=11, position=2),
            entry("m3", not_before=60, queue_offset=12, position=3),
        ]
        delivered = [DeliveryRecord(message_id="m2", queue_offset=11)]
        kwargs = dict(
            entries=entries,
            delivered=delivered,
            scan_cursor=5,
            scan_cursor_token=1,
            index_commit_point=2,
        )

        expired = recover(
            plan_input(lease=Lease("worker-a", 1, expires_at=50), **kwargs)
        )
        self.assertFalse(expired.lease_valid)
        self.assertEqual(expired.safe_cursor, 2)
        self.assertTrue(expired.decided("m3").duplicate_risk)
        self.assertTrue(expired.note_contains("cannot be advanced"))

        takeover = recover(
            plan_input(lease=Lease("worker-b", 2, expires_at=500), **kwargs)
        )
        self.assertTrue(takeover.lease_valid)
        self.assertEqual(takeover.safe_cursor, 2)
        self.assertTrue(takeover.decided("m3").duplicate_risk)
        self.assertTrue(takeover.note_contains("does not match lease token"))

    def test_trusted_cursor_past_window_rebuilds_without_risk(self):
        lease = Lease(owner="worker-a", fencing_token=7, expires_at=500)
        entries = [entry("m9", not_before=50, queue_offset=20, position=9)]
        plan = recover(
            plan_input(
                entries,
                lease=lease,
                scan_cursor=8,
                scan_cursor_token=7,
                index_commit_point=8,
            )
        )
        self.assertEqual(plan.safe_cursor, 8)
        decision = plan.decided("m9")
        self.assertEqual(decision.verdict, Verdict.REBUILD)
        self.assertFalse(decision.duplicate_risk)


class CommitPointTest(unittest.TestCase):
    def test_commit_point_cannot_overtake_unconfirmed_delivery(self):
        entries = [
            entry("m1", not_before=0, queue_offset=10, position=1),
            entry("m2", not_before=10, queue_offset=11, position=2),
            entry("m3", not_before=20, queue_offset=12, position=3),
        ]
        delivered = [
            DeliveryRecord("m1", 10),
            DeliveryRecord("m3", 12),
        ]
        lease = Lease(owner="worker-a", fencing_token=1, expires_at=500)
        plan = recover(
            plan_input(
                entries,
                delivered=delivered,
                lease=lease,
                scan_cursor=3,
                scan_cursor_token=1,
                index_commit_point=3,
            )
        )
        self.assertEqual(plan.claimed_commit_point, 3)
        self.assertEqual(plan.effective_commit_point, 1)
        self.assertTrue(plan.note_contains("overtakes confirmed delivery prefix"))
        self.assertEqual(plan.decided("m1").verdict, Verdict.DELETE)
        self.assertEqual(plan.decided("m2").verdict, Verdict.REBUILD)
        self.assertEqual(plan.decided("m3").verdict, Verdict.DELETE)


class RemainingDelayedTest(unittest.TestCase):
    def test_future_entries_wait_and_are_ordered_by_not_before(self):
        lease = Lease(owner="worker-a", fencing_token=1, expires_at=500)
        entries = [
            entry("late", not_before=900, queue_offset=30, position=3),
            entry("soon", not_before=150, queue_offset=31, position=4),
        ]
        plan = recover(
            plan_input(
                entries,
                lease=lease,
                scan_cursor=2,
                scan_cursor_token=1,
            )
        )
        self.assertEqual(
            [item.message_id for item in plan.remaining_delayed],
            ["soon", "late"],
        )
        self.assertEqual(plan.ids_for(Verdict.WAIT), ("late", "soon"))
        self.assertIn("recovery plan", plan.describe())


if __name__ == "__main__":
    unittest.main()
