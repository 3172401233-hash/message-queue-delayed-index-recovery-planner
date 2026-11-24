from __future__ import annotations

from typing import Dict, List, Tuple

from .model import (
    DeliveryRecord,
    EntryDecision,
    IndexEntry,
    Lease,
    RecoveryInput,
    RecoveryPlan,
    Verdict,
)


def _lease_is_valid(lease: Lease, now: int) -> bool:
    return lease is not None and lease.expires_at > now


def _cursor_is_trusted(
    lease: Lease, lease_valid: bool, scan_cursor_token: int
) -> bool:
    """A persisted cursor may only be used under the lease that wrote it."""
    if not lease_valid:
        return False
    if scan_cursor_token is None:
        return False
    return scan_cursor_token == lease.fencing_token


def _confirmed_prefix(
    entries: Tuple[IndexEntry, ...],
    delivered_by_id: Dict[str, DeliveryRecord],
) -> int:
    """Highest contiguous index position whose enqueue was acknowledged.

    The commit point is a prefix marker. Any gap where the enqueue is not
    confirmed stops advancement even if later entries are already delivered.
    """
    prefix = 0
    for entry in sorted(entries, key=lambda item: item.position):
        record = delivered_by_id.get(entry.message_id)
        if record is None or record.queue_offset != entry.queue_offset:
            break
        prefix = entry.position
    return prefix


def _classify_entry(
    entry: IndexEntry,
    delivered_by_id: Dict[str, DeliveryRecord],
    safe_cursor: int,
    claimed_cursor: int,
    cursor_trusted: bool,
    now: int,
) -> EntryDecision:
    record = delivered_by_id.get(entry.message_id)
    if record is not None:
        if record.queue_offset == entry.queue_offset:
            return EntryDecision(
                entry=entry,
                verdict=Verdict.DELETE,
                duplicate_risk=False,
                reason="enqueue already acknowledged; stale index row can be removed",
            )
        return EntryDecision(
            entry=entry,
            verdict=Verdict.REBUILD,
            duplicate_risk=True,
            reason=(
                "delivery record points at offset {0} but index expected {1}; "
                "re-enqueue must be idempotent".format(
                    record.queue_offset, entry.queue_offset
                )
            ),
        )

    if entry.not_before > now:
        return EntryDecision(
            entry=entry,
            verdict=Verdict.WAIT,
            duplicate_risk=False,
            reason="not-before {0} is still in the future".format(entry.not_before),
        )

    if not cursor_trusted:
        return EntryDecision(
            entry=entry,
            verdict=Verdict.REBUILD,
            duplicate_risk=True,
            reason=(
                "scan cursor is not trusted; the entry may already have been "
                "enqueued before the crash"
            ),
        )

    if entry.position <= safe_cursor:
        return EntryDecision(
            entry=entry,
            verdict=Verdict.REBUILD,
            duplicate_risk=True,
            reason=(
                "due entry was already inside the crashed scan window; verify "
                "idempotently before re-enqueuing"
            ),
        )

    return EntryDecision(
        entry=entry,
        verdict=Verdict.REBUILD,
        duplicate_risk=False,
        reason=(
            "due entry sits past the durable scan cursor {0}; enqueue it on the "
            "new scan".format(claimed_cursor)
        ),
    )


def recover(data: RecoveryInput) -> RecoveryPlan:
    now = data.now
    lease_valid = _lease_is_valid(data.lease, now)
    cursor_trusted = _cursor_is_trusted(
        data.lease, lease_valid, data.scan_cursor_token
    )

    delivered_by_id: Dict[str, DeliveryRecord] = {
        record.message_id: record for record in data.delivered
    }

    confirmed_prefix = _confirmed_prefix(data.entries, delivered_by_id)
    effective_commit = min(data.index_commit_point, confirmed_prefix)

    if cursor_trusted:
        safe_cursor = max(data.scan_cursor, effective_commit)
    else:
        safe_cursor = effective_commit

    decisions: List[EntryDecision] = [
        _classify_entry(
            entry=entry,
            delivered_by_id=delivered_by_id,
            safe_cursor=safe_cursor,
            claimed_cursor=data.scan_cursor,
            cursor_trusted=cursor_trusted,
            now=now,
        )
        for entry in data.entries
    ]

    order_rank = {Verdict.DELETE: 0, Verdict.REBUILD: 1, Verdict.WAIT: 2}
    recovery_order = tuple(
        sorted(
            decisions,
            key=lambda decision: (
                order_rank[decision.verdict],
                decision.entry.not_before,
                decision.entry.position,
            ),
        )
    )

    remaining_delayed = tuple(
        sorted(
            (
                decision.entry
                for decision in decisions
                if decision.verdict is not Verdict.DELETE
            ),
            key=lambda entry: (entry.not_before, entry.position),
        )
    )
    duplicate_risk_ids = tuple(
        decision.entry.message_id
        for decision in decisions
        if decision.duplicate_risk
    )

    risk_notes: List[str] = []
    if not lease_valid:
        risk_notes.append(
            "no valid lease; the scan cursor cannot be advanced and is clamped "
            "to commit point {0}".format(effective_commit)
        )
    elif not cursor_trusted:
        risk_notes.append(
            "cursor fencing token {0} does not match lease token {1}; cursor "
            "rolled back to commit point {2}".format(
                data.scan_cursor_token,
                data.lease.fencing_token,
                effective_commit,
            )
        )
    if data.index_commit_point > effective_commit:
        risk_notes.append(
            "claimed commit point {0} overtakes confirmed delivery prefix {1}; "
            "clamped back".format(data.index_commit_point, effective_commit)
        )
    for decision in decisions:
        if decision.entry.message_id in delivered_by_id and decision.duplicate_risk:
            risk_notes.append(
                "{0}: {1}".format(decision.entry.message_id, decision.reason)
            )

    return RecoveryPlan(
        decisions=tuple(decisions),
        claimed_cursor=data.scan_cursor,
        safe_cursor=safe_cursor,
        claimed_commit_point=data.index_commit_point,
        effective_commit_point=effective_commit,
        lease_valid=lease_valid,
        remaining_delayed=remaining_delayed,
        duplicate_risk_ids=duplicate_risk_ids,
        risk_notes=tuple(risk_notes),
        recovery_order=recovery_order,
    )
