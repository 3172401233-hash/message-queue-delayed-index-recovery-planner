from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set, Tuple


class Verdict(str, Enum):
    """What recovery should do with one surviving index entry."""

    DELETE = "delete"
    REBUILD = "rebuild"
    WAIT = "wait"


@dataclass(frozen=True)
class IndexEntry:
    """One row of the independent not-before index.

    ``position`` is the index position/sequence used by the index commit point.
    ``queue_offset`` is the offset the message was or will be enqueued at in the
    original queue once its not-before time is reached.
    """

    message_id: str
    not_before: int
    queue_offset: int
    position: int


@dataclass(frozen=True)
class DeliveryRecord:
    """A confirmed enqueue into the original queue.

    The record only exists once the enqueue itself is durably acknowledged, so
    index cleanup may never overtake it.
    """

    message_id: str
    queue_offset: int


@dataclass(frozen=True)
class Lease:
    """Scanning lease held by an index worker.

    ``fencing_token`` increases on every takeover; a lease is current when
    ``now < expires_at`` and superseded once a higher token exists.
    """

    owner: str
    fencing_token: int
    expires_at: int


@dataclass(frozen=True)
class RecoveryInput:
    entries: Tuple[IndexEntry, ...]
    delivered: Tuple[DeliveryRecord, ...]
    lease: Optional[Lease]
    scan_cursor: int
    scan_cursor_token: Optional[int]
    index_commit_point: int
    now: int


@dataclass(frozen=True)
class EntryDecision:
    entry: IndexEntry
    verdict: Verdict
    duplicate_risk: bool
    reason: str


@dataclass(frozen=True)
class RecoveryPlan:
    decisions: Tuple[EntryDecision, ...]
    claimed_cursor: int
    safe_cursor: int
    claimed_commit_point: int
    effective_commit_point: int
    lease_valid: bool
    remaining_delayed: Tuple[IndexEntry, ...]
    duplicate_risk_ids: Tuple[str, ...]
    risk_notes: Tuple[str, ...]
    recovery_order: Tuple[EntryDecision, ...]

    def ids_for(self, verdict: Verdict) -> Tuple[str, ...]:
        return tuple(
            decision.entry.message_id
            for decision in self.decisions
            if decision.verdict is verdict
        )

    def decided(self, message_id: str) -> EntryDecision:
        for decision in self.decisions:
            if decision.entry.message_id == message_id:
                return decision
        raise KeyError(message_id)

    def note_contains(self, fragment: str) -> bool:
        return any(fragment in note for note in self.risk_notes)

    def describe(self) -> str:
        lines = [
            "delayed-index recovery plan",
            "  lease valid: {0}".format(self.lease_valid),
            "  cursor: claimed={0} safe={1}".format(
                self.claimed_cursor, self.safe_cursor
            ),
            "  commit point: claimed={0} effective={1}".format(
                self.claimed_commit_point, self.effective_commit_point
            ),
            "  recovery order:",
        ]
        for index, decision in enumerate(self.recovery_order, start=1):
            lines.append(
                "    {0}. {1} {2} (duplicate_risk={3}) - {4}".format(
                    index,
                    decision.entry.message_id,
                    decision.verdict.value,
                    decision.duplicate_risk,
                    decision.reason,
                )
            )
        lines.append(
            "  duplicate risk: {0}".format(
                ",".join(self.duplicate_risk_ids) or "none"
            )
        )
        lines.append(
            "  remaining delayed: {0}".format(
                ",".join(entry.message_id for entry in self.remaining_delayed)
                or "none"
            )
        )
        if self.risk_notes:
            lines.append("  notes:")
            lines.extend("    - {0}".format(note) for note in self.risk_notes)
        return "\n".join(lines)
