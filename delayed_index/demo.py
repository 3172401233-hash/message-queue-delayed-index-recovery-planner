from __future__ import annotations

from .model import DeliveryRecord, IndexEntry, Lease, RecoveryInput
from .recovery import recover


def build_scenario() -> RecoveryInput:
    entries = (
        IndexEntry("m1", not_before=0, queue_offset=10, position=1),
        IndexEntry("m2", not_before=50, queue_offset=11, position=2),
        IndexEntry("m3", not_before=60, queue_offset=12, position=3),
        IndexEntry("m5", not_before=80, queue_offset=14, position=5),
        IndexEntry("m4", not_before=200, queue_offset=13, position=4),
    )
    return RecoveryInput(
        entries=entries,
        delivered=(DeliveryRecord("m1", queue_offset=10),),
        lease=Lease(owner="worker-a", fencing_token=1, expires_at=200),
        scan_cursor=3,
        scan_cursor_token=1,
        index_commit_point=1,
        now=100,
    )


def main() -> None:
    print(recover(build_scenario()).describe())


if __name__ == "__main__":
    main()
