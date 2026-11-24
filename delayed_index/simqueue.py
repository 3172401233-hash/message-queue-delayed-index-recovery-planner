from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class EnqueueResult:
    message_id: str
    queue_offset: int
    duplicated: bool


class SimulatedQueue:
    """In-memory stand-in for the real original queue and its ack log.

    Everything here lives only in process memory: it simulates enqueue,
    idempotent redelivery and durable delivery acknowledgements.
    """

    def __init__(self) -> None:
        self._offset_by_id: Dict[str, int] = {}
        self._ids_by_offset: Dict[int, str] = {}
        self._acked_ids: set = set()
        self._next_offset = 0

    def enqueue(
        self, message_id: str, queue_offset: Optional[int] = None
    ) -> EnqueueResult:
        existing = self._offset_by_id.get(message_id)
        if existing is not None:
            return EnqueueResult(
                message_id=message_id,
                queue_offset=existing,
                duplicated=True,
            )
        if queue_offset is None:
            offset = self._next_offset
        else:
            offset = queue_offset
            if offset in self._ids_by_offset:
                raise ValueError(
                    "offset {0} is already occupied".format(offset)
                )
        self._next_offset = max(self._next_offset, offset + 1)
        self._offset_by_id[message_id] = offset
        self._ids_by_offset[offset] = message_id
        return EnqueueResult(
            message_id=message_id,
            queue_offset=offset,
            duplicated=False,
        )

    def ack(self, message_id: str) -> None:
        if message_id not in self._offset_by_id:
            raise KeyError("unknown message: {0}".format(message_id))
        self._acked_ids.add(message_id)

    def is_acked(self, message_id: str) -> bool:
        return message_id in self._acked_ids

    def offset_of(self, message_id: str) -> int:
        return self._offset_by_id[message_id]

    def delivery_records(self) -> Tuple:
        from .model import DeliveryRecord

        return tuple(
            DeliveryRecord(message_id=message_id, queue_offset=offset)
            for message_id, offset in sorted(
                self._offset_by_id.items(), key=lambda item: item[1]
            )
            if message_id in self._acked_ids
        )

    def crash(self) -> None:
        """Simulate losing un-acknowledged enqueue state on an index crash."""
        unacked = [
            message_id
            for message_id in self._offset_by_id
            if message_id not in self._acked_ids
        ]
        for message_id in unacked:
            offset = self._offset_by_id.pop(message_id)
            self._ids_by_offset.pop(offset, None)

    def size(self) -> int:
        return len(self._offset_by_id)

    def snapshot(self) -> List[str]:
        return [
            self._ids_by_offset[offset]
            for offset in sorted(self._ids_by_offset)
        ]
