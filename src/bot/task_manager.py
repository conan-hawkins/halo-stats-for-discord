from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import count
from typing import Dict, List, Optional, Tuple


@dataclass
class ActiveTerminalTask:
    task_id: str
    action_label: str
    requester_id: int
    requester_name: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "running"
    stage: str = "Running"
    progress_percent: Optional[float] = None
    detail: str = ""
    error: str = ""
    task: Optional[asyncio.Task] = None

    @property
    def elapsed_seconds(self) -> int:
        return max(0, int((datetime.now(timezone.utc) - self.created_at).total_seconds()))


class TerminalTaskManager:
    """Tracks terminal-dispatched actions across all terminal sessions."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._id_counter = count(1)
        self._tasks: Dict[str, ActiveTerminalTask] = {}

    async def register_task(
        self,
        action_label: str,
        requester_id: int,
        requester_name: str,
        task: asyncio.Task,
    ) -> str:
        task_id = f"T{next(self._id_counter):06d}"
        async with self._lock:
            self._tasks[task_id] = ActiveTerminalTask(
                task_id=task_id,
                action_label=action_label,
                requester_id=requester_id,
                requester_name=requester_name,
                task=task,
            )
        return task_id

    async def update_progress(
        self,
        task_id: str,
        stage: Optional[str] = None,
        percent: Optional[float] = None,
        detail: Optional[str] = None,
    ) -> None:
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            if task.status not in {"running", "cancelling"}:
                return

            if isinstance(stage, str) and stage.strip():
                task.stage = stage.strip()
            if isinstance(percent, (int, float)):
                task.progress_percent = max(0.0, min(100.0, float(percent)))
            if isinstance(detail, str):
                task.detail = detail.strip()

    async def mark_completed(self, task_id: str) -> None:
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            task.status = "completed"
            if not task.stage:
                task.stage = "Completed"

    async def mark_failed(self, task_id: str, error: str) -> None:
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            task.status = "failed"
            task.error = str(error or "")
            task.stage = "Failed"

    async def mark_cancelled(self, task_id: str) -> None:
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            task.status = "cancelled"
            task.stage = "Cancelled"

    async def list_tasks(self, requester_id: int, is_admin: bool) -> List[ActiveTerminalTask]:
        async with self._lock:
            if is_admin:
                rows = list(self._tasks.values())
            else:
                rows = [t for t in self._tasks.values() if t.requester_id == requester_id]

            rows.sort(key=lambda t: t.created_at, reverse=True)
            return rows

    async def list_running_tasks(self, requester_id: int, is_admin: bool) -> List[ActiveTerminalTask]:
        rows = await self.list_tasks(requester_id=requester_id, is_admin=is_admin)
        return [t for t in rows if t.status in {"running", "cancelling"}]

    async def cancel_task(self, task_id: str, requester_id: int, is_admin: bool) -> Tuple[bool, str]:
        normalized_id = str(task_id or "").strip().upper()
        if not normalized_id:
            return False, "Task id is required."

        async with self._lock:
            task = self._tasks.get(normalized_id)
            if not task:
                return False, f"Task {normalized_id} was not found."

            if not is_admin and task.requester_id != requester_id:
                return False, "You can only stop tasks you initiated."

            if task.status not in {"running", "cancelling"}:
                return False, f"Task {normalized_id} is not running."

            if task.task and not task.task.done():
                task.status = "cancelling"
                task.stage = "Cancelling"
                task.task.cancel()
                return True, f"Cancellation requested for {normalized_id}."

            return False, f"Task {normalized_id} is no longer active."

    async def cancel_all(self, requester_id: int, is_admin: bool) -> Tuple[int, str]:
        async with self._lock:
            cancellable = [
                task
                for task in self._tasks.values()
                if task.status in {"running", "cancelling"}
                and (is_admin or task.requester_id == requester_id)
            ]

            for task in cancellable:
                if task.task and not task.task.done():
                    task.status = "cancelling"
                    task.stage = "Cancelling"
                    task.task.cancel()

            count_cancelled = len(cancellable)

        if count_cancelled == 0:
            return 0, "No running tasks were available to stop."

        if is_admin:
            return count_cancelled, f"Cancellation requested for {count_cancelled} running task(s)."

        return count_cancelled, f"Cancellation requested for {count_cancelled} of your running task(s)."


terminal_task_manager = TerminalTaskManager()
