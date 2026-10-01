"""
Tracked Background Tasks
------------------------
asyncio only keeps a weak reference to tasks created with asyncio.create_task():
an untracked fire-and-forget task can be garbage-collected mid-flight, and its
exception is lost. BackgroundTaskSet keeps a strong reference until completion
and logs every failure.
"""

import asyncio
import logging
from typing import Any, Coroutine, Optional, Set

logger = logging.getLogger("core.tasks")


# [FEATURE: TRACKED_BACKGROUND_TASKS] Strong references + logged exceptions for fire-and-forget tasks.
# Raison: orchestrator, websocket and poller dispatched callbacks with bare create_task();
#         a failing executor disappeared without any trace (documented asyncio pitfall).
# Attention: call cancel_all() on shutdown so no task outlives its owner.
class BackgroundTaskSet:
    def __init__(self, owner_name: str):
        self.owner_name = owner_name
        self._tasks: Set[asyncio.Task] = set()
        self.failed_count = 0

    def spawn(self, coroutine: Coroutine[Any, Any, Any], name: Optional[str] = None) -> asyncio.Task:
        task = asyncio.create_task(coroutine, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._on_done)
        return task

    def _on_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self.failed_count += 1
            logger.error(
                "[%s] background task %s failed",
                self.owner_name,
                task.get_name(),
                exc_info=(type(error), error, error.__traceback__),
            )

    def __len__(self) -> int:
        return len(self._tasks)

    async def wait_all(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def cancel_all(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        await self.wait_all()
