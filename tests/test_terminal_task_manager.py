import asyncio

import pytest

from src.bot.task_manager import TerminalTaskManager


@pytest.mark.asyncio
async def test_task_manager_register_and_filter_running_tasks():
    manager = TerminalTaskManager()

    task1 = asyncio.create_task(asyncio.sleep(5))
    task2 = asyncio.create_task(asyncio.sleep(5))

    task1_id = await manager.register_task("FULL STATS", 10, "UserA", task1)
    task2_id = await manager.register_task("ISS LEVEL 0", 20, "UserB", task2)

    user_a_tasks = await manager.list_running_tasks(requester_id=10, is_admin=False)
    admin_tasks = await manager.list_running_tasks(requester_id=10, is_admin=True)

    assert {task.task_id for task in user_a_tasks} == {task1_id}
    assert {task.task_id for task in admin_tasks} == {task1_id, task2_id}

    task1.cancel()
    task2.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task1
    with pytest.raises(asyncio.CancelledError):
        await task2


@pytest.mark.asyncio
async def test_task_manager_cancel_task_enforces_ownership_and_marks_state():
    manager = TerminalTaskManager()

    task = asyncio.create_task(asyncio.sleep(5))
    task_id = await manager.register_task("ISS LEVEL 1", 99, "Owner", task)

    denied, denied_msg = await manager.cancel_task(task_id, requester_id=42, is_admin=False)
    assert denied is False
    assert "only stop tasks you initiated" in denied_msg

    allowed, allowed_msg = await manager.cancel_task(task_id, requester_id=99, is_admin=False)
    assert allowed is True
    assert "Cancellation requested" in allowed_msg

    with pytest.raises(asyncio.CancelledError):
        await task

    await manager.mark_cancelled(task_id)
    running = await manager.list_running_tasks(requester_id=99, is_admin=True)
    assert all(row.task_id != task_id for row in running)


@pytest.mark.asyncio
async def test_task_manager_cancel_all_respects_role_scope():
    manager = TerminalTaskManager()

    user_task = asyncio.create_task(asyncio.sleep(5))
    other_task = asyncio.create_task(asyncio.sleep(5))

    user_task_id = await manager.register_task("RANKED STATS", 1, "UserOne", user_task)
    other_task_id = await manager.register_task("CASUAL STATS", 2, "UserTwo", other_task)

    count_user, _msg_user = await manager.cancel_all(requester_id=1, is_admin=False)
    assert count_user == 1

    with pytest.raises(asyncio.CancelledError):
        await user_task

    # Other user's task should still be running until admin stops it.
    still_running = await manager.list_running_tasks(requester_id=2, is_admin=True)
    assert any(task.task_id == other_task_id for task in still_running)

    count_admin, _msg_admin = await manager.cancel_all(requester_id=999, is_admin=True)
    assert count_admin >= 1

    with pytest.raises(asyncio.CancelledError):
        await other_task

    await manager.mark_cancelled(user_task_id)
    await manager.mark_cancelled(other_task_id)


@pytest.mark.asyncio
async def test_task_manager_progress_completion_status_removes_task_from_running():
    manager = TerminalTaskManager()

    task = asyncio.create_task(asyncio.sleep(5))
    task_id = await manager.register_task("BUILD CO-PLAY EDGES", 11, "Crawler", task)

    await manager.update_progress(
        task_id,
        stage="Finalizing",
        percent=100.0,
        detail="Co-play crawl complete",
        status="completed",
    )

    tasks = await manager.list_tasks(requester_id=11, is_admin=True)
    row = next(item for item in tasks if item.task_id == task_id)
    assert row.status == "completed"
    assert row.stage == "Completed"
    assert row.progress_percent == pytest.approx(100.0)
    assert row.detail == "Co-play crawl complete"

    running = await manager.list_running_tasks(requester_id=11, is_admin=True)
    assert all(item.task_id != task_id for item in running)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
