from pathlib import Path
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock

import asyncio
import importlib.util
import sys


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("dsh_scheduler_test_plugin", ROOT / "plugin.py")
bridge = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bridge
SPEC.loader.exec_module(bridge)


class ScheduleDispatchTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.plugin = bridge.DshBridgePlugin()
        config = self.plugin.build_default_config()
        if hasattr(config, "desktop"):
            config.desktop.enabled = False
        self.plugin.set_plugin_config(config)
        self.plugin._schedule_wakeup = asyncio.Event()
        self.plugin._scheduled_runs = {}
        self.plugin._tasks = {
            "later": {"id": "later", "next_at": 20},
            "earlier": {"id": "earlier", "next_at": 10},
        }

    async def test_dispatch_orders_due_tasks_and_keeps_one_cli_worker(self):
        started = asyncio.Event()
        finish = asyncio.Event()

        async def fire(task):
            started.set()
            await finish.wait()
            self.plugin._tasks.pop(task["id"])

        self.plugin._fire_task = AsyncMock(side_effect=fire)
        self.plugin._dispatch_due_tasks(30)
        worker = self.plugin._scheduled_runs["earlier"]
        await started.wait()
        self.plugin._dispatch_due_tasks(30)
        self.assertEqual(list(self.plugin._scheduled_runs), ["earlier"])
        self.plugin._fire_task.assert_awaited_once()
        finish.set()
        await worker
        self.assertTrue(self.plugin._schedule_wakeup.is_set())
        self.plugin._dispatch_due_tasks(30)
        await self.plugin._scheduled_runs["later"]
        self.assertEqual(self.plugin._tasks, {})

    async def test_cancelled_task_is_ignored_before_worker_starts(self):
        self.plugin._fire_task = AsyncMock()
        self.plugin._dispatch_due_tasks(15)
        self.plugin._tasks.pop("earlier")
        await self.plugin._scheduled_runs["earlier"]
        self.plugin._fire_task.assert_not_awaited()

    async def test_desktop_submissions_are_bounded_and_deduplicated(self):
        self.plugin.config.desktop.enabled = True
        for index in range(5):
            task_id = f"desktop-{index}"
            self.plugin._tasks[task_id] = {"id": task_id, "next_at": index + 1}
        self.plugin._fire_task = AsyncMock()
        self.plugin._dispatch_due_tasks(30)
        self.plugin._dispatch_due_tasks(30)
        self.assertEqual(list(self.plugin._scheduled_runs), [f"desktop-{index}" for index in range(4)])
        await asyncio.gather(*self.plugin._scheduled_runs.values())
        self.assertEqual(self.plugin._fire_task.await_count, 4)

    async def test_cancelled_recurring_task_is_not_rescheduled_after_execution(self):
        task = self.plugin._tasks["earlier"]
        task["trigger"] = {"kind": "every", "seconds": 60}
        self.plugin._running = False
        self.plugin._ctx = SimpleNamespace(logger=Mock(), send=SimpleNamespace(text=AsyncMock()))
        self.plugin._save_tasks = Mock()

        async def run_and_cancel(*args):
            self.plugin._tasks.pop("earlier")

        self.plugin._run_and_report = AsyncMock(side_effect=run_and_cancel)
        await self.plugin._fire_task(task)
        self.assertNotIn("earlier", self.plugin._tasks)
        self.assertEqual(task["next_at"], 10)
        self.plugin._save_tasks.assert_not_called()
