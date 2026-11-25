from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import importlib.util
import logging
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("dsh_async_plugin_test", ROOT / "plugin.py")
bridge = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bridge
SPEC.loader.exec_module(bridge)
SPEC = importlib.util.spec_from_file_location("dsh_async_transport_test", ROOT / "desktop_transport.py")
transport = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(transport)
JOB = "11111111-1111-4111-8111-111111111111"
RECEIPT = "这是守岸人组织后的回复。"


class AsyncDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="maibot-delivery-")
        self.addCleanup(self.temporary.cleanup)
        self.plugin = bridge.DshBridgePlugin()
        self.plugin._plugin_config_instance = bridge.DshBridgeConfig()
        self.plugin.config.desktop.enabled = True
        self.plugin.config.dsh.receipt_format = "text"
        self.plugin.config.permission.enforce_tools = False
        self.plugin._running = False
        self.plugin._delivery_progress = {}
        self.plugin._receipt_prompt = (ROOT / "prompts" / "receipt.zh-CN.prompt").read_text(encoding="utf-8")
        self.plugin._ctx = SimpleNamespace(
            paths=SimpleNamespace(data_dir=self.temporary.name),
            logger=logging.getLogger("bridge-test"),
            send=SimpleNamespace(text=AsyncMock(return_value=True), image=AsyncMock(return_value=True)),
            maisaka=SimpleNamespace(context=SimpleNamespace(append=AsyncMock())),
            config=SimpleNamespace(get=AsyncMock(side_effect=lambda key: {
                "bot.nickname": "守岸人", "personality.personality": "沉静、温柔，关注真实用户。",
                "personality.reply_style": "简短自然，不额外追问。",
            }[key])),
            llm=SimpleNamespace(generate=AsyncMock(return_value={"success": True, "response": RECEIPT})),
        )
        self.plugin._desktop_client = SimpleNamespace(
            submit_task=Mock(return_value={"status": "accepted", "jobId": JOB}), acknowledge=Mock()
        )
        self.plugin._desktop_endpoint = Mock(return_value=(12345, "unused-token"))
        self.item = {
            "jobId": JOB, "streamId": "real-stream", "context": {},
            "status": "completed", "final": "requested answer only", "files": [],
        }

    async def test_submit_returns_accepted_without_execution_or_visible_reply(self):
        self.plugin._run_and_report = AsyncMock()
        response = await self.plugin._tool_dsh_run(task="capture screenshot", stream_id="real-stream", timeout_seconds=60)
        self.assertEqual(response["job_id"], JOB)
        self.assertEqual(response["status"], "queued")
        self.plugin._run_and_report.assert_not_awaited()
        self.plugin.ctx.send.text.assert_not_awaited()
        self.plugin.ctx.maisaka.context.append.assert_not_awaited()
        self.assertFalse(self.plugin._reject_busy())
        sent = self.plugin._desktop_client.submit_task.call_args.kwargs
        self.assertEqual(sent["stream_id"], "real-stream")
        self.assertEqual(sent["task"], "capture screenshot")

    async def test_no_false_acceptance_when_submit_fails(self):
        self.plugin._desktop_client.submit_task.side_effect = transport.DesktopTransportError("HTTP failure")
        with self.assertLogs("bridge-test", level="WARNING"):
            result = await self.plugin._tool_dsh_run(task="test", stream_id="real-stream", timeout_seconds=60)
        self.assertFalse(result["success"])
        self.plugin.ctx.send.text.assert_not_awaited()

    async def test_ack_failure_and_restart_do_not_repeat_delivered_parts(self):
        self.item["files"] = ["C:/workspace/screen.png"]
        self.plugin._send_image_file = AsyncMock(return_value=True)
        self.plugin._desktop_client.acknowledge.side_effect = RuntimeError("lost ack response")
        with self.assertRaisesRegex(RuntimeError, "lost ack"):
            await self.plugin._deliver_notification(self.item, 12345, "token")
        self.plugin._delivery_progress = self.plugin._load_delivery_progress()
        self.plugin._desktop_client.acknowledge.side_effect = None
        await self.plugin._deliver_notification(self.item, 12345, "token")
        self.plugin.ctx.send.text.assert_awaited_once_with(RECEIPT, "real-stream")
        self.plugin.ctx.llm.generate.assert_awaited_once()
        self.plugin._send_image_file.assert_awaited_once()
        recorded = self.plugin.ctx.maisaka.context.append.call_args.kwargs["segments"][0]["content"]
        self.assertEqual(recorded, RECEIPT)
        self.assertEqual(self.plugin._delivery_progress, {})

    async def test_failed_delivery_not_acknowledged(self):
        self.plugin.ctx.send.text.return_value = False
        with self.assertRaises(RuntimeError):
            await self.plugin._deliver_notification(self.item, 12345, "token")
        self.plugin._desktop_client.acknowledge.assert_not_called()

    async def test_card_retry_resumes_page_after_restart_and_format_change(self):
        self.plugin.config.dsh.receipt_format = "card"
        self.plugin._render_receipt_cards = Mock(return_value=["page-one", "page-two"])
        self.plugin.ctx.send.image.side_effect = [True, False, True]
        with self.assertRaisesRegex(RuntimeError, "回执未发送成功"):
            await self.plugin._deliver_notification(self.item, 12345, "token")
        self.plugin._desktop_client.acknowledge.assert_not_called()
        self.plugin._delivery_progress = self.plugin._load_delivery_progress()
        self.plugin.config.dsh.receipt_format = "text"
        await self.plugin._deliver_notification(self.item, 12345, "token")
        self.assertEqual([call.args[0] for call in self.plugin.ctx.send.image.call_args_list],
                         ["page-one", "page-two", "page-two"])
        self.plugin.ctx.send.text.assert_not_awaited()
        self.plugin.ctx.llm.generate.assert_awaited_once()
        self.assertEqual(self.plugin.ctx.maisaka.context.append.call_args.kwargs["segments"][0]["content"], RECEIPT)
        self.plugin._desktop_client.acknowledge.assert_called_once()

    async def test_card_render_error_leaves_notification_unacknowledged(self):
        self.plugin.config.dsh.receipt_format = "card"
        self.plugin._render_receipt_cards = Mock(side_effect=RuntimeError("missing font"))
        with self.assertRaisesRegex(RuntimeError, "missing font"):
            await self.plugin._deliver_notification(self.item, 12345, "token")
        self.plugin.ctx.send.text.assert_not_awaited()
        self.plugin._desktop_client.acknowledge.assert_not_called()

    async def test_long_results_are_not_truncated(self):
        self.plugin.config.dsh.max_output_chars = 100
        self.item["final"] = "abc" * 120
        self.plugin.ctx.llm.generate.return_value = {"success": True, "response": self.item["final"]}
        await self.plugin._deliver_notification(self.item, 12345, "token")
        actual = "".join(call.args[0] for call in self.plugin.ctx.send.text.call_args_list)
        self.assertEqual(actual, self.item["final"])
        self.plugin._desktop_client.acknowledge.assert_called_once()

    async def test_file_failure_resumes_without_repeating_text(self):
        self.item["files"] = ["C:/workspace/screen.png"]
        self.plugin._send_image_file = AsyncMock(side_effect=[False, True])
        with self.assertRaises(RuntimeError):
            await self.plugin._deliver_notification(self.item, 12345, "token")
        self.plugin._desktop_client.acknowledge.assert_not_called()
        await self.plugin._deliver_notification(self.item, 12345, "token")
        self.plugin.ctx.send.text.assert_awaited_once()
        self.plugin._desktop_client.acknowledge.assert_called_once()

    async def test_direct_qq_summary_is_not_forwarded_as_shorekeeper_speech(self):
        self.item.update(delivery="qq", qqDelivery="sent", final="任务已完成。我通过 notify_qq 接口发送了以下回复：守岸人，你好……")
        await self.plugin._deliver_notification(self.item, 12345, "token")
        self.plugin.ctx.send.text.assert_awaited_once_with("泰缇斯已经把内容通过 QQ 发给你了。", "real-stream")
        recorded = self.plugin.ctx.maisaka.context.append.call_args.kwargs["segments"][0]["content"]
        self.assertNotIn("notify_qq", recorded)
        self.assertNotIn("任务已完成", recorded)

    async def test_explicit_tethys_send_to_me_selects_direct_delivery(self):
        task = "让泰提斯系统把截图发给我"
        result = await self.plugin._tool_dsh_run(
            task=task, delivery="qq", stream_id="real-stream", timeout_seconds=60
        )
        self.assertTrue(result["accepted"])
        sent = self.plugin._desktop_client.submit_task.call_args.kwargs
        self.assertEqual(sent["delivery"], "qq")
        self.assertEqual(sent["task"], task)

    async def test_status_only_direct_delivery_does_not_regenerate_or_forward_content(self):
        self.item.update(delivery="qq", qqDelivery="sent", final="", files=[])
        await self.plugin._deliver_notification(self.item, 12345, "token")
        self.plugin.ctx.llm.generate.assert_not_awaited()
        self.plugin.ctx.send.text.assert_awaited_once_with("泰缇斯已经把内容通过 QQ 发给你了。", "real-stream")

    def test_queued_notification_is_not_called_delivered(self):
        self.item.update(delivery="qq", qqDelivery="queued")
        body = self.plugin._notification_body(self.item)
        self.assertIn("排队", body)
        self.assertNotIn("已经通过 QQ 回复", body)

    def test_model_claim_alone_does_not_prove_direct_delivery(self):
        self.item.update(delivery="qq", qqDelivery="none", final="我已经通过 QQ 成功发送了消息。")
        self.assertEqual(self.plugin._notification_body(self.item), "泰缇斯尚未确认 QQ 回复已发送。")

    async def test_reply_to_assistant_is_attributed_by_maibot_replyer(self):
        self.item.update(delivery="to_assistant", final="守岸人，你好。")
        self.plugin.ctx.llm.generate.return_value = {"success": True, "response": "泰缇斯刚刚向我问好了。"}
        self.assertEqual(await self.plugin._compose_receipt(self.item), "泰缇斯刚刚向我问好了。")
        self.plugin.ctx.llm.generate.assert_awaited_once()

    async def test_delivery_recipient_is_carried_to_host(self):
        await self.plugin._tool_dsh_run(task="让泰缇斯回复你一段话", delivery="to_assistant", stream_id="real-stream", timeout_seconds=60)
        submitted = self.plugin._desktop_client.submit_task.call_args.kwargs
        self.assertEqual(submitted["delivery"], "to_assistant")
        self.assertEqual(submitted["task"], "让泰缇斯回复你一段话")

    async def test_screenshot_uses_persona_receipt_after_image_delivery(self):
        events = []
        self.item.update(
            request="截取当前屏幕并发给我",
            final="已捕获全屏截图（2560×1440 PNG），文件已声明交付：.ai/temp/desktop-screenshot.png。",
            files=["C:/workspace/.ai/temp/desktop-screenshot.png"],
        )

        async def image(*args):
            events.append("image")
            return True

        async def generate(**kwargs):
            events.append("replyer")
            self.assertIn("沉静、温柔", kwargs["prompt"][0]["content"])
            self.assertIn("截取当前屏幕", kwargs["prompt"][1]["content"])
            self.assertEqual(kwargs["task_name"], "replyer")
            return {"success": True, "response": "截图已经发给你了。"}

        self.plugin._send_image_file = AsyncMock(side_effect=image)
        self.plugin.ctx.llm.generate.side_effect = generate
        await self.plugin._deliver_notification(self.item, 12345, "token")
        self.assertEqual(events, ["image", "replyer"])
        self.plugin.ctx.send.text.assert_awaited_once_with("截图已经发给你了。", "real-stream")

    async def test_reply_model_failure_never_falls_back_to_raw_executor_output(self):
        self.item["files"] = ["C:/workspace/screen.png"]
        self.plugin._send_image_file = AsyncMock(return_value=True)
        self.plugin.ctx.llm.generate.return_value = {"success": False, "error": "model unavailable"}
        with self.assertRaisesRegex(RuntimeError, "回执生成失败"):
            await self.plugin._deliver_notification(self.item, 12345, "token")
        self.plugin.ctx.send.text.assert_not_awaited()
        self.plugin._desktop_client.acknowledge.assert_not_called()
        self.plugin.ctx.llm.generate.return_value = {"success": True, "response": RECEIPT}
        await self.plugin._deliver_notification(self.item, 12345, "token")
        self.plugin._send_image_file.assert_awaited_once()
        self.plugin.ctx.send.text.assert_awaited_once_with(RECEIPT, "real-stream")


class TransportTests(unittest.TestCase):
    def test_invalid_notification_rejected(self):
        with unittest.mock.patch.object(transport, "_request_json", return_value={"notification": {"jobId": "../escape"}}):
            with self.assertRaises(transport.DesktopTransportError):
                transport.get_notification(port=1, token_file="unused")

    def test_file_only_notification_accepted(self):
        item = {"jobId": JOB, "streamId": "real-stream", "context": {}, "status": "completed", "final": "", "files": ["C:/workspace/a.png"]}
        with unittest.mock.patch.object(transport, "_request_json", return_value={"notification": item}):
            self.assertEqual(transport.get_notification(port=1, token_file="unused")["files"], item["files"])


if __name__ == "__main__":
    unittest.main()
