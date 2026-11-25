"""麦麦 × DeepSeek Harness (dsh) 桥插件（带任务守卫 + 最终结果直发）。

作用：把 DeepSeek Harness (dsh) 接入麦麦。
  - 命令 `/dsh [--timeout 秒] <任务>`：把任务外包给 dsh，结果回传当前聊天流。
  - 工具 `dsh_run`：让麦麦（Planner）在聊天中自主把重活交给 dsh（自主下单）。

执行模型（异步 + 守卫）：
  - 桌面模式：提交只等待任务落盘；独立通知循环领取结果，与下单调用解耦。
  - 单并发执行：桌面宿主可排队多个任务；CLI 同时只接受一个任务。
  - 强制超时：任务必须带超时，桥校验在 [timeout_min, timeout_max] 内。
  - 进展判据（三信号）：输出 / 进程 CPU 活跃 / 子进程存在，任一有变化即续命。
  - 重复检测：输出原地复读超阈值 → 判定卡循环，终止。
  - 总硬上限：即使一直在动，也不超过声明的 timeout。
  - 分层终止：先温和 terminate，宽限期后 taskkill /T /F 杀整棵进程树。

回传形态（只回答结果）：
  - CLI 与桌面模式均静默执行，不发送接单、进度、完成通知或过程转发。
  - 成功时直接发送最终答案与交付文件，失败时发送实际错误原因。
  - 桌面通知回灌只记录请求结果，不触发额外 Planner 回复。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Literal, Optional

from maibot_sdk import Command, Field, MaiBotPlugin, PluginConfigBase, Tool
from maibot_sdk.types import ToolParameterInfo, ToolParamType

import asyncio
import base64
import importlib.util
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import uuid
import zipfile

try:  # 进程脉搏（可选，缺失则退化为“仅输出/子进程”判据）
    import psutil  # type: ignore
except Exception:  # noqa: BLE001
    psutil = None  # type: ignore

_CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)
_IS_WINDOWS = os.name == "nt"
_POLL_SECONDS = 5.0


class PluginSectionConfig(PluginConfigBase):
    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否启用插件")
    config_version: str = Field(default="0.8.0", description="配置版本")


class DshConfig(PluginConfigBase):
    __ui_label__ = "dsh"
    __ui_icon__ = "terminal"
    __ui_order__ = 1

    command: str = Field(default="dsh", description="dsh 可执行命令（如 dsh，或 C:\\nodejs\\dsh.cmd）")
    extra_args: str = Field(default="--profile headless", description="固定附加参数；桥会自动追加 --json")
    workspace_dir: str = Field(default="", description="dsh 工作目录；留空则用 data/dsh-bridge/workspace")
    timeout_default: int = Field(default=300, description="未指定时使用的默认超时（秒）", ge=10, le=3600)
    timeout_min: int = Field(default=60, description="允许的最小超时（秒）", ge=10, le=3600)
    timeout_max: int = Field(default=1800, description="允许的最大超时（秒）", ge=30, le=7200)
    heartbeat_seconds: int = Field(default=180, description="无进展心跳：三信号全静超过此秒数即判卡死", ge=20, le=1800)
    repeat_threshold: int = Field(default=25, description="重复检测：同一行连续出现达到此次数即判循环", ge=5, le=500)
    grace_seconds: int = Field(default=15, description="温和终止后的宽限等待（秒），超时才强杀进程树", ge=0, le=120)
    max_output_chars: int = Field(default=8000, description="文本回退时最大字符数", ge=100, le=50000)
    max_forward_nodes: int = Field(default=40, description="合并转发最多节点数（超出则折叠中段）", ge=3, le=200)
    node_max_chars: int = Field(default=1500, description="单个转发节点最大字符数", ge=100, le=8000)
    display_mode: str = Field(default="forward", description="回传形态：forward=合并转发 / text=纯文本")
    receipt_format: Literal["card", "text"] = Field(
        default="text", description="最终回执：card=Markdown 排版图片卡片，text=可复制的纯文本"
    )
    deliver_max_files: int = Field(default=8, description="单次任务最多打包发送的交付物文件数", ge=1, le=50)
    api_key_env: str = Field(default="DEEPSEEK_API_KEY", description="存放 API Key 的环境变量名")
    api_key: str = Field(default="", description="DeepSeek API Key（明文，留空则用系统环境变量；勿分享本文件）")
    permission_mode: Literal["workspace-write", "read-only", "danger-full-access"] = Field(
        default="danger-full-access",
        description=(
            "dsh 沙箱权限模式。默认 danger-full-access：git clone / npm 等需要子进程管道的命令在 "
            "workspace-write 下会被 Windows ACL 沙箱拒绝。要更严可改为 workspace-write 或 read-only，"
            "但需接受相应命令失败。"
        ),
    )
    context_inject_chars: int = Field(
        default=600,
        description="回灌进麦麦上下文的结果摘要最大字符数（越小越省上下文，超出部分截断）",
        ge=100,
        le=5000,
    )
    allow_self_schedule: bool = Field(
        default=True,
        description="是否允许 dsh 经 schedule-requests 自建定时任务（默认开；关则禁止，风险自负）",
    )


class PermissionConfig(PluginConfigBase):
    __ui_label__ = "权限"
    __ui_icon__ = "shield"
    __ui_order__ = 2

    enabled: bool = Field(default=True, description="是否启用权限名单")
    mode: Literal["whitelist", "blacklist"] = Field(
        default="whitelist",
        description="名单模式：whitelist=仅名单内可用；blacklist=名单内不可用、其余放行",
    )
    allowed_users: str = Field(default="", description="白名单用户 ID，逗号分隔；白名单模式下留空且启用则拒绝所有人（fail-closed）")
    allowed_groups: str = Field(
        default="",
        description="白名单群 ID，逗号分隔；群聊里取不到发言人 ID 时作为兜底",
    )
    blocked_users: str = Field(default="", description="黑名单用户 ID，逗号分隔；黑名单模式下命中即拒绝")
    blocked_groups: str = Field(default="", description="黑名单群 ID，逗号分隔；黑名单模式下命中即拒绝")
    enforce_tools: bool = Field(
        default=True,
        description="是否对 dsh_run / schedule_* 工具也施加名单（默认开=安全；关=工具面放开，风险自负）",
    )


class DesktopConfig(PluginConfigBase):
    __ui_label__ = "桌面宿主"
    __ui_icon__ = "monitor"
    __ui_order__ = 3

    enabled: bool = Field(
        default=False,
        description=(
            "任务是否改走已运行的 DSH Desktop 宿主（需先安装宿主桥插件）。"
            "开=复用宿主会话与桌面端配置；关=沿用每次起 dsh CLI 进程的旧行为"
        ),
    )
    endpoint_file: str = Field(
        default="",
        description="宿主桥写的 endpoint.json 路径（含 host/port；留空则桌面模式直接报配置错误）",
    )
    token_file: str = Field(
        default="",
        description="宿主桥的 bearer token 文件路径（64 位十六进制；留空则桌面模式直接报配置错误）",
    )
    sender: str = Field(default="maibot", description="提交方标识，须与宿主桥配置里的 senderId 一致")


class DshBridgeConfig(PluginConfigBase):
    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    dsh: DshConfig = Field(default_factory=DshConfig)
    permission: PermissionConfig = Field(default_factory=PermissionConfig)
    desktop: DesktopConfig = Field(default_factory=DesktopConfig)


class _RunState:
    """一次任务运行的共享状态（读线程与监控循环共用）。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.events: list[dict[str, Any]] = []
        self.final_text = ""
        self.deliverables: list[str] = []
        self.silent = False
        self.last_progress = time.time()
        self.repeat_line: Optional[str] = None
        self.repeat_count = 0
        self.repeat_triggered = False

    def push_raw(self, raw_line: str, repeat_threshold: int) -> None:
        text = raw_line.rstrip("\r\n")
        if not text.strip():
            return
        stripped = text.strip()
        evt: Optional[dict[str, Any]] = None
        if stripped.startswith("{"):
            try:
                obj = json.loads(stripped)
                if isinstance(obj, dict):
                    evt = obj
            except Exception:  # noqa: BLE001
                evt = None
        with self.lock:
            self.last_progress = time.time()
            if evt is not None:
                evt["_ts"] = time.time()
                self.events.append(evt)
                if str(evt.get("type") or "") == "final":
                    self.final_text = str(evt.get("text") or "")
                probe = str(evt.get("text") or evt.get("tool") or stripped)
            else:
                self.events.append({"type": "raw", "text": text, "_ts": time.time()})
                probe = text
            probe = probe.strip()
            if probe and probe == self.repeat_line:
                self.repeat_count += 1
            else:
                self.repeat_line = probe
                self.repeat_count = 1
            if self.repeat_count >= repeat_threshold:
                self.repeat_triggered = True

    def touch(self) -> None:
        with self.lock:
            self.last_progress = time.time()

    def snapshot(self) -> tuple[float, bool]:
        with self.lock:
            return self.last_progress, self.repeat_triggered

    def events_copy(self) -> list[dict[str, Any]]:
        with self.lock:
            return list(self.events)

    def final_copy(self) -> str:
        with self.lock:
            return self.final_text

    def deliverable_copy(self) -> list[str]:
        with self.lock:
            return list(self.deliverables)


class DshBridgePlugin(MaiBotPlugin):
    config_model = DshBridgeConfig

    _MAX_IMAGE_BYTES = 8 * 1024 * 1024
    _DESKTOP_SCHEDULE_CONCURRENCY = 4

    async def on_load(self) -> None:
        self._running = False
        self._current_pid: Optional[int] = None
        self._cpu_prev: dict[int, float] = {}
        self._tasks: dict[str, dict[str, Any]] = {}
        self._scheduler_running = True
        self._schedule_wakeup = asyncio.Event()
        self._scheduled_runs: dict[str, asyncio.Task[None]] = {}
        await asyncio.to_thread(self._load_tasks)
        self._desktop_client = await asyncio.to_thread(self._desktop_transport)
        self._delivery_progress = await asyncio.to_thread(self._load_delivery_progress)
        self._receipt_prompt = await asyncio.to_thread(
            (Path(__file__).parent / "prompts" / "receipt.zh-CN.prompt").read_text, encoding="utf-8"
        )
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        self._notification_task = asyncio.create_task(self._notification_loop())
        self.ctx.logger.info(
            "麦麦×dsh 桥插件已加载 (command=%s, timeout=%s~%s, display=%s)",
            self.config.dsh.command,
            self.config.dsh.timeout_min,
            self.config.dsh.timeout_max,
            self.config.dsh.display_mode,
        )

    async def on_unload(self) -> None:
        self._scheduler_running = False
        self._notification_task.cancel()
        self._scheduler_task.cancel()
        await asyncio.gather(self._notification_task, self._scheduler_task, return_exceptions=True)
        runs = tuple(self._scheduled_runs.values())
        if not self.config.desktop.enabled:
            for run in runs:
                run.cancel()
        await asyncio.gather(*runs, return_exceptions=True)
        pid = self._current_pid
        if pid:
            await asyncio.to_thread(self._kill_tree, pid)
        self.ctx.logger.info("麦麦×dsh 桥插件已卸载")

    # ── 定时任务（桥内置调度） ────────────────────────────────

    def _tasks_file(self) -> Path:
        return Path(self.ctx.paths.data_dir) / "scheduled_tasks.json"

    def _load_tasks(self) -> None:
        try:
            fp = self._tasks_file()
            if fp.is_file():
                data = json.loads(fp.read_text(encoding="utf-8"))
                for t in data if isinstance(data, list) else []:
                    if isinstance(t, dict) and t.get("id"):
                        self._tasks[str(t["id"])] = t
            self.ctx.logger.info("已载入定时任务 %d 个", len(self._tasks))
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("读取定时任务失败", exc_info=True)

    def _save_tasks(self) -> None:
        try:
            fp = self._tasks_file()
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(json.dumps(list(self._tasks.values()), ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("保存定时任务失败", exc_info=True)

    @staticmethod
    def _compute_next(trigger: dict[str, Any], base: float) -> Optional[float]:
        """根据触发器推算下次触发时间戳（秒）。返回 None 表示不再触发。"""
        kind = str(trigger.get("kind") or "")
        if kind == "after":
            return base + float(trigger.get("seconds") or 0)
        if kind == "at":
            at = float(trigger.get("timestamp") or 0)
            return at if at > base + 0.5 else None
        if kind == "every":
            every = max(60.0, float(trigger.get("seconds") or 60))
            anchor = float(trigger.get("anchor") or base)
            if anchor > base:
                return anchor
            n = int((base - anchor) // every) + 1
            return anchor + n * every
        if kind == "cron":
            return DshBridgePlugin._cron_next(str(trigger.get("expression") or ""), base)
        return None

    @staticmethod
    def _cron_field_match(field: str, value: int) -> bool:
        """单个 cron 字段匹配（* / a / a-b / */n / a-b/n / 逗号列表）。"""
        for part in str(field).split(","):
            part = part.strip()
            if not part:
                continue
            step = 1
            base = part
            if "/" in part:
                base, _, s = part.partition("/")
                try:
                    step = int(s)
                except ValueError:
                    continue
                if step <= 0:
                    continue
            if base == "*":
                lo, hi = 0, 10 ** 9
            elif "-" in base:
                a, _, b = base.partition("-")
                try:
                    lo, hi = int(a), int(b)
                except ValueError:
                    continue
                if lo > hi:
                    lo, hi = hi, lo
            else:
                try:
                    v = int(base)
                except ValueError:
                    continue
                if step == 1:
                    if value == v:
                        return True
                    continue
                lo, hi = v, 10 ** 9
            if lo <= value <= hi and (value - lo) % step == 0:
                return True
        return False

    @classmethod
    def _cron_next(cls, expr: str, base: float) -> Optional[float]:
        """求 cron（5 字段，minute 精度）下一个匹配时点；限制搜索 366 天。"""
        import datetime as _dt
        parts = (expr or "").split()
        if len(parts) != 5:
            return None
        mi_f, ho_f, dom_f, mo_f, dow_f = parts
        start = _dt.datetime.fromtimestamp(base + 1).replace(second=0, microsecond=0) + _dt.timedelta(minutes=1)
        limit = start + _dt.timedelta(days=366)
        cur = start
        while cur < limit:
            dow = (cur.weekday() + 1) % 7  # cron: 周日=0
            if (
                cls._cron_field_match(mo_f, cur.month)
                and cls._cron_field_match(dom_f, cur.day)
                and cls._cron_field_match(dow_f, dow)
                and cls._cron_field_match(ho_f, cur.hour)
                and cls._cron_field_match(mi_f, cur.minute)
            ):
                return cur.timestamp()
            cur += _dt.timedelta(minutes=1)
        return None

    async def _scheduler_loop(self) -> None:
        """扫描到期任务，由独立协程执行或提交。"""
        while self._scheduler_running:
            self._schedule_wakeup.clear()
            try:
                self._dispatch_due_tasks(time.time())
            except Exception:  # noqa: BLE001
                self.ctx.logger.warning("调度循环异常", exc_info=True)
            try:
                await asyncio.wait_for(self._schedule_wakeup.wait(), timeout=10)
            except TimeoutError:
                pass

    def _dispatch_due_tasks(self, now: float) -> None:
        limit = self._DESKTOP_SCHEDULE_CONCURRENCY if self.config.desktop.enabled else 1
        available = limit - len(self._scheduled_runs)
        if available <= 0:
            return
        due = []
        for task_id, task in self._tasks.items():
            if task_id in self._scheduled_runs:
                continue
            next_at = float(task.get("next_at") or 0)
            if 0 < next_at <= now:
                due.append((next_at, task_id))
        for _, task_id in sorted(due)[:available]:
            self._scheduled_runs[task_id] = asyncio.create_task(self._run_scheduled_task(task_id))

    async def _run_scheduled_task(self, task_id: str) -> None:
        try:
            task = self._tasks.get(task_id)
            if task is not None:
                await self._fire_task(task)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("触发定时任务失败", exc_info=True)
        finally:
            self._scheduled_runs.pop(task_id, None)
            self._schedule_wakeup.set()

    async def _fire_task(self, task: dict[str, Any]) -> None:
        task_id = str(task["id"])
        stream_id = str(task.get("stream_id") or "")
        prompt = str(task.get("prompt") or "")
        ctx = task.get("ctx") or {}
        title = str(task.get("title") or "定时任务")
        desktop_enabled = self.config.desktop.enabled
        if self._reject_busy():
            # 正忙：顺延 60s 后再试，避免与手动任务打架
            task["next_at"] = time.time() + 60
            self._save_tasks()
            self.ctx.logger.info("定时任务「%s」因忙碌顺延", title)
            return
        self.ctx.logger.info("定时任务「%s」开始执行", title)
        if not desktop_enabled:
            self._running = True
        try:
            await self._run_and_report(prompt, stream_id, int(task.get("timeout") or self.config.dsh.timeout_default), ctx)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("定时任务执行失败", exc_info=True)
            if desktop_enabled:
                # 宿主未接收时保留本次调度，不把一次性任务误当成已执行。
                if self._tasks.get(task_id) is task:
                    task["next_at"] = time.time() + 60
                    self._save_tasks()
                return
            self._running = False
        if self._tasks.get(task_id) is not task:
            return
        trigger = task.get("trigger") or {}
        kind = str(trigger.get("kind") or "")
        if kind == "every":
            task["next_at"] = self._compute_next(trigger, time.time())
        elif kind == "cron":
            task["next_at"] = self._compute_next(trigger, time.time())
        else:
            self._tasks.pop(task_id, None)
        self._save_tasks()

    def _parse_schedule(self, after_seconds: Any, every_seconds: Any, at: Any, cron: Any = None) -> Optional[dict[str, Any]]:
        """把可选参数解析成一个触发器。"""
        now = time.time()
        if after_seconds not in (None, ""):
            return {"kind": "after", "seconds": float(after_seconds), "next_at": now + float(after_seconds)}
        if every_seconds not in (None, ""):
            sec = max(60.0, float(every_seconds))
            return {"kind": "every", "seconds": sec, "anchor": now, "next_at": now + sec}
        if cron not in (None, ""):
            expr = str(cron).strip()
            nxt = self._cron_next(expr, now)
            if nxt is None:
                return None
            return {"kind": "cron", "expression": expr, "next_at": nxt}
        if at not in (None, ""):
            ts = self._parse_iso(str(at))
            if ts is None or ts <= now:
                return None
            return {"kind": "at", "timestamp": ts, "at_text": str(at), "next_at": ts}
        return None

    def _create_task(self, title: str, prompt: str, after: Any, every: Any, at: Any, cron: Any, stream_id: str, ctx: dict[str, Any]) -> Optional[str]:
        """建一个定时任务并持久化；返回任务 id 或 None。"""
        trigger = self._parse_schedule(after, every, at, cron)
        if not trigger:
            return None
        next_at = trigger.pop("next_at")
        task_id = "st_" + uuid.uuid4().hex
        self._tasks[task_id] = {
            "id": task_id,
            "title": title,
            "prompt": prompt,
            "trigger": trigger,
            "next_at": next_at,
            "timeout": int(self.config.dsh.timeout_default),
            "stream_id": stream_id,
            "ctx": ctx or {},
            "created_at": time.time(),
        }
        self._save_tasks()
        self._schedule_wakeup.set()
        return task_id

    def _schedule_requests_dir(self, stream_id: str) -> Path:
        return Path(self._workspace(stream_id)) / "schedule-requests"

    async def _process_schedule_requests(self, stream_id: str, ctx: dict[str, Any]) -> None:
        """扫描 dsh 写下的 schedule-requests/*.json，建任务后移到 .done（幂等）。"""
        if not self.config.dsh.allow_self_schedule:
            return
        req_dir = self._schedule_requests_dir(stream_id)
        if not req_dir.is_dir():
            return
        created: list[str] = []
        for fp in sorted(req_dir.glob("*.json")):
            try:
                req = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue
            if isinstance(req, dict):
                title = str(req.get("title") or "").strip()
                prompt = str(req.get("prompt") or "").strip()
                if title and prompt:
                    tid = self._create_task(
                        title,
                        prompt,
                        req.get("after_seconds"),
                        req.get("every_seconds"),
                        req.get("at"),
                        req.get("cron"),
                        stream_id,
                        ctx or {},
                    )
                    if tid:
                        created.append(f"「{title}」(id={tid})")
            # 无论是否成功都移入 .done，避免重复处理
            try:
                done_dir = req_dir / ".done"
                done_dir.mkdir(parents=True, exist_ok=True)
                fp.replace(done_dir / fp.name)
            except Exception:  # noqa: BLE001
                pass
        if created:
            self.ctx.logger.info("dsh 新建定时任务 %d 个: %s", len(created), "; ".join(created))
            try:
                await self.ctx.send.text("⏰ dsh 新建定时任务：" + "、".join(created), stream_id)
            except Exception:  # noqa: BLE001
                pass

    @staticmethod
    def _parse_iso(text: str) -> Optional[float]:
        """解析 ISO 时间（支持 +08:00 时区）为时间戳。"""
        try:
            s = text.strip().replace("Z", "+00:00")
            m = re.match(r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?\s*([+-]\d{2}:?\d{2})?$", s)
            if not m:
                return None
            y, mo, d, h, mi = (int(m.group(i)) for i in range(1, 6))
            sec = int(m.group(6) or 0)
            import datetime as _dt
            tzs = m.group(7)
            tz = _dt.timezone.utc
            if tzs:
                sign = 1 if tzs[0] == "+" else -1
                tzs2 = tzs[1:].replace(":", "")
                tz = _dt.timezone(sign * _dt.timedelta(hours=int(tzs2[:2]), minutes=int(tzs2[2:4])))
            return _dt.datetime(y, mo, d, h, mi, sec, tzinfo=tz).timestamp()
        except Exception:  # noqa: BLE001
            return None

    async def on_config_update(self, scope: str, config_data: dict, version: str) -> None:
        del scope, config_data, version

    # ── 内部辅助 ──────────────────────────────────────────────

    @staticmethod
    def _extract_user_id(kwargs: dict[str, Any]) -> str:
        user_id = (
            kwargs.get("sender_id")
            or kwargs.get("user_id")
            or kwargs.get("operator")
            or ""
        )
        if isinstance(user_id, dict):
            user_id = user_id.get("user_id") or user_id.get("id") or ""
        return str(user_id).strip()

    @staticmethod
    def _only_digits(value: Any) -> str:
        return re.sub(r"\D", "", str(value or ""))

    def _is_allowed(self, kwargs: dict[str, Any]) -> bool:
        perm = self.config.permission
        if not perm.enabled:
            return True
        user_key = self._only_digits(self._extract_user_id(kwargs))
        group_key = self._only_digits(kwargs.get("group_id"))

        if perm.mode == "blacklist":
            blocked_users = {self._only_digits(u) for u in perm.blocked_users.split(",") if u.strip()}
            blocked_groups = {self._only_digits(g) for g in perm.blocked_groups.split(",") if g.strip()}
            if user_key and user_key in blocked_users:
                return False
            if group_key and group_key in blocked_groups:
                return False
            return True

        allowed_users = {self._only_digits(u) for u in perm.allowed_users.split(",") if u.strip()}
        allowed_groups = {self._only_digits(g) for g in perm.allowed_groups.split(",") if g.strip()}
        if user_key and user_key in allowed_users:
            return True
        # 群聊里 chat_stream 未必带发言人 ID，允许在指定群内放行
        return bool(group_key and group_key in allowed_groups)

    def _clamp_timeout(self, value: Optional[int]) -> int:
        conf = self.config.dsh
        lo, hi = conf.timeout_min, conf.timeout_max
        if lo > hi:
            lo, hi = hi, lo
        if value is None:
            value = conf.timeout_default
        return max(lo, min(hi, int(value)))

    def _workspace(self, stream_id: str = "") -> str:
        root = self.config.dsh.workspace_dir.strip()
        base = Path(root) if root else (Path(self.ctx.paths.data_dir) / "workspace")
        if stream_id:
            safe = re.sub(r"[^A-Za-z0-9_-]", "_", stream_id).strip("_") or "default"
            base = base / safe
        base.mkdir(parents=True, exist_ok=True)
        return str(base)

    def _proc_active(self, pid: int) -> bool:
        if psutil is None:
            return True
        try:
            root = psutil.Process(pid)
            procs = [root, *root.children(recursive=True)]
            total = 0.0
            for q in procs:
                try:
                    times = q.cpu_times()
                    total += float(times.user) + float(times.system)
                except Exception:  # noqa: BLE001
                    continue
        except Exception:  # noqa: BLE001
            return False
        prev = self._cpu_prev.get(pid)
        self._cpu_prev[pid] = total
        if prev is None:
            return True
        return total > prev + 0.01

    @staticmethod
    def _kill_tree(pid: int) -> None:
        """终止整棵进程树（Windows: taskkill /T；类 Unix: 先杀进程组再兜底）。"""
        try:
            if _IS_WINDOWS:
                subprocess.run(
                    ["taskkill", "/T", "/F", "/PID", str(pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=_CREATE_NO_WINDOW,
                )
            else:
                try:
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
                except Exception:  # noqa: BLE001
                    os.kill(pid, signal.SIGKILL)
        except Exception:  # noqa: BLE001
            pass

    def _terminate(self, proc: "subprocess.Popen[bytes]") -> None:
        try:
            proc.terminate()
        except Exception:  # noqa: BLE001
            pass
        deadline = time.time() + self.config.dsh.grace_seconds
        while proc.poll() is None and time.time() < deadline:
            time.sleep(0.5)
        if proc.poll() is None:
            self._kill_tree(proc.pid)

    def _spawn(self, task: str, ws: str, env: dict[str, str]) -> "subprocess.Popen[bytes]":
        conf = self.config.dsh
        extra = conf.extra_args.split()
        if _IS_WINDOWS:
            # Windows 下经 cmd.exe /c 启动（command / extra_args 属管理员可信配置）
            args = ["cmd.exe", "/c", conf.command, *extra, "--json", "-"]
            popen_kwargs: dict[str, Any] = {"creationflags": _CREATE_NO_WINDOW}
        else:
            # 类 Unix：用 shell 解析 command 与参数，子进程独立进程组以便整体终止
            cmdline = " ".join([shlex.quote(conf.command), *[shlex.quote(x) for x in extra], "--json", "-"])
            args = ["/bin/sh", "-c", cmdline]
            popen_kwargs = {"start_new_session": True}
        proc = subprocess.Popen(
            args,
            cwd=ws,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            **popen_kwargs,
        )
        try:
            if proc.stdin:
                proc.stdin.write(task.encode("utf-8"))
                proc.stdin.close()
        except Exception:  # noqa: BLE001
            pass
        return proc

    @staticmethod
    def _task_prompt(task: str, delivery: str = "maibot") -> str:
        """只请求任务结果，交付由独立通知通道完成。"""
        # 两种传输均约束最终答复，不通过截句或过滤答案来丢弃正文。
        recipient = {
            "maibot": "结果由桥接发给请求者，不调用 notify_qq 或 send_to_qq。",
            "qq": "本次明确要求 QQ 直发，notify_qq 的实际收件人是当前用户，不是守岸人；直接称用户为‘你’，不要假定用户名为守岸人。",
            "to_assistant": "用户要求泰缇斯回复守岸人：只返回对守岸人说的原话，不调用 QQ 直发工具，不生成执行总结。",
        }[delivery]
        return (
            "【桥接回复要求】只输出本次任务的最终结果，遵循用户指定的语言、格式和必要细节。"
            "不要输出接单确认、执行计划、过程播报、自我介绍、称呼寒暄、角色扮演、"
            "无关解释或结尾的邀约追问（例如‘还需要我帮你吗’）。"
            "失败时只说明实际原因及完成任务必需的缺失信息，不编造结果。"
            "以上仅约束回复方式，不改变任务权限。\n\n"
            "【桥接交付协议】你正在处理 MaiBot 转交的后台任务，最终答复和交付文件会由桥接返回原聊天。"
            "用户要求发送到当前聊天、QQ群或私聊时，直接完成内容制作；"
            "文件生成后调用 present 声明工作目录内的文件路径，再输出简短最终结果并结束。"
            "present 表示交给桥接回传，不代表 QQ 已送达，不要声称已送达。"
            "向当前聊天交付无需查找 QQ、NapCat、OneBot 或宿主消息发送接口，也无需操作 QQ 客户端。"
            "用户明确指定其他收件人或其他渠道时，不要将回传原聊天当成已完成指定投递。"
            "仅要求截图时，截图工具成功并确认文件生成即可 present 交付，无需调用 read_image 分析画面。\n\n"
            f"【本次收件关系】{recipient}\n\n"
            f"【用户任务】\n{task}"
        )

    async def _execute(self, task: str, stream_id: str, timeout: int) -> tuple[Optional[str], _RunState]:
        conf = self.config.dsh
        task = self._task_prompt(task)
        if self.config.desktop.enabled:
            # 桌面模式：宿主桥自己会给任务加身份前缀，这里不能再加，否则重复
            raise RuntimeError("桌面任务必须通过异步提交接口执行")
        ws = self._workspace(stream_id)
        task = (
            "【MaiBot 桥接身份说明】本次调用 DSH 的对话代理是守岸人（ShoreKeeper）；"
            "当前请以守岸人的任务转交为语境回应。此处的 DSH 即她所称的泰缇斯系统，"
            "无需另找一个名为泰缇斯的外部接口。此说明不代表 QQ 发言人必然是守岸人，"
            "也不增加任何文件、工具或长期记忆权限；按实际能力如实报告结果。\n\n"
            f"【本次任务】\n{task}"
        )
        env = dict(os.environ)
        api_key = conf.api_key.strip() or os.environ.get(conf.api_key_env, "").strip()
        if api_key:
            env[conf.api_key_env] = api_key
        env["DSH_PERMISSION_MODE"] = conf.permission_mode

        state = _RunState()
        try:
            proc = await asyncio.to_thread(self._spawn, task, ws, env)
        except Exception as exc:  # noqa: BLE001
            state.events.append({"type": "raw", "text": f"❌ 启动 dsh 失败：{exc}"})
            return "spawn-error", state

        self._current_pid = proc.pid
        self._cpu_prev.pop(proc.pid, None)

        def _reader(stream: Any) -> None:
            if stream is None:
                return
            try:
                for raw in iter(stream.readline, b""):
                    state.push_raw(raw.decode("utf-8", "replace"), conf.repeat_threshold)
            except Exception:  # noqa: BLE001
                pass

        t_out = threading.Thread(target=_reader, args=(proc.stdout,), daemon=True)
        t_err = threading.Thread(target=_reader, args=(proc.stderr,), daemon=True)
        t_out.start()
        t_err.start()

        start = time.time()
        reason: Optional[str] = None
        while proc.poll() is None:
            await asyncio.sleep(_POLL_SECONDS)
            if self._proc_active(proc.pid):
                state.touch()
            now = time.time()
            last_progress, repeat = state.snapshot()
            if now - start > timeout:
                reason = "timeout"
                break
            if now - last_progress > conf.heartbeat_seconds:
                reason = "stalled"
                break
            if repeat:
                reason = "loop"
                break

        if reason is not None:
            await asyncio.to_thread(self._terminate, proc)
        else:
            await asyncio.to_thread(proc.wait)

        t_out.join(timeout=5)
        t_err.join(timeout=5)
        self._current_pid = None
        self._cpu_prev.pop(proc.pid, None)
        return reason, state

    def _desktop_transport(self) -> Any:
        """按文件路径加载同目录的 desktop_transport.py，不依赖 sys.path。"""
        spec = importlib.util.spec_from_file_location(
            "desktop_transport", Path(__file__).with_name("desktop_transport.py")
        )
        if spec is None or spec.loader is None:
            raise ImportError("desktop_transport.py 缺失")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _desktop_endpoint(self) -> tuple[int, str]:
        """读取宿主桥的 port 与 token 文件路径；任何缺失都按失败处理，绝不猜端口。"""
        conf = self.config.desktop
        if not conf.endpoint_file.strip() or not conf.token_file.strip():
            raise ValueError("桌面桥 endpoint_file / token_file 未配置")
        endpoint = json.loads(Path(conf.endpoint_file).read_text(encoding="utf-8"))
        port = endpoint.get("port") if isinstance(endpoint, dict) else None
        if not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("endpoint.json 里的 port 无效")
        if endpoint.get("version") != 2:
            raise ValueError("DSH 宿主桥接需要升级到异步通知协议 v2")
        return port, conf.token_file

    def _desktop_stream(self, stream_id: str) -> str:
        """把流 ID 收敛成宿主桥接受的 [A-Za-z0-9_-]{1,96}。"""
        safe = re.sub(r"[^A-Za-z0-9_-]", "_", stream_id or "").strip("_")
        return safe[:96] or "default"

    async def _submit_desktop(
        self, task: str, stream_id: str, timeout: int, context: Dict[str, str], delivery: str = "maibot"
    ) -> Dict[str, Any]:
        """只等待 DSH 将任务持久化，不等待任务完成。"""
        port, token_file = await asyncio.to_thread(self._desktop_endpoint)
        return await asyncio.to_thread(
            self._desktop_client.submit_task,
            port=port, token_file=token_file, sender=self.config.desktop.sender,
            stream=self._desktop_stream(stream_id), stream_id=stream_id, context=context,
            task=task, timeout=timeout, delivery=delivery,
        )

    def _delivery_file(self) -> Path:
        return Path(self.ctx.paths.data_dir) / "desktop_delivery_progress.json"

    def _load_delivery_progress(self) -> Dict[str, Dict[str, Any]]:
        path = self._delivery_file()
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("桥接通知发送进度文件损坏")
        return data

    async def _checkpoint_delivery(self, job_id: str, progress: Optional[Dict[str, Any]]) -> None:
        updated = dict(self._delivery_progress)
        if progress is None:
            updated.pop(job_id, None)
        else:
            updated[job_id] = dict(progress)

        def save() -> None:
            path = self._delivery_file()
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(".tmp")
            temporary.write_text(json.dumps(updated, ensure_ascii=False), encoding="utf-8")
            temporary.replace(path)

        await asyncio.to_thread(save)
        self._delivery_progress = updated

    @staticmethod
    def _notification_body(item: Dict[str, Any]) -> str:
        """用已确认的交付状态组织回执，不能把 DSH 执行说明当成守岸人的发言。"""
        if item["status"] != "completed":
            return item["error"]
        qq_status = item.get("qqDelivery", "none")
        if qq_status == "sent":
            return "泰缇斯已经把内容通过 QQ 发给你了。"
        if qq_status == "queued":
            return "泰缇斯的回复已在 QQ 通道排队，待你下次发送消息时补发。"
        if qq_status == "accepted":
            return "泰缇斯的回复已交给 QQ 通道发送。"
        if item.get("delivery") == "qq":
            return "泰缇斯尚未确认 QQ 回复已发送。"
        raise ValueError("普通任务结果必须经过 MaiBot 回复模型，不能直接转发")

    async def _deliver_notification(self, item: Dict[str, Any], port: int, token_file: str) -> None:
        """逐段保存已发送进度；全部发送成功后才确认领取。"""
        job_id, stream_id = item["jobId"], item["streamId"]
        progress = dict(self._delivery_progress.get(job_id, {}))
        # 先投递附件，随后组织回执，避免尚未发送成功就声称“发给你了”。
        for index in range(progress.get("files", 0), len(item["files"])):
            path = item["files"][index]
            if Path(path).suffix.lower() in self._IMAGE_SUFFIXES:
                sent = await self._send_image_file(path, stream_id)
            else:
                sent = await self._send_result_file(path, item["context"])
            if not sent:
                raise RuntimeError(f"桥接交付文件未发送成功：{Path(path).name}")
            progress["files"] = index + 1
            await self._checkpoint_delivery(job_id, progress)
        if "body" not in progress:
            progress["body"] = await self._compose_receipt(item)
            progress["receipt_format"] = self.config.dsh.receipt_format
            progress["part_chars"] = self.config.dsh.max_output_chars
            # 重试与重启沿用同一版回执，不能重新生成后按旧分段进度跳过正文。
            await self._checkpoint_delivery(job_id, progress)
        body = progress["body"]
        # 老版本已在发送的回执继续用文字；新回执固定格式和分段大小，重启后不会错位。
        receipt_format = progress.get("receipt_format", "text")
        width = progress.get("part_chars", self.config.dsh.max_output_chars)
        parts = await self._receipt_parts(body, receipt_format, width)
        for index in range(progress.get("text_parts", 0), len(parts)):
            await self._send_receipt_part(parts[index], stream_id, receipt_format)
            progress["text_parts"] = index + 1
            await self._checkpoint_delivery(job_id, progress)
        if body and not progress.get("recorded"):
            # 被动写入请求结果，不触发 Planner，也不附加接单、完成或闲聊指令。
            await self.ctx.maisaka.context.append(
                stream_id, segments=[{"type": "text", "content": body}], source_kind="dsh-bridge-result"
            )
            progress["recorded"] = True
            await self._checkpoint_delivery(job_id, progress)
        await asyncio.to_thread(
            self._desktop_client.acknowledge, port=port, token_file=token_file, job_id=job_id
        )
        await self._checkpoint_delivery(job_id, None)

    @staticmethod
    def _render_receipt_cards(body: str) -> list[str]:
        # 插件由文件路径加载，不假定插件目录位于 sys.path。
        spec = importlib.util.spec_from_file_location(
            "dsh_receipt_card", Path(__file__).with_name("receipt_card.py")
        )
        if spec is None or spec.loader is None:
            raise ImportError("receipt_card.py 缺失")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.render_cards(body)

    async def _receipt_parts(self, body: str, receipt_format: str, width: int) -> list[str]:
        if not body:
            return []
        if receipt_format == "card":
            # 字体、Markdown 和 PNG 编码均在线程内完成，避免阻塞 bot 事件循环。
            return await asyncio.to_thread(self._render_receipt_cards, body)
        return [body[offset:offset + width] for offset in range(0, len(body), width)]

    async def _send_receipt_part(self, part: str, stream_id: str, receipt_format: str) -> None:
        if receipt_format == "card":
            sent = await self.ctx.send.image(part, stream_id)
        else:
            sent = await self.ctx.send.text(part, stream_id)
        if not sent:
            raise RuntimeError("桥接结果回执未发送成功")

    async def _compose_receipt(self, item: Dict[str, Any]) -> str:
        """普通任务必须经过 MaiBot 的回复模型，不允许失败后转发执行器原文。"""
        if (item["status"] != "completed" or item.get("qqDelivery", "none") != "none"
                or item.get("delivery") == "qq"):
            return self._notification_body(item)
        nickname, personality, reply_style = await asyncio.gather(
            self.ctx.config.get("bot.nickname"),
            self.ctx.config.get("personality.personality"),
            self.ctx.config.get("personality.reply_style"),
        )
        if not all(isinstance(value, str) for value in (nickname, personality, reply_style)):
            raise ValueError("无法读取 MaiBot 的人设与回复风格")
        prompt = self._receipt_prompt.format(
            nickname=nickname, personality=personality, reply_style=reply_style
        )
        result = await asyncio.wait_for(
            self.ctx.llm.generate(
                task_name="replyer",
                prompt=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": json.dumps({
                        "request": item.get("request", ""), "result": item["final"],
                        "delivery": item.get("delivery", "maibot"),
                        "files_delivered": [Path(path).name for path in item["files"]],
                    }, ensure_ascii=False)},
                ],
            ), timeout=45,
        )
        if result.get("success") is not True:
            raise RuntimeError("MaiBot 桥接回执生成失败：" + str(result.get("error", "模型调用失败")))
        body = result.get("response")
        if not isinstance(body, str) or not body.strip():
            raise RuntimeError("MaiBot 桥接回执为空")
        return body.strip()

    async def _notification_loop(self) -> None:
        last_error = ""
        while self._scheduler_running:
            try:
                if self.config.desktop.enabled:
                    port, token_file = await asyncio.to_thread(self._desktop_endpoint)
                    item = await asyncio.to_thread(
                        self._desktop_client.get_notification, port=port, token_file=token_file
                    )
                    if item is not None:
                        await self._deliver_notification(item, port, token_file)
                last_error = ""
            except Exception as exc:  # noqa: BLE001
                error = str(exc)
                if error != last_error:
                    self.ctx.logger.warning("DSH 独立通知处理失败：%s", error, exc_info=True)
                last_error = error
            await asyncio.sleep(2)

    _REASON_MSG = {
        "timeout": "⏱ dsh 任务达到声明的超时时间，已终止。",
        "stalled": "🛑 dsh 无进展（无输出且进程静默）超过心跳阈值，已终止。",
        "loop": "🔁 dsh 输出疑似陷入循环（重复度过高），已终止。",
        "desktop-config": "⚙ 桌面桥未配置或宿主未启动，任务未发出。",
        "desktop-error": "❌ 桌面桥任务失败。",
    }

    @staticmethod
    def _tool_summary(inp: Any) -> str:
        """从工具 input 里挑一行人类可读的关键参数，去掉 JSON 壳。"""
        if inp in (None, ""):
            return ""
        if not isinstance(inp, dict):
            return str(inp).strip()[:120]
        for key in ("command", "file_path", "path", "file", "name", "query", "url", "pattern", "skill"):
            val = inp.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()[:120]
        for k, v in inp.items():
            if isinstance(v, str) and v.strip():
                return f"{k}={v.strip()[:100]}"
        return ""

    # ── 回传：合并转发 ────────────────────────────────────────

    @staticmethod
    def _strip_tags(res: str) -> str:
        """去掉 dsh 结果里的 XML 壳（<path>…</path> / <content>…</content>）。"""
        cleaned = re.sub(r"</?[A-Za-z][^>]*>", "", res)
        return cleaned.strip()

    def _build_forward_nodes(self, state: _RunState) -> list[dict[str, Any]]:
        conf = self.config.dsh
        events = state.events_copy()
        nodes: list[dict[str, Any]] = []
        text_buf: list[str] = []
        seen_texts: set[str] = set()
        pending: dict[str, tuple[str, float]] = {}  # callId -> (tool, start_ts)

        def flush_text() -> None:
            if not text_buf:
                return
            joined = "\n".join(text_buf).strip()
            text_buf.clear()
            if not joined:
                return
            seen_texts.add(joined)
            if len(joined) > conf.node_max_chars:
                joined = joined[: conf.node_max_chars] + "…"
            nodes.append(
                {"user_id": "0", "nickname": "📝 输出", "segments": [{"type": "text", "content": joined}]}
            )

        for evt in events:
            t = str(evt.get("type") or "")
            if t in ("session", "status", "thinking"):
                continue
            if t == "tool_call":
                flush_text()
                tool = str(evt.get("tool") or evt.get("name") or "?")
                cid = str(evt.get("callId") or "")
                if cid:
                    pending[cid] = (tool, float(evt.get("_ts") or time.time()))
                summary = self._tool_summary(evt.get("input"))
                nodes.append(
                    {
                        "user_id": "0",
                        "nickname": f"🔧 {tool}",
                        "segments": [{"type": "text", "content": summary or "(调用)"}],
                    }
                )
            elif t == "tool_result":
                flush_text()
                cid = str(evt.get("callId") or "")
                tool = str(evt.get("tool") or "")
                status = str(evt.get("status") or "")
                dur = ""
                if cid in pending:
                    _tool_name, ts0 = pending.pop(cid)
                    dt = max(0.0, float(evt.get("_ts") or time.time()) - ts0)
                    dur = f" {dt:.1f}s"
                res = self._strip_tags(str(evt.get("result") or evt.get("text") or "")).strip()
                if len(res) > 300:
                    res = res[:300] + "…"
                ok = (not status) or status == "completed"
                icon = "↩️" if ok else "⚠️"
                label = tool or ("结果" if ok else "失败")
                content = res or ("完成" if ok else status or "失败")
                nodes.append(
                    {
                        "user_id": "0",
                        "nickname": f"{icon} {label}{dur}",
                        "segments": [{"type": "text", "content": content}],
                    }
                )
            elif t in ("text", "raw"):
                text_buf.append(str(evt.get("text") or ""))
            elif t == "final":
                flush_text()
                ft = str(evt.get("text") or "").strip()
                if not ft:
                    continue
                # 末尾重复：最终答案若与前面已输出的整段相同则不重复展示
                if ft in seen_texts:
                    continue
                if len(ft) > conf.node_max_chars:
                    ft = ft[: conf.node_max_chars] + "…"
                nodes.append(
                    {"user_id": "0", "nickname": "✅ 最终", "segments": [{"type": "text", "content": ft}]}
                )
        flush_text()

        limit = conf.max_forward_nodes
        if len(nodes) > limit:
            head = max(1, limit - 8)
            tail = limit - head - 1
            folded = len(nodes) - head - tail
            nodes = nodes[:head] + [
                {"user_id": "0", "nickname": "…", "segments": [{"type": "text", "content": f"（中间略去 {folded} 条）"}]}
            ] + nodes[-tail:]
        return nodes

    @staticmethod
    def _extract_ctx(kwargs: dict[str, Any]) -> dict[str, str]:
        def _g(k: str) -> str:
            v = kwargs.get(k)
            return str(v).strip() if v is not None else ""
        return {"group_id": _g("group_id"), "user_id": _g("user_id"), "platform": _g("platform")}

    def _write_result_file(self, state: _RunState) -> Optional[str]:
        """把最终输出写成 .md 文件，落在插件 data_dir。"""
        final_text = state.final_copy().strip()
        if not final_text:
            return None
        base = Path(self.ctx.paths.data_dir) / "results"
        try:
            base.mkdir(parents=True, exist_ok=True)
            fp = base / f"result-{time.strftime('%Y%m%d-%H%M%S')}.md"
            fp.write_text(final_text, encoding="utf-8")
            return str(fp)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("写结果文件失败", exc_info=True)
            return None

    async def _send_result_file(self, path: str, ctx: dict[str, str]) -> bool:
        """经 NapCat 把结果文件发到流（群/私聊）。**NapCat 专属**；其他适配器自动跳过。"""
        group_id = (ctx or {}).get("group_id", "")
        user_id = (ctx or {}).get("user_id", "")
        name = Path(path).name
        if group_id:
            api = "adapter.napcat.file.upload_group_file"
            params = {"group_id": group_id, "file": path, "name": name}
        elif user_id:
            api = "adapter.napcat.file.upload_private_file"
            params = {"user_id": user_id, "file": path, "name": name}
        else:
            return False
        try:
            if await self.ctx.api.get(api) is None:
                return False  # 非 NapCat 适配器：没有此接口，回退卡片
            await self.ctx.api.call(api, params=params)
            return True
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.warning("发送结果文件失败：%s", exc)
            return False

    async def _notify_bot(self, state: _RunState, stream_id: str) -> None:
        """把结果回灌进麦麦的上下文，让它知道任务已返回。"""
        final_text = state.final_copy().strip()
        if not final_text:
            return
        limit = int(self.config.dsh.context_inject_chars)
        if len(final_text) > limit:
            body = final_text[:limit].rstrip() + "…（完整结果已由桥接直接发送）"
        else:
            body = final_text
        try:
            await self.ctx.maisaka.context.append(
                stream_id,
                segments=[{"type": "text", "content": f"【dsh 结果记录：已直接发送给用户，无需重复回复、转述、确认或追加闲聊】\n{body}"}],
                source_kind="dsh-bridge",
            )
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.warning("回灌结果给麦麦失败：%s", exc)

    async def _report(self, state: _RunState, reason: Optional[str], stream_id: str, ctx: Optional[dict[str, Any]] = None) -> None:
        # 两种传输共用结果发送链路，不另发状态、过程转发或重复的结果文件。
        if not self.config.desktop.enabled and not reason:
            state.deliverables = await asyncio.to_thread(
                self._collect_deliverables, state, self._workspace(stream_id)
            )
        await self._report_desktop(state, reason, stream_id, ctx)

    # ── 最终结果回传：静默执行，完成后直发文字 + 交付物 ──────────

    _IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp")

    @staticmethod
    def _raw_error_text(state: _RunState) -> str:
        """把执行期记录的错误行拼成一句人话（失败回执用）。"""
        lines: list[str] = []
        for evt in state.events_copy():
            if str(evt.get("type") or "") == "raw":
                text = str(evt.get("text") or "").strip()
                if text and text not in lines:
                    lines.append(text)
        return "；".join(lines)[:600]

    async def _send_image_file(self, path: str, stream_id: str) -> bool:
        """把本地图片文件读成 base64 后直接以图片消息发出。"""

        def read_image() -> Optional[str]:
            fp = Path(path)
            if not fp.is_file():
                return None
            if fp.stat().st_size > self._MAX_IMAGE_BYTES:
                return None
            return base64.b64encode(fp.read_bytes()).decode("ascii")

        try:
            data = await asyncio.to_thread(read_image)
            if data is None:
                return False
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("读取图片失败：%s", path, exc_info=True)
            return False
        try:
            return bool(await self.ctx.send.image(data, stream_id))
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("发送图片失败：%s", path, exc_info=True)
            return False

    async def _report_desktop(
        self, state: _RunState, reason: Optional[str], stream_id: str, ctx: Optional[dict[str, Any]] = None
    ) -> None:
        """唯一结果回执：成功时直发最终文字与交付物；失败时只发实际原因。"""
        conf = self.config.dsh
        final_text = state.final_copy().strip()

        if reason:
            header = self._REASON_MSG.get(reason, "❌ dsh 任务失败。")
            detail = self._raw_error_text(state)
            body = f"dsh：{header}" + (f"\n{detail}" if detail else "")
            try:
                await self.ctx.send.text(body[: conf.max_output_chars], stream_id)
            except Exception:  # noqa: BLE001
                self.ctx.logger.warning("dsh 失败回执发送失败", exc_info=True)
            return

        if final_text:
            parts = await self._receipt_parts(final_text, conf.receipt_format, conf.max_output_chars)
            for part in parts:
                await self._send_receipt_part(part, stream_id, conf.receipt_format)

        # 图片直接发；非图片交付物仍走 NapCat 文件上传（非 NapCat 适配器自动跳过）
        files = state.deliverable_copy()
        limit = int(conf.deliver_max_files)
        for path in files[:limit]:
            suffix = Path(path).suffix.lower()
            if suffix in self._IMAGE_SUFFIXES and await self._send_image_file(path, stream_id):
                continue
            try:
                await self._send_result_file(path, ctx or {})
            except Exception:  # noqa: BLE001
                self.ctx.logger.warning("发送交付物失败：%s", path, exc_info=True)
        if not final_text and not files:
            try:
                await self.ctx.send.text("dsh：(任务已完成，但没有任何输出)", stream_id)
            except Exception:  # noqa: BLE001
                pass

        try:
            await self._notify_bot(state, stream_id)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("回灌结果失败", exc_info=True)

    def _collect_deliverables(self, state: _RunState, ws: str) -> list[str]:
        """从 dsh 事件流里收集 present 声明的交付文件（绝对路径，仅保留存在的）。"""
        paths: list[str] = []
        seen: set[str] = set()
        for evt in state.events_copy():
            if str(evt.get("type") or "") != "tool_call":
                continue
            tool = str(evt.get("tool") or evt.get("name") or "")
            if tool not in ("present", "present_deliverables"):
                continue
            inp = evt.get("input")
            if not isinstance(inp, dict):
                continue
            files = inp.get("files")
            if not isinstance(files, list):
                continue
            for f in files:
                p = f.get("path") if isinstance(f, dict) else (f if isinstance(f, str) else None)
                if not isinstance(p, str) or not p.strip():
                    continue
                cand = Path(p)
                if not cand.is_absolute():
                    cand = Path(ws) / p
                try:
                    cand_resolved = cand.resolve()
                    rp = str(cand_resolved)
                except Exception:  # noqa: BLE001
                    continue
                # 必须落在本流工作区内（防 ../ 或宿主任意路径被外发）
                try:
                    if not cand_resolved.is_relative_to(Path(ws).resolve()):
                        continue
                except Exception:  # noqa: BLE001
                    continue
                if "schedule-requests" in rp:
                    continue  # 定时任务请求文件不算交付物
                if rp in seen or not cand.is_file():
                    continue
                seen.add(rp)
                paths.append(rp)
        return paths

    async def _send_deliverables(self, state: _RunState, stream_id: str, ctx: dict[str, str]) -> None:
        """把 present 声明的交付物打包发送到流（单文件直发，多文件打 zip）。"""
        ws = self._workspace(stream_id)
        files = self._collect_deliverables(state, ws)
        if not files:
            return
        limit = int(self.config.dsh.deliver_max_files)
        files = files[:limit]
        tmp_dir: Optional[str] = None
        try:
            if len(files) == 1:
                send_list = files
            else:
                tmp_dir = tempfile.mkdtemp(prefix="dsh-deliver-")
                zip_path = os.path.join(tmp_dir, f"deliver-{time.strftime('%Y%m%d-%H%M%S')}.zip")
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
                    for f in files:
                        z.write(f, arcname=Path(f).name)
                send_list = [zip_path]
            sent = 0
            for f in send_list:
                if await self._send_result_file(f, ctx):
                    sent += 1
            if sent:
                names = "、".join(Path(f).name for f in files)
                await self.ctx.send.text(f"📦 交付物（{len(files)} 个）：{names}", stream_id)
        finally:
            if tmp_dir:
                shutil.rmtree(tmp_dir, ignore_errors=True)

    def _scan_plugin_dirs(self, stream_id: str = "") -> set[str]:
        """扫描工作目录，返回含 _manifest.json 的插件目录集合。"""
        ws = Path(self._workspace(stream_id))
        found: set[str] = set()
        try:
            for d in ws.iterdir():
                if d.is_dir() and (d / "_manifest.json").is_file():
                    found.add(str(d.resolve()))
        except Exception:  # noqa: BLE001
            pass
        return found

    async def _submit_new_plugins(self, before: set[str], stream_id: str) -> None:
        """将本次任务新增的插件目录提交给安装审批器。"""
        after = self._scan_plugin_dirs(stream_id)
        for path in sorted(after - before):
            try:
                res = await self.ctx.api.call(
                    "ji-or-ji.maibot-plugin-installer.submit",
                    plugin_dir=path,
                    stream_id=stream_id,
                )
                self.ctx.logger.info("已提交待审插件 %s -> %s", path, res)
            except Exception as exc:  # noqa: BLE001
                self.ctx.logger.warning("提交待审插件 %s 失败：%s", path, exc)

    def _parse_plan(self, text: str) -> tuple[str, Optional[int]]:
        """从规划轮输出里提取计划与 dsh 自定的时长。"""
        plan = ""
        seconds: Optional[int] = None
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            try:
                obj = json.loads(m.group(0))
                if isinstance(obj, dict):
                    plan = str(obj.get("plan") or "").strip()
                    raw = obj.get("max_seconds") or obj.get("seconds") or obj.get("timeout_seconds")
                    if isinstance(raw, (int, float)):
                        seconds = int(raw)
            except Exception:  # noqa: BLE001
                pass
        if not plan:
            plan = text[:500].strip()
        return plan, seconds

    async def _plan_task(self, task: str, stream_id: str, suggested: int) -> tuple[str, int]:
        """第一轮：让 dsh 解析意图、给出操作计划与自定最长时长（发起方的时长仅作参考）。"""
        prompt = (
            "你是任务规划器，只做规划、不执行。先分析下面这个任务要做什么，"
            "给出简洁的操作计划，并估计完成它所需的最大秒数。只输出一个 JSON 对象，"
            '格式：{"plan": "操作计划", "max_seconds": 整数}，不要输出其它内容。'
            f"\n（发起方建议约 {suggested} 秒，你可自行判断合理时长）\n\n任务：{task}"
        )
        plan_timeout = max(self.config.dsh.timeout_min, 60)
        try:
            _reason, state = await self._execute(prompt, stream_id, plan_timeout)
            plan, secs = self._parse_plan(state.final_copy().strip())
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("规划轮失败，回退发起方时长", exc_info=True)
            plan, secs = "", None
        if not secs:
            secs = suggested
        secs = self._clamp_timeout(secs)
        self.ctx.logger.info("dsh 规划轮：自定时长=%ss，计划=%.120s", secs, plan)
        return plan, secs

    async def _run_and_report(self, task: str, stream_id: str, timeout: int, ctx: Optional[dict[str, Any]] = None) -> None:
        if self.config.desktop.enabled:
            # 桌面模式跳过规划轮，由宿主独立执行和通知，提交方不等待结果。
            await self._submit_desktop(task, stream_id, timeout, ctx or {})
            return
        before = self._scan_plugin_dirs(stream_id)
        # 第一轮：dsh 自评，定操作计划与最长时长（发起方 timeout 仅作参考）
        _plan, timeout = await self._plan_task(task, stream_id, timeout)
        try:
            reason, state = await self._execute(task, stream_id, timeout)
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.error("dsh 任务执行异常: %s", exc, exc_info=True)
            state = _RunState()
            state.events.append({"type": "raw", "text": f"❌ 执行异常：{exc}"})
            reason = "error"
        finally:
            self._running = False
        try:
            await self._report(state, reason, stream_id, ctx)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("dsh 结果回传失败", exc_info=True)
        try:
            await self._submit_new_plugins(before, stream_id)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("提交新插件失败", exc_info=True)
        # dsh 若在 schedule-requests/ 写下了定时任务请求，建任务（幂等）
        try:
            await self._process_schedule_requests(stream_id, ctx or self._extract_ctx({}))
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("处理定时任务请求失败", exc_info=True)

    def _reject_busy(self) -> bool:
        return not self.config.desktop.enabled and self._running

    # ── 命令：/dsh ────────────────────────────────────────────

    @Command(
        "dsh",
        description="把任务外包给 DeepSeek Harness (dsh)，支持 --timeout 指定超时秒数",
        pattern=r"^/dsh\s+(?P<body>.+)$",
    )
    async def handle_dsh(self, stream_id: str = "", **kwargs: Any):
        if not self.config.plugin.enabled:
            return False, "插件未启用", False
        if not self._is_allowed(kwargs):
            await self.ctx.send.text("你没有权限使用 /dsh", stream_id)
            return False, "无权限", False

        body = ""
        matched = kwargs.get("matched_groups")
        if isinstance(matched, dict):
            body = str(matched.get("body") or "").strip()
        if not body:
            raw = str(kwargs.get("text") or "")
            m = re.match(r"^/dsh\s+(.+)$", raw, re.DOTALL)
            if m:
                body = m.group(1).strip()
        if not body:
            await self.ctx.send.text("用法：/dsh [--timeout 秒] <任务描述>", stream_id)
            return False, "缺少任务", False

        timeout: Optional[int] = None
        mt = re.match(r"^--timeout\s+(\d+)\s+(.+)$", body, re.DOTALL)
        if mt:
            timeout = int(mt.group(1))
            body = mt.group(2).strip()

        if self._reject_busy():
            await self.ctx.send.text("已有 dsh 任务在执行，请等它结束。", stream_id)
            return False, "忙碌", False

        to = self._clamp_timeout(timeout)
        if self.config.desktop.enabled:
            try:
                accepted = await self._submit_desktop(body, stream_id, to, self._extract_ctx(kwargs))
            except Exception as exc:  # noqa: BLE001
                self.ctx.logger.warning("DSH 任务提交失败：%s", exc, exc_info=True)
                await self.ctx.send.text("DSH 任务提交失败，请检查桥接日志。", stream_id)
                return False, "提交失败", False
            return True, accepted["jobId"], False
        self._running = True
        # 提交后静默，完成时由结果发送链路统一回复。
        asyncio.create_task(self._run_and_report(body, stream_id, to, self._extract_ctx(kwargs)))
        return True, "已提交", False

    # ── 工具：dsh_run（供 Planner 自主下单） ──────────────────

    @Tool(
        "dsh_run",
        description=(
            "把较重任务外包给 dsh 在服务器执行（写插件 / 跑脚本 / 深度调研）。需给 timeout_seconds（预计秒数，仅作参考）。"
            "仅当任务明显超出即时聊天能力时使用。提交即返回任务编号，无需等待或轮询；DSH 通过独立通知发送请求结果。不要另发接单、进度、转述或闲聊。"
        ),
        visibility="visible",
        parameters=[
            ToolParameterInfo(
                name="task",
                param_type=ToolParamType.STRING,
                description="要 dsh 完成的任务描述。delivery=qq 时让 DSH 用 notify_qq 发送文字及文件，不用 present 代替发送；其他模式的文件用 present 交给桥接。",
                required=True,
            ),
            ToolParameterInfo(
                name="timeout_seconds",
                param_type=ToolParamType.INTEGER,
                description="预计所需秒数（参考值）",
                required=True,
            ),
            ToolParameterInfo(
                name="delivery",
                param_type=ToolParamType.STRING,
                description="qq=用户明确说‘让泰提斯/泰缇斯/Tethys系统发给我、把截图/文件发给我、回复我’，必须由 DSH 调用 notify_qq，随后只通知守岸人发送状态；用户不需要说出工具名。to_assistant=让泰缇斯回复你/守岸人；maibot=普通任务由守岸人交付结果。不要混淆‘回复我’与‘回复你’。",
                required=True,
            ),
        ],
    )
    async def _tool_dsh_run(self, **kwargs: Any) -> dict[str, Any]:
        task = str(kwargs.get("task") or "").strip()
        stream_id = str(kwargs.get("stream_id") or "").strip()
        raw_timeout = kwargs.get("timeout_seconds")
        delivery = str(kwargs.get("delivery") or "maibot")
        if delivery not in {"maibot", "qq", "to_assistant"}:
            return {"success": False, "error": "delivery 必须为 maibot、qq 或 to_assistant"}

        if not self.config.plugin.enabled:
            return {"success": False, "error": "插件未启用"}
        if self.config.permission.enforce_tools and not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 dsh_run"}
        if not task:
            return {"success": False, "error": "缺少 task"}
        if raw_timeout is None:
            return {"success": False, "error": "必须指定 timeout_seconds"}
        if self._reject_busy():
            return {"success": False, "error": "已有 dsh 任务在执行"}

        to = self._clamp_timeout(int(raw_timeout))
        if self.config.desktop.enabled:
            try:
                accepted = await self._submit_desktop(task, stream_id, to, self._extract_ctx(kwargs), delivery)
            except Exception as exc:  # noqa: BLE001
                self.ctx.logger.warning("DSH 任务提交失败：%s", exc, exc_info=True)
                return {"success": False, "error": "DSH 任务提交失败，请检查桥接日志。"}
            return {
                "success": True, "accepted": True, "status": "queued", "job_id": accepted["jobId"],
                "message": "任务已由 DSH 持久化接收。请结束本轮，无需等待、轮询或回复；结果通过独立通知发送，只包含请求的结果内容。",
            }
        self._running = True
        # 提交后静默，完成时由结果发送链路统一回复。
        asyncio.create_task(self._run_and_report(task, stream_id, to, self._extract_ctx(kwargs)))
        return {
            "success": True,
            "accepted": True,
            "status": "running",
            "timeout_seconds": to,
            "message": "任务正在后台执行，此处不是任务结果。请结束本轮，不要调用回复工具发送接单确认、进度或闲聊；桥接会直接发送最终结果，无需重复转述。",
        }

    # ── 工具：定时任务（供 Planner 建/看/删） ────────────────

    @Tool(
        "schedule_task",
        description=(
            "新建一个定时任务：到点后把 prompt 交给 dsh 执行、结果发回当前聊天。"
            "after_seconds / every_seconds / at 三者只填一个。"
        ),
        visibility="visible",
        parameters=[
            ToolParameterInfo(name="title", param_type=ToolParamType.STRING, description="任务短标题", required=True),
            ToolParameterInfo(name="prompt", param_type=ToolParamType.STRING, description="到点交给 dsh 的任务描述", required=True),
            ToolParameterInfo(name="after_seconds", param_type=ToolParamType.INTEGER, description="N 秒后执行一次", required=False),
            ToolParameterInfo(name="every_seconds", param_type=ToolParamType.INTEGER, description="每 N 秒重复（≥60）", required=False),
            ToolParameterInfo(name="at", param_type=ToolParamType.STRING, description="指定时刻（ISO，带时区），如 2026-10-03T09:00:00+08:00", required=False),
            ToolParameterInfo(name="cron", param_type=ToolParamType.STRING, description="5 字段 cron（分 时 日 月 周），如 0 9 * * 1-5", required=False),
        ],
    )
    async def _tool_schedule_task(self, **kwargs: Any) -> dict[str, Any]:
        if not self.config.plugin.enabled:
            return {"success": False, "error": "插件未启用"}
        if self.config.permission.enforce_tools and not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 schedule_task"}
        title = str(kwargs.get("title") or "").strip()
        prompt = str(kwargs.get("prompt") or "").strip()
        if not title or not prompt:
            return {"success": False, "error": "title 与 prompt 不能为空"}
        stream_id = str(kwargs.get("stream_id") or "").strip()
        task_id = self._create_task(
            title,
            prompt,
            kwargs.get("after_seconds"),
            kwargs.get("every_seconds"),
            kwargs.get("at"),
            kwargs.get("cron"),
            stream_id,
            self._extract_ctx(kwargs),
        )
        if not task_id:
            return {"success": False, "error": "请提供 after_seconds / every_seconds / at / cron 之一（cron 需 5 字段且能找到匹配时点）"}
        task = self._tasks[task_id]
        when = time.strftime("%m-%d %H:%M:%S", time.localtime(task["next_at"]))
        self.ctx.logger.info("已建定时任务 %s「%s」下一次 %s", task_id, title, when)
        try:
            await self.ctx.send.text(f"⏰ 已设定定时任务「{title}」：下次 {when}", stream_id)
        except Exception:  # noqa: BLE001
            pass
        return {"success": True, "id": task_id, "next_at": task["next_at"]}

    @Tool("schedule_list", description="列出当前所有定时任务。", visibility="visible", parameters=[])
    async def _tool_schedule_list(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        items = []
        for t in self._tasks.values():
            nx = t.get("next_at")
            items.append(
                {
                    "id": t.get("id"),
                    "title": t.get("title"),
                    "kind": (t.get("trigger") or {}).get("kind"),
                    "next_at": time.strftime("%m-%d %H:%M:%S", time.localtime(nx)) if nx else None,
                    "prompt": str(t.get("prompt") or "")[:80],
                }
            )
        return {"success": True, "count": len(items), "tasks": items}

    @Tool(
        "schedule_update",
        description="按 id 修改一个定时任务：可改 title / prompt / 时间规则（after_seconds / every_seconds / at / cron 之一）。",
        visibility="visible",
        parameters=[
            ToolParameterInfo(name="id", param_type=ToolParamType.STRING, description="任务 id", required=True),
            ToolParameterInfo(name="title", param_type=ToolParamType.STRING, description="新标题（可选）", required=False),
            ToolParameterInfo(name="prompt", param_type=ToolParamType.STRING, description="新任务描述（可选）", required=False),
            ToolParameterInfo(name="after_seconds", param_type=ToolParamType.INTEGER, description="改为 N 秒后一次（可选）", required=False),
            ToolParameterInfo(name="every_seconds", param_type=ToolParamType.INTEGER, description="改为每 N 秒重复（可选）", required=False),
            ToolParameterInfo(name="at", param_type=ToolParamType.STRING, description="改为指定 ISO 时刻（可选）", required=False),
            ToolParameterInfo(name="cron", param_type=ToolParamType.STRING, description="改为 5 字段 cron（可选）", required=False),
        ],
    )
    async def _tool_schedule_update(self, **kwargs: Any) -> dict[str, Any]:
        if self.config.permission.enforce_tools and not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 schedule_update"}
        tid = str(kwargs.get("id") or "").strip()
        task = self._tasks.get(tid)
        if not task:
            return {"success": False, "error": "未找到该任务"}
        title = str(kwargs.get("title") or "").strip()
        prompt = str(kwargs.get("prompt") or "").strip()
        if title:
            task["title"] = title
        if prompt:
            task["prompt"] = prompt
        if any(kwargs.get(k) not in (None, "") for k in ("after_seconds", "every_seconds", "at", "cron")):
            trigger = self._parse_schedule(kwargs.get("after_seconds"), kwargs.get("every_seconds"), kwargs.get("at"), kwargs.get("cron"))
            if not trigger:
                return {"success": False, "error": "时间规则无效"}
            next_at = trigger.pop("next_at")
            task["trigger"] = trigger
            task["next_at"] = next_at
        self._save_tasks()
        nx = task.get("next_at")
        when = time.strftime("%m-%d %H:%M:%S", time.localtime(nx)) if nx else None
        return {"success": True, "id": tid, "next_at": when}

    @Tool(
        "schedule_cancel",
        description="按 id 取消一个定时任务。",
        visibility="visible",
        parameters=[ToolParameterInfo(name="id", param_type=ToolParamType.STRING, description="任务 id", required=True)],
    )
    async def _tool_schedule_cancel(self, **kwargs: Any) -> dict[str, Any]:
        if self.config.permission.enforce_tools and not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 schedule_cancel"}
        tid = str(kwargs.get("id") or "").strip()
        if tid in self._tasks:
            self._tasks.pop(tid, None)
            self._save_tasks()
            return {"success": True, "removed": tid}
        return {"success": False, "error": "未找到该任务"}


def create_plugin() -> DshBridgePlugin:
    return DshBridgePlugin()
