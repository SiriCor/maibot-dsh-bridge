"""Fail-closed synchronous client for the local Desktop Host bridge.

Submit tasks with a short request, then consume and acknowledge independent
notifications. Model execution never holds a MaiBot HTTP connection open.
"""

from pathlib import Path
from typing import Any, Dict, Optional

import http.client
import json
import re
import time


BASE = "/.maibot-desktop-bridge"
ROUTE = BASE + "/tasks"
MAX_REQUEST_BYTES = 256 * 1024
MAX_RESPONSE_BYTES = 512 * 1024
MAX_FILES = 32
MAX_FILE_CHARS = 4096
_TOKEN = re.compile(rb"[a-f0-9]{64}\Z")
_JOB = re.compile(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}\Z")
_STREAM = re.compile(r"[A-Za-z0-9_-]{1,96}\Z")
_SESSION = re.compile(r"maibot-[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}\Z")
_ABSOLUTE = re.compile(r"(?:[A-Za-z]:[\\/]|/)")


class DesktopTransportError(Exception):
    """A sanitized local bridge transport or protocol failure."""


def _failure_message(data: Dict[str, Any], status: int) -> str:
    """Expose known host failures and a validated session ID, never arbitrary server text."""
    error = data.get("error")
    known_errors = {
        "Desktop task timed out",
        "Desktop bridge busy",
        "Desktop bridge unloaded",
        "Desktop bridge queue full",
        "client disconnected",
        "invalid request",
    }
    known_endings = {"completed", "aborted", "failed", "error", "unknown"}
    if isinstance(error, str) and (
        error in known_errors
        or error in {f"Desktop turn ended: {kind} without final text" for kind in known_endings}
    ):
        message = error
    else:
        message = f"Desktop bridge HTTP failure (status {status})"
    session_id = data.get("sessionId")
    if isinstance(session_id, str) and _SESSION.fullmatch(session_id):
        message += f" (session: {session_id})"
    return message


def _remaining(deadline):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise DesktopTransportError("Desktop bridge timed out")
    return seconds


def _files(value):
    """Accept only the bounded list of absolute deliverable paths the bridge sends."""
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > MAX_FILES:
        raise DesktopTransportError("Invalid Desktop bridge response")
    paths = []
    for entry in value:
        if (not isinstance(entry, str) or not entry or len(entry) > MAX_FILE_CHARS or
                not _ABSOLUTE.match(entry) or "\x00" in entry):
            raise DesktopTransportError("Invalid Desktop bridge response")
        paths.append(entry)
    return paths


def _request_json(*, port: int, token_file: str | Path, method: str,
                  path: str, payload: Optional[Dict[str, Any]] = None, expected: int = 200) -> Dict[str, Any]:
    """A bounded, short request. Execution lifetime is owned entirely by DSH."""
    if type(port) is not int or not 1 <= port <= 65535:
        raise DesktopTransportError("Invalid Desktop bridge endpoint")
    deadline = time.monotonic() + 10
    try:
        with open(token_file, "rb") as file:
            raw_token = file.read(67)
        token = raw_token.strip()
        if len(raw_token) > 66 or not _TOKEN.fullmatch(token):
            raise DesktopTransportError("Invalid Desktop bridge token")
    except (OSError, TypeError, ValueError):
        raise DesktopTransportError("Desktop bridge token unavailable") from None

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
    if body is not None and len(body) > MAX_REQUEST_BYTES:
        raise DesktopTransportError("Desktop bridge request too large")
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=_remaining(deadline))
    try:
        connection.connect()
        connection.sock.settimeout(_remaining(deadline))
        connection.request(method, path, body=body, headers={
            "Authorization": "Bearer " + token.decode("ascii"),
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        })
        connection.sock.settimeout(_remaining(deadline))
        response = connection.getresponse()
        length = response.getheader("Content-Length")
        if length is not None and not 0 <= int(length) <= MAX_RESPONSE_BYTES:
            raise DesktopTransportError("Desktop bridge response too large")
        chunks = []
        size = 0
        while True:
            connection.sock.settimeout(_remaining(deadline))
            chunk = response.read(min(8192, MAX_RESPONSE_BYTES + 1 - size))
            size += len(chunk)
            if size > MAX_RESPONSE_BYTES:
                raise DesktopTransportError("Desktop bridge response too large")
            if not chunk:
                break
            chunks.append(chunk)
        data = json.loads(b"".join(chunks).decode("utf-8"))
        if response.status != expected:
            raise DesktopTransportError(_failure_message(data if isinstance(data, dict) else {}, response.status))
        if not isinstance(data, dict):
            raise DesktopTransportError("Invalid Desktop bridge response")
        _remaining(deadline)
        return data
    except DesktopTransportError:
        raise
    except (OSError, TimeoutError, http.client.HTTPException, ValueError, TypeError, UnicodeError):
        raise DesktopTransportError("Desktop bridge transport failure") from None
    finally:
        connection.close()


def submit_task(*, port: int, token_file: str | Path, sender: str, stream: str,
                stream_id: str, context: Dict[str, str], task: str, timeout: int,
                delivery: str = "maibot") -> Dict[str, Any]:
    """Return an accepted task ID without waiting for a model response."""
    if (not isinstance(sender, str) or not sender or not _STREAM.fullmatch(stream)
            or not stream_id or len(stream_id) > 256 or not task.strip() or len(task) > 24000
            or type(timeout) is not int or not 10 <= timeout <= 1800
            or delivery not in {"maibot", "qq", "to_assistant"}):
        raise DesktopTransportError("Invalid Desktop bridge request")
    result = _request_json(port=port, token_file=token_file, method="POST", path=ROUTE,
                           payload={"sender": sender, "stream": stream, "streamId": stream_id,
                                    "context": context, "task": task, "timeout": timeout,
                                    "delivery": delivery}, expected=202)
    if result.get("status") != "accepted" or not _JOB.fullmatch(str(result.get("jobId", ""))):
        raise DesktopTransportError("Desktop bridge did not accept the task")
    return result


def get_notification(*, port: int, token_file: str | Path) -> Optional[Dict[str, Any]]:
    """Read one durable notification; it remains available until acknowledged."""
    result = _request_json(port=port, token_file=token_file, method="GET", path=BASE + "/notifications")
    if "notification" not in result:
        raise DesktopTransportError("Invalid Desktop bridge notification")
    item = result["notification"]
    if item is None:
        return None
    if (not isinstance(item, dict) or not _JOB.fullmatch(str(item.get("jobId", "")))
            or item.get("status") not in {"completed", "failed"}
            or not isinstance(item.get("streamId"), str) or not 1 <= len(item["streamId"]) <= 256
            or not isinstance(item.get("final"), str) or not isinstance(item.get("context"), dict)
            or set(item["context"]) - {"user_id", "group_id", "platform"}
            or any(not isinstance(value, str) or len(value) > 128 for value in item["context"].values())):
        raise DesktopTransportError("Invalid Desktop bridge notification")
    item["files"] = _files(item.get("files"))
    item.setdefault("delivery", "maibot")
    item.setdefault("qqDelivery", "none")
    item.setdefault("request", "")
    if not isinstance(item["request"], str) or len(item["request"]) > 24000:
        raise DesktopTransportError("Invalid Desktop bridge request metadata")
    if (item["delivery"] not in {"maibot", "qq", "to_assistant"}
            or item["qqDelivery"] not in {"none", "sent", "queued", "accepted"}):
        raise DesktopTransportError("Invalid Desktop bridge delivery state")
    if (item["status"] == "completed" and not item["final"].strip()
            and not item["files"] and item["qqDelivery"] == "none"):
        raise DesktopTransportError("Desktop bridge returned an empty result")
    if item["status"] == "failed":
        # Never echo an arbitrary server exception, credentials or task content as an error.
        errors = {
            "Desktop task timed out": "DSH 任务执行超时。",
            "Desktop bridge unloaded": "DSH 宿主桥接重载，任务已中断。",
            "DSH 宿主在任务执行期间重启，任务已中断。": "DSH 宿主在任务执行期间重启，任务已中断。",
            "DSH 未通过 QQ 发送请求内容。": "泰缇斯尚未通过 QQ 发送请求的内容。",
            "DSH 尚未通过 QQ 发送全部请求的文件。": "泰缇斯尚未通过 QQ 发完请求的文件。",
        }
        item["error"] = errors.get(item.get("error"), "DSH 任务执行失败，请检查宿主日志。")
    return item


def acknowledge(*, port: int, token_file: str | Path, job_id: str) -> None:
    """Delete a notification only after MaiBot has delivered every result part."""
    if not _JOB.fullmatch(job_id):
        raise DesktopTransportError("Invalid Desktop bridge task ID")
    result = _request_json(port=port, token_file=token_file, method="POST",
                           path=BASE + "/notifications/" + job_id + "/ack")
    if result.get("acknowledged") is not True:
        raise DesktopTransportError("Desktop bridge notification was not acknowledged")
