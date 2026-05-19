from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx


@dataclass(frozen=True)
class LocalLLMConfig:
    backend: str = "ollama"
    base_url: str = "http://127.0.0.1:11434"
    model: str = ""
    timeout_sec: int = 300


@dataclass(frozen=True)
class LocalLLMResponse:
    status: str
    text: str = ""
    skipped_reason: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == "ok"


class LocalLLMClient:
    def __init__(self, config: LocalLLMConfig) -> None:
        self.config = config

    def chat(self, messages: list[dict[str, str]]) -> LocalLLMResponse:
        backend = self.config.backend.strip().lower()
        if not self.config.model.strip():
            return LocalLLMResponse(
                status="skipped",
                skipped_reason="LOCAL_LLM_MODEL is empty",
            )
        if backend != "ollama":
            return LocalLLMResponse(
                status="skipped",
                skipped_reason=f"unsupported local LLM backend: {self.config.backend}",
            )
        return self._ollama_chat(messages)

    def _ollama_chat(self, messages: list[dict[str, str]]) -> LocalLLMResponse:
        url = self.config.base_url.rstrip("/") + "/api/chat"
        payload = {
            "model": self.config.model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": 0.2,
            },
        }
        try:
            with httpx.Client(timeout=self.config.timeout_sec) as client:
                response = client.post(url, json=payload)
                response.raise_for_status()
                data = response.json()
        except httpx.ConnectError:
            return LocalLLMResponse(
                status="skipped",
                skipped_reason=f"local LLM server is not reachable: {self.config.base_url}",
            )
        except httpx.TimeoutException:
            return LocalLLMResponse(
                status="skipped",
                skipped_reason=f"local LLM request timed out after {self.config.timeout_sec}s",
            )
        except httpx.HTTPStatusError as exc:
            return LocalLLMResponse(
                status="skipped",
                skipped_reason=f"local LLM HTTP error: {exc.response.status_code}",
            )
        except (httpx.RequestError, ValueError) as exc:
            return LocalLLMResponse(
                status="skipped",
                skipped_reason=f"local LLM request failed: {type(exc).__name__}: {exc}",
            )

        text = str(data.get("message", {}).get("content", "")).strip()
        if not text:
            return LocalLLMResponse(
                status="skipped",
                skipped_reason="local LLM returned an empty response",
                raw=data,
            )
        return LocalLLMResponse(status="ok", text=text, raw=data)
