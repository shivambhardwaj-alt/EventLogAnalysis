
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from typing import Any

from .base import DropContext, ExplainerError, build_messages

log = logging.getLogger(__name__)
_RETRY_STATUS = {429, 500, 502, 503, 504}


def _post_json(url: str, payload: dict[str, Any], headers: dict[str, str], timeout: float,
               retries: int = 2, backoff: float = 1.5) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    last_error = "unknown error"
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=body, method="POST",
                                     headers={"Content-Type": "application/json", **headers})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read(300).decode("utf-8", "replace") if exc.fp else ""
            last_error = f"HTTP {exc.code} from {url}: {detail[:200]}"
            if exc.code in (401, 403):
                raise ExplainerError(f"authentication failed (HTTP {exc.code}) - check the API key in .env") from None
            if exc.code not in _RETRY_STATUS:
                raise ExplainerError(last_error) from None
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
            reason = getattr(exc, "reason", exc)
            last_error = f"cannot reach {url}: {reason}"
        except json.JSONDecodeError:
            raise ExplainerError(f"non-JSON response from {url}") from None
        if attempt < retries:
            time.sleep(backoff * (attempt + 1))
    raise ExplainerError(last_error)


class OpenAICompatibleExplainer:
    def __init__(self, *, name: str, base_url: str, model: str, api_key: str | None,
                 timeout: float = 30.0, temperature: float = 0.2, max_tokens: int = 450,
                 retries: int = 2) -> None:
        self.name = name
        self.model = model
        self.base_url = base_url.rstrip("/")
        self._api_key = api_key
        self.timeout = timeout
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.retries = retries

    def __repr__(self) -> str:  
        return f"OpenAICompatibleExplainer(name={self.name!r}, base_url={self.base_url!r}, model={self.model!r})"

    def explain(self, ctx: DropContext) -> str:
        headers = {"Authorization": f"Bearer {self._api_key}"} if self._api_key else {}
        data = _post_json(
            f"{self.base_url}/chat/completions",
            {"model": self.model, "messages": build_messages(ctx),
             "temperature": self.temperature, "max_tokens": self.max_tokens},
            headers, self.timeout, retries=self.retries,
        )
        try:
            return str(data["choices"][0]["message"]["content"]).strip()
        except (KeyError, IndexError, TypeError):
            raise ExplainerError(f"unexpected response shape from {self.name}") from None


class OllamaExplainer:
    def __init__(self, *, base_url: str = "http://localhost:11434", model: str = "llama3.2",
                 timeout: float = 60.0, temperature: float = 0.2, retries: int = 0) -> None:
        self.name = "ollama"
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.temperature = temperature
        self.retries = retries

    def __repr__(self) -> str:
        return f"OllamaExplainer(base_url={self.base_url!r}, model={self.model!r})"

    def explain(self, ctx: DropContext) -> str:
        data = _post_json(
            f"{self.base_url}/api/chat",
            {"model": self.model, "messages": build_messages(ctx), "stream": False,
             "options": {"temperature": self.temperature}},
            {}, self.timeout, retries=self.retries,
        )
        try:
            return str(data["message"]["content"]).strip()
        except (KeyError, TypeError):
            raise ExplainerError("unexpected response shape from ollama") from None
