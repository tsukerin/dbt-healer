from abc import ABC, abstractmethod
import logging
import requests
from google import genai
from google.genai import types
from ollama import Client

from common.config import Config, get_config
from app.utils import get_error_files_from_dbt_log, get_file_context, get_instruction

class AbstractProvider(ABC):
    def __init__(
        self,
        context: str | None = None,
        *,
        config: Config | None = None,
        api_key: str | None = None,
        model: str | None = None,
    ):
        self.context = context
        self.config = config or get_config()
        self.ai_api_key = self.config.ai_api_key if api_key is None else api_key
        self.model = self.config.ai_model if model is None else model

    @property
    @abstractmethod
    def client(self):
        ...

    @abstractmethod
    def send_for_llm(self, file_context: str) -> str:
        """Sends the file name where the error occurs and returns a response"""
        ...

    @abstractmethod
    def get_models_list(self) -> list[str]:
        """Provides list of models of current provider"""
        ...

    def get_solution(self) -> str:
        """Gets the final response that contains presumably solution"""
        files = get_error_files_from_dbt_log(self.context)
        if not files and self.config.uploaded_dbt_log.exists():
            try:
                files = get_error_files_from_dbt_log(
                    self.config.uploaded_dbt_log.read_text(encoding="utf-8", errors="replace")
                )
            except OSError as exc:
                logging.warning("Unable to read uploaded dbt log for file detection: %s", exc)

        if files:
            logging.info("Files selected from dbt log: %s", files)
        else:
            logging.warning("No dbt error file found in logs; skipping AI fix generation.")
            return ""

        files = list(dict.fromkeys(files))
        results = []

        for file in files:
            file_ctx = get_file_context(file)
            if not file_ctx:
                logging.warning("No file context found for: %s", file)
                continue
            results.append(self.send_for_llm(file_ctx))

        return "\n----\n".join(results)
    
class GoogleAIProvider(AbstractProvider):
    @property
    def client(self):
        return genai.Client(api_key=self.ai_api_key)

    def send_for_llm(self, file_context: str) -> str:
        response = self.client.models.generate_content(
                model=self.model,
                config=types.GenerateContentConfig(
                system_instruction=get_instruction("handle_solution")),
                contents=self.context + f'\n{file_context}'
            )

        return response.text
        
    def get_models_list(self):
        return [model.name for model in self.client.models.list() if model.name]


class BaseChatProvider(AbstractProvider):
    def _normalize_model_output(self, text: str) -> str:
        if "<think>" in text:
            return text.split("</think>")[-1].strip()
        return text.strip()

    def _solution_messages(self, file_context: str) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": get_instruction("handle_solution")
            },
            {
                "role": "user",
                "content": self.context + f'\n{file_context}',
            },
        ]


class DeepSeekProvider(BaseChatProvider):
    base_url = "https://api.deepseek.com"
    fallback_models = ["deepseek-v4-flash", "deepseek-v4-pro"]

    @property
    def client(self):
        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {self.ai_api_key}",
            "Content-Type": "application/json",
        })
        return session

    def _request(self, method: str, path: str, **kwargs) -> dict:
        response = self.client.request(
            method=method,
            url=f"{self.base_url}{path}",
            timeout=120,
            **kwargs,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = response.text.strip()
            if detail:
                raise RuntimeError(f"DeepSeek API request failed: {detail}") from exc
            raise

        return response.json()

    def _chat(self, messages: list[dict[str, str]]) -> str:
        data = self._request(
            "POST",
            "/chat/completions",
            json={
                "model": self.model,
                "messages": messages,
                "stream": False,
            },
        )
        choices = data.get("choices") or []
        if not choices:
            raise ValueError("DeepSeek API response did not include choices.")

        message = choices[0].get("message") or {}
        return message.get("content") or ""

    def send_for_llm(self, file_context: str) -> str:
        return self._normalize_model_output(self._chat(self._solution_messages(file_context)))

    def get_models_list(self) -> list[str]:
        if not self.ai_api_key:
            return self.fallback_models

        try:
            data = self._request("GET", "/models")
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            logging.warning("Unable to list DeepSeek models: %s", exc)
            return self.fallback_models

        models = [model["id"] for model in data.get("data", []) if model.get("id")]
        return models or self.fallback_models
    
class BaseOllamaProvider(BaseChatProvider):
    def _chat(self, messages: list[dict[str, str]]):
        return self.client.chat(model=self.model, messages=messages)

    def send_for_llm(self, file_context: str) -> str:
        file = self._chat(self._solution_messages(file_context))

        res = file.message.content or ""

        return self._normalize_model_output(res)

    def get_models_list(self) -> list[str]:
        return [model["model"] for model in self.client.list()["models"] if model["model"]]


class LocalOllamaProvider(BaseOllamaProvider):
    def _limit_text(self, text: str) -> str:
        max_chars = self.config.ai_max_input_chars
        if not max_chars or max_chars <= 0 or len(text) <= max_chars:
            return text

        marker = (
            f"\n\n[... truncated {len(text) - max_chars} chars "
            "to keep the local Ollama prompt within the configured input limit ...]\n\n"
        )
        budget = max_chars - len(marker)
        if budget <= 0:
            return text[:max_chars]

        head = budget // 2
        tail = budget - head
        logging.warning(f"Truncating local Ollama prompt from {len(text)} to {max_chars} characters")
        return text[:head] + marker + text[-tail:]

    def _limit_messages(self, messages: list[dict[str, str]]) -> list[dict[str, str]]:
        limited = []
        for message in messages:
            if message.get("role") == "user":
                message = {**message, "content": self._limit_text(message.get("content", ""))}
            limited.append(message)
        return limited

    def _chat(self, messages: list[dict[str, str]]):
        kwargs = {
            "model": self.model,
            "messages": self._limit_messages(messages),
        }
        if self.config.ollama_num_ctx and self.config.ollama_num_ctx > 0:
            kwargs["options"] = {"num_ctx": self.config.ollama_num_ctx}

        return self.client.chat(**kwargs)

    @property
    def client(self):
        if self.config.ollama_host:
            return Client(host=self.config.ollama_host)
        return Client()


class APIOllamaProvider(BaseOllamaProvider):
    @property
    def client(self):
        return Client(
            host="https://ollama.com",
            headers={'Authorization': 'Bearer ' + self.ai_api_key}
        )
