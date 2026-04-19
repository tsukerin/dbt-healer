from abc import ABC, abstractmethod
import logging
import re
from google import genai
from google.genai import types
from ollama import Client

from common.config import Config, get_config
from app.utils import get_file_context, get_instruction

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
    def get_error_files(self) -> list[str]:
        """Provides list of dbt models where error appear"""
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
        files = [f.strip() for f in self.get_error_files() if f and f.strip()]
        files = list(dict.fromkeys(files))
        logging.info("Files selected for analysis: %s", files)
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

    def get_error_files(self) -> list[str]:
        file = self.client.models.generate_content(
                model=self.model,
                config=types.GenerateContentConfig(
                system_instruction=get_instruction("handle_error_file")),
                contents=self.context
            )

        if file.text is None:
            raise ValueError('No error file identified.')
        
        res = [file.text] if ',' not in file.text else list(map(str.strip, file.text.split(',')))

        logging.info("The following files were found: ", res)
        return res

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
    
class BaseOllamaProvider(AbstractProvider):
    def _normalize_model_output(self, text: str) -> str:
        if "<think>" in text:
            return text.split("</think>")[-1].strip()
        return text.strip()

    def _extract_file_names(self, text: str) -> list[str]:
        matches = re.findall(r"(?:[\w.-]+[\\/])*[\w.-]+\.[A-Za-z0-9_]+", text)
        files = []

        for match in matches:
            item = match.strip("`'\".,;:()[]{}")
            if item and item not in files:
                files.append(item)

        return files

    def get_error_files(self) -> list[str]:
        file = self.client.chat(model=self.model, messages=[
            {
                "role": "system",
                "content": get_instruction("handle_error_file")
            },
            {
                "role": "user",
                "content": self.context,
            },
        ])
        if file.message.content is None:
            raise ValueError('No error file identified.')

        text = self._normalize_model_output(file.message.content)
        res = self._extract_file_names(text)
        if not res:
            first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
            if first_line:
                res = [first_line]

        return res
    
    def send_for_llm(self, file_context: str) -> str:
        file = self.client.chat(model=self.model, messages=[
            {
                "role": "system",
                "content": get_instruction("handle_solution")
            },
            {
                "role": "user",
                "content": self.context + f'\n{file_context}',
            },
        ])

        res = file.message.content or ""

        return self._normalize_model_output(res)

    def get_models_list(self) -> list[str]:
        return [model["model"] for model in self.client.list()["models"] if model["model"]]


class LocalOllamaProvider(BaseOllamaProvider):
    @property
    def client(self):
        return Client()


class APIOllamaProvider(BaseOllamaProvider):
    @property
    def client(self):
        return Client(
            host="https://ollama.com",
            headers={'Authorization': 'Bearer ' + self.ai_api_key}
        )


class OllamaProvider(APIOllamaProvider):
    """Backward-compatible alias for the hosted Ollama API provider."""
