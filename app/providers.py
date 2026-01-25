from abc import ABC, abstractmethod
from google import genai
from google.genai import types
import os

class AbstractProvider(ABC):
    def __init__(self, context):
        self.context = context

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

    def get_solution(self) -> str:
        """Gets the final response that contains presumably solution"""
        files = self.get_error_files()
        results = []

        for file in files:
            file_ctx = self._get_file_context(file)
            results.append(self.send_for_llm(file_ctx))

        return "\n----\n".join(results)

    def _get_file_context(self, file: str) -> str:
        from app.utils import get_file_context
        return get_file_context(file)
    
    def _get_instruction(self, name: str) -> str:
        from app.utils import get_instruction
        return get_instruction(name) 
    
class GoogleAI(AbstractProvider):
    _client = genai.Client(api_key=os.getenv('API_KEY'))

    @property
    def client(self):
        return self._client

    def __init__(self, context):
        super().__init__(context)

    def get_error_files(self) -> list[str]:
        file = self.client.models.generate_content(
            model="gemini-2.5-flash",
            config=types.GenerateContentConfig(
                system_instruction=self._get_instruction("handle_error_file")),
                contents=self.context
            )

        if file.text is None:
            raise ValueError('No error file identified.')

        return [file.text] if ',' not in file.text else list(map(str.strip, file.text.split(',')))

    def send_for_llm(self, file_context: str) -> str:
        response = self.client.models.generate_content(
            model="gemini-2.5-flash",
            config=types.GenerateContentConfig(
                system_instruction=self._get_instruction("handle_solution")),
                contents=self.context + f'\n{file_context}'
            )

        return response.text