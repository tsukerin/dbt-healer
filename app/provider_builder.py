from enum import Enum

from app.providers import AbstractProvider, GoogleAIProvider, OllamaProvider


class ProviderType(str, Enum):
    OLLAMA = "Ollama"
    GOOGLE_AI = "Google AI Studio"


def build_provider(ai_provider: ProviderType | str, **kwargs) -> AbstractProvider:
    provider_type = ProviderType(ai_provider)

    if provider_type is ProviderType.OLLAMA:
        return OllamaProvider(**kwargs)
    if provider_type is ProviderType.GOOGLE_AI:
        return GoogleAIProvider(**kwargs)
    raise ValueError(f"Unsupported provider: {ai_provider}")
