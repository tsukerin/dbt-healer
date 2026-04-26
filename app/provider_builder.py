from enum import Enum

from common.config import get_config
from app.providers import (
    AbstractProvider,
    APIOllamaProvider,
    DeepSeekProvider,
    GoogleAIProvider,
    LocalOllamaProvider,
)


class ProviderType(str, Enum):
    OLLAMA = "Ollama"
    GOOGLE_AI = "Google AI Studio"
    DEEPSEEK = "DeepSeek API"


class OllamaProviderType(str, Enum):
    API = "Ollama (API)"
    LOCAL = "Ollama (Local)"


def build_provider(ai_provider: ProviderType | str, **kwargs) -> AbstractProvider:
    provider_aliases = {
        "DeepSeek": ProviderType.DEEPSEEK.value,
    }
    provider_type = ProviderType(provider_aliases.get(str(ai_provider), ai_provider))
    ollama_type = (
        kwargs.pop("ollama_type", None)
        or kwargs.pop("provider_type", None)
        or kwargs.pop("type", None)
    )

    if provider_type is ProviderType.OLLAMA:
        selected_type = OllamaProviderType(ollama_type or get_config().ai_provider_type or OllamaProviderType.API.value)
        if selected_type is OllamaProviderType.LOCAL:
            return LocalOllamaProvider(**kwargs)
        return APIOllamaProvider(**kwargs)
    if provider_type is ProviderType.GOOGLE_AI:
        return GoogleAIProvider(**kwargs)
    if provider_type is ProviderType.DEEPSEEK:
        return DeepSeekProvider(**kwargs)
    raise ValueError(f"Unsupported provider: {ai_provider}")
