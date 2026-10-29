"""The only module that knows about LLM providers.

    DAGFOX_LLM_PROVIDER = offline | anthropic | bedrock | azure_openai
    DAGFOX_LLM_MODEL    = model id (Anthropic), model/inference-profile id (Bedrock),
                          or deployment name (Azure OpenAI)

Credentials are read by each provider SDK from its standard environment
variables (ANTHROPIC_API_KEY; the AWS credential chain + AWS_REGION;
AZURE_OPENAI_ENDPOINT / AZURE_OPENAI_API_KEY / OPENAI_API_VERSION).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pydantic_ai.models import Model

PROVIDERS = ("offline", "anthropic", "bedrock", "azure_openai")
DEFAULT_ANTHROPIC_MODEL = "claude-opus-5-5"


@dataclass(frozen=True)
class ModelHandle:
    model: Model
    provider: str
    name: str


class LLMConfigError(ValueError):
    pass


def build_model(provider: str, model_name: str | None, agent: str) -> ModelHandle:
    """Return a pydantic-ai Model for ``provider``. ``agent`` selects the offline rule set."""
    provider = provider.lower()

    if provider == "offline":
        from pydantic_ai.models.function import FunctionModel

        from dagfox.llm.offline import RULES

        name = "dagfox-offline-rules-v1"
        return ModelHandle(FunctionModel(RULES[agent], model_name=name), provider, name)

    if provider == "anthropic":
        from pydantic_ai.models.anthropic import AnthropicModel
        from pydantic_ai.providers.anthropic import AnthropicProvider

        name = model_name or DEFAULT_ANTHROPIC_MODEL
        return ModelHandle(AnthropicModel(name, provider=AnthropicProvider()), provider, name)

    if provider == "bedrock":
        from pydantic_ai.models.bedrock import BedrockConverseModel
        from pydantic_ai.providers.bedrock import BedrockProvider

        if not model_name:
            raise LLMConfigError("DAGFOX_LLM_MODEL must name a Bedrock model or inference profile id")
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        return ModelHandle(
            BedrockConverseModel(model_name, provider=BedrockProvider(region_name=region)), provider, model_name
        )

    if provider == "azure_openai":
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.azure import AzureProvider

        if not model_name:
            raise LLMConfigError("DAGFOX_LLM_MODEL must name the Azure OpenAI deployment")
        return ModelHandle(OpenAIChatModel(model_name, provider=AzureProvider()), provider, model_name)

    raise LLMConfigError(f"unknown DAGFOX_LLM_PROVIDER {provider!r}; choose from {PROVIDERS}")
