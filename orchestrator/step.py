from dataclasses import dataclass
from typing import Any, Type

from orchestrator.command import Command


@dataclass
class Message:
    correlation_id: str
    payload: Any


@dataclass
class StepRequest:
    topic: str
    command: Command


@dataclass
class StepResponse:
    topic: str
    payload_type: Type


@dataclass
class Step:
    name: str
    request: StepRequest
    response: StepResponse
