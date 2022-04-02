from abc import ABC
import asyncio
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from typing import Any

from orchestrator.exceptions import JobAlreadyComplete, JobNotStarted
from orchestrator.step import Message, Step
from orchestrator.broker import Producer


class JobResult(Enum):
    NOT_STARTED = 1
    COMPLETED = 2
    CANCELLED = 3


class StepStatus(Enum):
    NOT_STARTED = 1
    STARTED = 2
    COMPLETED = 3


@dataclass
class StepState:
    step: Step
    status: StepStatus
    response_payload: Any
    fut: asyncio.Future[Message]


class Job(ABC):
    steps: list[Step]

    def __init__(self, correlation_id: str, producer: Producer) -> None:
        self.correlation_id = correlation_id
        self._producer = producer

        self._current_step_idx: int = 0
        self._step_states: list[StepState] = [
            StepState(
                step=s,
                status=StepStatus.NOT_STARTED,
                response_payload=None,
                fut=asyncio.Future(),
            )
            for s in self.steps
        ]
        self._task = asyncio.create_task(self._start())
        self._job_result = asyncio.Future()

    def _cur_step_state(self) -> StepState:
        if self._current_step_idx >= len(self._step_states):
            raise JobAlreadyComplete("step index out of bounds")
        return self._step_states[self._current_step_idx]

    def _step_response_satisfied(self, topic: str, message: Message) -> bool:
        step_state = self._cur_step_state()

        expected_response = step_state.step.response

        return (
            expected_response.topic == topic
            and message.correlation_id == self.correlation_id
            and type(message.payload) == expected_response.payload_type
        )

    @classmethod
    def response_topics(cls) -> list[str]:
        return [s.response.topic for s in cls.steps]

    def handle_message(self, topic: str, message: Message):
        if self._step_response_satisfied(topic, message):
            self._cur_step_state().fut.set_result(message)

    async def cancel(self) -> None:
        if self._task is not None:
            with suppress(asyncio.CancelledError):
                self._task.cancel()
                await self._task
        if not self._job_result.done():
            self._job_result.set_result(JobResult.CANCELLED)

    async def _start(self) -> None:
        for i, step_state in enumerate(self._step_states):
            self._current_step_idx = i
            step_state.status = StepStatus.STARTED

            # send the request
            req = step_state.step.request
            await self._producer.send(
                topic=req.topic,
                payload=Message(
                    correlation_id=self.correlation_id, payload=req.command
                ),
            )

            # wait for the response
            message = await step_state.fut

            step_state.response_payload = message.payload
            step_state.status = StepStatus.COMPLETED

        self._job_result.set_result(JobResult.COMPLETED)

    def __await__(self) -> JobResult:
        if self._job_result is None:
            raise JobNotStarted("must call the start method before awaiting")
        return self._job_result.__await__()
