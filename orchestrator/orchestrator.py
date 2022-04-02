import asyncio
from typing import Any, NamedTuple, Optional, Type, TypeVar

from orchestrator import Job
from orchestrator.broker import Broker

CorrID = TypeVar("CorrID", bound=str)


class Trigger(NamedTuple):
    topic: str
    payload: Any


class Orchestrator:
    def __init__(self, broker: Broker) -> None:
        self.broker = broker
        self.triggers: dict[Trigger, Type[Job]] = {}

        self._running_jobs: dict[CorrID, Job] = {}
        self._trigger_listener_task: Optional[asyncio.Task] = None
        self._job_listener_task: Optional[asyncio.Task] = None

    def register_trigger(self, trigger: Trigger, job_type: Type[Job]):
        self.triggers[trigger] = job_type

    def _all_trigger_topics(self) -> list[str]:
        return [t.topic for t in self.triggers]

    def _all_job_topics(self) -> list[str]:
        topics = []
        for job_type in self.triggers.values():
            topics.extend(job_type.response_topics())
        return topics

    async def _start_job_listener(self) -> None:
        """Start listening to all response topics defined by all of the registered jobs

        When a message arrives, check to see if the correlation ID has a matching job.
        If so, allow that job to handle the message
        """
        consumer = await self.broker.get_consumer()

        async for record in consumer.subscribe(self._all_trigger_topics()):
            corr_id = record.message.correlation_id
            if corr_id in self._running_jobs:
                self._running_jobs[corr_id].handle_message(
                    topic=record.topic, message=record.message
                )

    async def _start_trigger_listener(self) -> None:
        """Start listening to all trigger topics

        When a trigger is satisfied, create an instance of the matching Job and store it.
        Jobs will be addressed by a correlation ID that all future messages should carry along with them
        """
        consumer = await self.broker.get_consumer()

        async for record in consumer.subscribe(self._all_trigger_topics()):
            trigger = Trigger(topic=record.topic, payload=record.message.payload)

            job_type = self.triggers.get(trigger)
            if job_type is None:
                continue
            corr_id = record.message.correlation_id
            self._running_jobs[corr_id] = job_type(correlation_id=corr_id)

    def start(self) -> None:
        self._trigger_listener_task = asyncio.create_task(
            self._start_trigger_listener()
        )
        self._job_listener_task = asyncio.create_task(self._start_job_listener())
