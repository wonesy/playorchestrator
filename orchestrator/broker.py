from typing import Any, AsyncIterator, Protocol

from orchestrator.job import Message


class ConsumerRecord:
    topic: str
    message: Message


class Producer(Protocol):
    async def send(self, topic: str, payload: Message) -> None:
        ...


class Consumer(Protocol):
    async def subscribe(self, topics: list[str]) -> AsyncIterator[ConsumerRecord]:
        yield ConsumerRecord(topic="", message=Message("", None))


class Broker(Protocol):
    async def get_producer(self) -> Producer:
        return Producer()

    async def get_consumer(self) -> Consumer:
        return Consumer()
