import asyncio
from time import sleep
from typing import AsyncIterator
from orchestrator import Message
from orchestrator.broker import ConsumerRecord


class DummyProducer:
    def __init__(self) -> None:
        self.sent = []

    async def send(self, topic: str, payload: Message):
        print(payload)
        self.sent.append((topic, payload))


class DummyConsumer:
    def __init__(self) -> None:
        self.to_send: asyncio.Queue[ConsumerRecord] = asyncio.Queue()

    async def subscribe(self, topics: list[str]) -> AsyncIterator[ConsumerRecord]:
        while self.to_send.empty():
            await asyncio.sleep(5)
        yield self.to_send.get_nowait()

    async def load_for_test(self, cr: ConsumerRecord):
        await self.to_send.put(cr)


class DummyBroker:
    def __init__(self) -> None:
        self.p = DummyProducer()

    async def get_producer(self):
        return self.p

    async def get_consumer(self):
        return DummyConsumer()
