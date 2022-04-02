from dataclasses import dataclass
import uuid
import pytest
from orchestrator import Orchestrator, Message, Trigger, Command
from orchestrator.broker import ConsumerRecord
from tests.dummy_broker import DummyBroker

from tests.jobdefs import ReadySetGoJob, _Resp
from tests.util import wait_for

pytestmark = pytest.mark.asyncio


@dataclass(eq=True, frozen=True)
class Person:
    name: str
    age: int


async def test_registration(dummy_broker):
    orch = Orchestrator(broker=dummy_broker)

    trigger = Trigger("a", Person("cameron", 32))
    orch.register_trigger(trigger, ReadySetGoJob)

    assert 1 == len(orch.triggers)
    assert [
        "topic.resp.rdy",
        "topic.resp.set",
        "topic.resp.go",
    ] == orch._all_job_topics()
    assert ["a"] == orch._all_trigger_topics()


async def test_trigger_flow(dummy_broker: DummyBroker):
    consumer = await dummy_broker.get_consumer()
    producer = await dummy_broker.get_producer()

    corr_id = str(uuid.uuid1())

    orch = Orchestrator(broker=dummy_broker)
    trigger = Trigger("a", Person("cameron", 32))

    orch.register_trigger(trigger, ReadySetGoJob)
    orch.start()

    # load the consumer with trigger message
    # normally this would come through on the "a" topic
    await consumer.load_for_test(
        ConsumerRecord(
            topic="a",
            message=Message(correlation_id=corr_id, payload=Person("cameron", 32)),
        )
    )

    # verifies that a new job was created due to trigger
    await wait_for(lambda: corr_id in orch._running_jobs)

    #
    # READY
    #

    # ensure request was sent
    await wait_for(
        lambda: producer.sent[-1]
        == ("topic.commands", Message(correlation_id=corr_id, payload=Command.READY))
    )
    # send response
    await consumer.load_for_test(
        ConsumerRecord(
            topic="topic.resp.rdy",
            message=Message(correlation_id=corr_id, payload=_Resp(0)),
        )
    )

    # #
    # # SET
    # #
    # # ensure request was sent
    # await wait_for(
    #     lambda: producer.sent[-1]
    #     == ("topic.commands", Message(correlation_id=corr_id, payload=Command.SET))
    # )
    # # send response
    # await consumer.load_for_test(
    #     ConsumerRecord(
    #         topic="topic.resp.set",
    #         message=Message(correlation_id=corr_id, payload=_Resp(1)),
    #     )
    # )

    await orch.cancel()
