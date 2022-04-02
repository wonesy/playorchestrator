import asyncio
from itertools import product
import uuid
import pytest

from orchestrator import Message
from orchestrator.job import JobResult, StepStatus

from .jobdefs import ReadySetGoJob, _Resp
from .util import wait_for
from .dummy_broker import DummyBroker

pytestmark = pytest.mark.asyncio


async def test_job(dummy_broker: DummyBroker):
    corr_id = str(uuid.uuid1())
    producer = await dummy_broker.get_producer()
    job = ReadySetGoJob(correlation_id=corr_id, producer=producer)

    # ready
    job.handle_message(
        topic="topic.resp.rdy",
        message=Message(correlation_id=corr_id, payload=_Resp(0)),
    )
    await wait_for(lambda: job._step_states[0].status == StepStatus.COMPLETED)

    # set
    job.handle_message(
        topic="topic.resp.set",
        message=Message(correlation_id=corr_id, payload=_Resp(1)),
    )
    await wait_for(lambda: job._step_states[1].status == StepStatus.COMPLETED)

    # go
    job.handle_message(
        topic="topic.resp.go",
        message=Message(correlation_id=corr_id, payload=_Resp(2)),
    )
    await wait_for(lambda: job._step_states[2].status == StepStatus.COMPLETED)

    assert await job == JobResult.COMPLETED
    assert all([s.status == StepStatus.COMPLETED for s in job._step_states])
    assert job._step_states[0].response_payload.num == 0
    assert job._step_states[1].response_payload.num == 1
    assert job._step_states[2].response_payload.num == 2


async def test_job_fail(dummy_broker: DummyBroker):
    corr_id = str(uuid.uuid1())
    producer = await dummy_broker.get_producer()
    job = ReadySetGoJob(correlation_id=corr_id, producer=producer)

    # "set", skip "ready"
    job.handle_message(
        topic="topic.resp.set",
        message=Message(correlation_id=corr_id, payload=_Resp(0)),
    )

    with pytest.raises(asyncio.TimeoutError):
        await wait_for(lambda: job._step_states[0].status == StepStatus.COMPLETED)
    await job.cancel()
    assert await job == JobResult.CANCELLED


async def test_job_response_topics():
    assert [
        "topic.resp.rdy",
        "topic.resp.set",
        "topic.resp.go",
    ] == ReadySetGoJob.response_topics()
