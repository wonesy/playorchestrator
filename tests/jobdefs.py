from dataclasses import dataclass
from orchestrator import Job, Step, StepRequest, StepResponse, Command, Message


@dataclass(eq=True, frozen=True)
class _Resp:
    num: int


class ReadySetGoJob(Job):
    steps = [
        Step(
            name="Ready",
            request=StepRequest(topic="topic.commands", command=Command.READY),
            response=StepResponse(topic="topic.resp.rdy", payload_type=_Resp),
        ),
        Step(
            name="Set",
            request=StepRequest(topic="topic.commands", command=Command.SET),
            response=StepResponse(topic="topic.resp.set", payload_type=_Resp),
        ),
        Step(
            name="Go",
            request=StepRequest(topic="topic.commands", command=Command.GO),
            response=StepResponse(topic="topic.resp.go", payload_type=_Resp),
        ),
    ]
