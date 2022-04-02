import pytest

from .dummy_broker import DummyBroker


@pytest.fixture
def dummy_broker():
    return DummyBroker()
