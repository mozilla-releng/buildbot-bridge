from concurrent.futures import CancelledError
from unittest import mock

import pytest

from bbb.reflector import ReflectedTask


class AsyncMock(mock.MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


class BBB_Task:
    def __init__(self, taskId=None, runId=None, takenUntil=0, buildrequestId=0):
        self.taskId = taskId
        self.runId = runId
        self.takenUntil = takenUntil
        self.buildrequestId = buildrequestId


@pytest.fixture
def rt():
    bbb_task = BBB_Task(takenUntil='2017-03-31 22:00:00Z')
    rt = ReflectedTask(bbb_task)
    return rt


@pytest.mark.asyncio
async def test_start_and_cancel_task(event_loop, rt):
    rt.loop = event_loop
    rt.start()

    assert rt.future
    assert not rt.future.cancelled()
    assert not rt.future.done()

    with pytest.raises(CancelledError), mock.patch('bbb.taskcluster.reclaim_task'):
        rt.cancel()
        await rt.future
    assert rt.future.cancelled()
    assert rt.future.done()


def test_reclaim_at():
    bbb_task = BBB_Task(takenUntil='2017-03-31 22:00:00Z')
    t = ReflectedTask(bbb_task)
    assert t.reclaim_at == 1490997300


def test_should_reclaim(rt):
    with mock.patch('arrow.now') as now:
        now.return_value.timestamp = 1490997300
        assert rt.should_reclaim

        now.return_value.timestamp = 1490997300 - 1
        assert not rt.should_reclaim

def test_snooze_time(rt):
    assert rt.reclaim_at == 1490997300

    with mock.patch('arrow.now') as now:
        now.return_value.timestamp = 1490997300
        assert rt.snooze_time == 0

        now.return_value.timestamp = 1490997300 - 10
        assert rt.snooze_time == 10

        now.return_value.timestamp = 1490997300 + 10
        assert rt.snooze_time == 0

@pytest.mark.asyncio
async def test_snooze(rt):
    with mock.patch('arrow.now') as now, mock.patch('asyncio.sleep', new_callable=AsyncMock) as sleep:
        now.return_value.timestamp = 1490997300 - 10
        assert rt.snooze_time == 10

        await rt.snooze()
        assert sleep.called_with(10)
