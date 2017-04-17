import asyncio
import logging

import arrow
from statsd import StatsClient

import bbb
import bbb.db as db
import bbb.taskcluster as tc

_reflected_tasks = dict()
log = logging.getLogger(__name__)
statsd = StatsClient(prefix='bbb.reflector')


class ReflectedTask:
    loop = asyncio.get_event_loop()

    def __init__(self, bbb_task):
        self.bbb_task = bbb_task
        self.future = None
        # bbb_task.takenUntil cannot be updated
        self.taken_until = bbb_task.takenUntil

    def start(self):
        def remove_task(_):
            del _reflected_tasks[self.bbb_task.buildrequestId]

        log.info("Start watching task: %s run: %s", self.bbb_task.taskId,
                 self.bbb_task.runId)
        self.future = self.loop.create_task(self.reclaim_loop())
        self.future.add_done_callback(remove_task)

    def cancel(self):
        log.info("Stop watching task: %s run: %s", self.bbb_task.taskId,
                 self.bbb_task.runId)
        self.future.cancel()

    def refresh(self, bbb_task):
        self.bbb_task = bbb_task
        self.taken_until = bbb_task.takenUntil

    @property
    def reclaim_at(self):
        taken_until = arrow.get(self.taken_until).timestamp
        reclaim_at = taken_until - bbb.RECLAIM_THRESHOLD
        return reclaim_at

    @property
    def should_reclaim(self):
        now = arrow.now().timestamp
        return now >= self.reclaim_at

    @property
    def snooze_time(self):
        now = arrow.now().timestamp
        snooze = max([self.reclaim_at - now, 0])
        return snooze

    async def snooze(self):
        snooze = self.snooze_time
        log.info("Will reclaim task: %s run:%s in %s seconds",
                 self.bbb_task.taskId, self.bbb_task.runId, snooze)
        await asyncio.sleep(snooze)

    async def reclaim_task(self):
        log.info("Reclaim task: %s run:%s ", self.bbb_task.taskId,
                 self.bbb_task.runId)
        res = await tc.reclaim_task(
            self.bbb_task.taskId,
            int(self.bbb_task.runId),
            self.bbb_task.buildrequestId)
        if res:
            self.taken_until = res["takenUntil"]
            log.info("Update takenUntil of task: %s run:%s to %s",
                     self.bbb_task.taskId, self.bbb_task.runId,
                     res["takenUntil"])
            await db.update_taken_until(
                self.bbb_task.buildrequestId,
                arrow.get(self.taken_until).timestamp)
        else:
            log.warning("Reclaim failed, sleep a bit. Task: %s, run: %s",
                        self.bbb_task.taskId, self.bbb_task.runId)
            await asyncio.sleep(60)

    async def reclaim_loop(self):
        while True:
            # TODO: Error handling here
            if self.should_reclaim:
                await self.reclaim_task()
            await self.snooze()


async def main_loop(poll_interval):
    log.info("Fetching BBB tasks...")
    all_bbb_tasks = await db.fetch_all_tasks()
    statsd.gauge('reflector.all_tasks', len(all_bbb_tasks))
    log.info("Total BBB tasks: %s", len(all_bbb_tasks))

    statsd.gauge('reflector.reflected_tasks', len(_reflected_tasks))
    log.info("Reflected BBB tasks: %s", len(_reflected_tasks))

    inactive_bbb_tasks = [t for t in all_bbb_tasks if t.takenUntil is None]
    statsd.gauge('reflector.inactive_tasks', len(inactive_bbb_tasks))
    log.info("BBB tasks without takenUntil set: %s", len(inactive_bbb_tasks))

    actionable_bbb_tasks = [t for t in all_bbb_tasks
                            if t.takenUntil is not None]
    log.info("BBB tasks with takenUntil set: %s", len(actionable_bbb_tasks))

    actionable_request_ids = [bbb_task.buildrequestId for bbb_task in
                              actionable_bbb_tasks]
    new_bbb_tasks = [bbb_task for bbb_task in actionable_bbb_tasks if
                     bbb_task.buildrequestId not in _reflected_tasks.keys()]
    statsd.gauge('reflector.new_tasks', len(new_bbb_tasks))
    log.info("New BBB tasks: %s", len(new_bbb_tasks))
    finished_tasks = [rt for brid, rt in _reflected_tasks.items() if
                      brid not in actionable_request_ids]
    statsd.gauge('reflector.finished_tasks', len(finished_tasks))
    log.info("Finished BBB tasks: %s", len(finished_tasks))

    refresh_reflected_tasks(actionable_bbb_tasks)

    await asyncio.wait([
        add_new_tasks(new_bbb_tasks),
        remove_finished_tasks(finished_tasks),
        process_inactive_tasks(inactive_bbb_tasks),
    ])
    await asyncio.sleep(poll_interval)


def refresh_reflected_tasks(bbb_tasks):
    """Refresh in-memory data
    Assuming that we can run multiple instances of the reflector, update the
    in-memory copies of the BBB tasks to avoid extra DB and TC calls to fetch
    latest values, e.g. takenUntil.
    """
    for bbb_task in bbb_tasks:
        if bbb_task.buildrequestId in _reflected_tasks.keys():
            log.debug("Refreshing %s", bbb_task.taskId)
            _reflected_tasks[bbb_task.buildrequestId].refresh(bbb_task)


async def add_new_tasks(new_bbb_tasks):
    """Start reflecting a task"""
    for bbb_task in new_bbb_tasks:
        rt = ReflectedTask(bbb_task)
        _reflected_tasks[bbb_task.buildrequestId] = rt
        rt.start()


async def remove_finished_tasks(finished_reflected_tasks):
    """Stop reflecting finished tasks

    After the BBListener removes a task from the database, the reflector
    stops reclaiming the task.
    """

    if finished_reflected_tasks:
        task_ids = [rt.bbb_task.taskId for rt in finished_reflected_tasks]
        log.info("Remove finished tasks: %s", ", ".join(task_ids))
        # After the asyncio task is cancelled, the "done_callback" will remove
        # the task from the list of watched tasks
        for rt in finished_reflected_tasks:
            rt.cancel()


async def process_inactive_tasks(tasks):
    """Process tasks with no `takenUntil` set.

    Cancel tasks when a buildrequest has been cancelled before starting.
    Cancelling tasks that were running is handled by the BB listener.
    """

    async def delete_task(task_id, request_id):
        log.info("Cancelling task: %s", task_id)
        await tc.cancel_task(task_id)
        log.info("Removing from DB, task: %s, requestid: %s", task_id,
                 request_id)
        await db.delete_task_by_request_id(request_id)

    request_ids = [t.buildrequestId for t in tasks]
    cancelled = await db.get_cancelled_build_requests(request_ids)
    cancelled_tasks = [t for t in tasks if t.buildrequestId in cancelled]
    if cancelled_tasks:
        log.info("Found cancelled-before-started tasks: %s",
                 ", ".join(t.taskId for t in cancelled_tasks))
        await asyncio.wait([
            delete_task(task_id=t.taskId, request_id=t.buildrequestId)
            for t in cancelled_tasks
        ])
