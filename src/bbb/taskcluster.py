import asyncio
import logging

import arrow
from statsd import StatsClient
from taskcluster.async import Queue
from taskcluster.async import TaskclusterRestFailure

import bbb
import bbb.db as db
import bbb.selfserve as selfserve

_queue = None
log = logging.getLogger(__name__)
statsd = StatsClient(prefix='bbb.taskcluster')


def init(options):
    global _queue
    _queue = Queue(options=options)


@statsd.timer('reclaim_task')
async def reclaim_task(task_id, run_id, request_id):
    if bbb.DRY_RUN_RECLAIM:
        log.info("DRY RUN: reclaim_task(%s, %s, %s)", task_id, run_id,
                 request_id)
        return

    try:

        task_status = await _queue.status(task_id)

        # Check the deadline first
        deadline = task_status["status"]["deadline"]
        if arrow.get(deadline).timestamp < arrow.now().timestamp:
            log.warning("task %s: run %s deadline exceeded (%s), deleting",
                        task_id, run_id, deadline)
            await db.delete_task_by_request_id(request_id)
            return

        # Only "running" tasks can be reclaimed
        state = task_status["status"]["runs"][run_id]["state"]
        if state == "running":
            return await _queue.reclaimTask(task_id, run_id)
        else:
            log.warning("task %s: run %s is not in running state, "
                        "not reclaiming (%s)", task_id, run_id, state)

    except TaskclusterRestFailure as e:
        status_code = e.status_code

        if status_code == 404:
            # Expired tasks are removed from the TC DB
            log.warning("task %s: run %s: Cannot find task in TC, removing it",
                        task_id, run_id)
            await db.delete_task_by_request_id(request_id)

        elif status_code == 403:
            # Bug 1270785. Claiming a completed task returns 403.
            log.warning("task %s: run %s: Cannot modify task in TC, removing",
                        task_id, run_id)
            await db.delete_task_by_request_id(request_id)

        elif status_code == 409:
            job_complete = (await db.get_build_request(request_id))['complete']
            if job_complete:
                log.info("task %s: run %s: buildrequest %s: got 409 when "
                         "reclaiming task; assuming task is complete and will "
                         "be handled by BBListener.", task_id, run_id,
                         request_id)
            else:
                log.warning(
                    "task %s: run %s: deadline exceeded; cancelling it",
                    task_id, run_id)
                branch = await db.get_branch(request_id)
                build_ids = await db.get_build_id(request_id)
                try:
                    await asyncio.wait([
                        selfserve.cancel_build(branch, build_id)
                        for build_id in build_ids
                    ])
                    # delete from the DB only if all cancel requests pass
                    await db.delete_task_by_request_id(request_id)
                except:
                    log.exception("task %s: run %s: buildrequest %s: "
                                  "failed to cancel task", task_id, run_id,
                                  request_id)

        else:
            log.warning("task %s: run %s: Unhandled TC status code %s",
                        task_id, run_id, status_code)


@statsd.timer('cancel_task')
async def cancel_task(task_id):
    if bbb.DRY_RUN:
        log.info("DRY RUN: cancel_task(%s)", task_id)
        return
    try:
        await _queue.cancelTask(task_id)
    except TaskclusterRestFailure as e:
        log.error("task %s:  status_code: %s body: %s", task_id, e.status_code,
                  e.body)
