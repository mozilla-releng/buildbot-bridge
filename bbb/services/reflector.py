import time

from taskcluster.exceptions import TaskclusterRestFailure

from ..servicebase import ServiceBase
from ..timeutils import parseDateString

import logging
log = logging.getLogger(__name__)

class Reflector(ServiceBase):
    """Reflects Task state in Taskcluster based on the state of the
    Buildbot and BBB databases. Each task may be in one of the following
    states:
     * If takenUntil is unset and the BuildRequest is complete the task is
     considered cancelled. This will be forwarded to Taskcluster and the
     task will be removed from our database.
     * If takenUntil is unset and the BuildRequest is incomplete the task
     is either still pending or just started. There is nothing for us to
     do in this case.
     * If takenUntil is set and the BuildRequest is not complete, there is
     a Buildbot Build running for this Task. We need to reclaim the task
     to avoid Taskcluster expiring our claim.
     * If takenUntil is set and the BuildRequest is complete, the Buildbot
     Build has already completed and we're waiting for the BBListener to
     update Taskcluster with the job status. We currently do nothing for
     this, but we may want to reclaim in case the BBListener takes a long
     time to process the completed Build.
    """
    def __init__(self, interval, *args, **kwargs):
        super(Reflector, self).__init__(*args, **kwargs)
        self.interval = interval

    def start(self):
        log.info("Starting reflector")
        while True:
            self.reflectTasks()
            time.sleep(self.interval)

    def reflectTasks(self):
        # TODO: Probably need some error handling here to make sure all tasks
        # are processed even if one hit an exception.
        for t in self.bbb_db.tasks:
            log.info("Processing task: %s", t.taskId)
            buildrequest = self.buildbot_db.getBuildRequest(t.buildrequestId)
            builds = self.buildbot_db.getBuilds(t.buildrequestId)
            log.debug("Task info: %s", t)
            log.debug("BuildRequest: %s", buildrequest)

            # If takenUntil isn't set, this task has either never been claimed
            # or got cancelled.
            if not t.takenUntil:
                # If the buildrequest is showing complete, it was cancelled
                # before it ever started, so we need to pass that along to
                # taskcluster. Ideally, we'd watch Pulse for notification of
                # this, but our version of Buildbot has a bug that causes it
                # not to send those messages.
                if buildrequest.complete:
                    log.info("BuildRequest disappeared before starting, cancelling task")
                    self.tc_queue.cancelTask(t.taskId)
                    self.bbb_db.deleteBuildRequest(t.buildrequestId)
                    continue
                # Otherwise we're just waiting for it to start, nothing to do
                # because it hasn't been claimed at all yet.
                else:
                    log.info("Build hasn't started yet, nothing to do")
                    continue
            # BuildRequest is complete, but hasn't been reaped yet. We should
            # continue claiming this task for now, but the BBListener should
            # come along and get rid of it soon.
            elif buildrequest.complete:
                log.info("BuildRequest %i is done. BBListener should process it soon, reclaiming in the meantime", t.buildrequestId)
                # TODO: RECLAIM!
                continue

            # Build is running, which means it has already been claimed.
            # We need to renew the claim to make sure Taskcluster doesn't
            # expire it on us.
            else:
                if len(builds) > t.runId + 1:
                    log.warn("Too many buildbot builds? runId is %i but we have %i builds", t.runId, len(builds))

                log.info("BuildRequest is in progress, reclaiming")
                try:
                    result = self.tc_queue.reclaimTask(t.taskId, t.runId)
                    # Update our own db with the new claim time.
                    self.bbb_db.updateTakenUntil(t.buildrequestId, parseDateString(result["takenUntil"]))
                    log.info("Task %s now takenUntil %s", t.taskId, result['takenUntil'])
                except TaskclusterRestFailure, e:
                    if e.superExc.response.status_code == 409:
                        # Conflict; it's expired
                        log.warn("couldn't reclaim task %s: HTTP 409; deleting", t.taskId)
                        # TODO: probably should cancel the job in buildbot?
                        self.bbb_db.deleteBuildRequest(t.buildrequestId)
                    else:
                        log.error("Couldn't reclaim task: %s", e.superExc)
