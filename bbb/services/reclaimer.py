import time

from taskcluster.exceptions import TaskclusterRestFailure

from ..servicebase import ServiceBase, parseDateString

import logging
log = logging.getLogger(__name__)

class Reclaimer(ServiceBase):
    def __init__(self, interval, *args, **kwargs):
        super(Reclaimer, self).__init__(*args, **kwargs)
        self.interval = interval

    def start(self):
        log.info("Starting reclaimer")
        while True:
            self.reclaimTasks()
            time.sleep(self.interval)

    def reclaimTasks(self):
        for t in self.bbb_db.getAllTasks():
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
            # BuildRequest is complete, but hasn't been reaped yet. Nothing to do,
            # but the BBListener should come along and get rid of it soon.
            elif buildrequest.complete:
                log.info("BuildRequest %i is done, BBListener should process it soon", t.buildrequestId)
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
                        self.bbb_db.deleteBuildRequest(t.buildrequestId)
                    else:
                        log.error("Couldn't reclaim task: %s", e.superExc)
