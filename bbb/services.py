import re
import time

import arrow
from taskcluster.exceptions import TaskclusterRestFailure

from .servicebase import ListenerService, ServiceBase, ListenerServiceEvent, TaskNotFound
from .tcutils import createJsonArtifact
from .timeutils import parseDateString

import logging
log = logging.getLogger(__name__)


# Buildbot status'- these must match http://mxr.mozilla.org/build/source/buildbot/master/buildbot/status/builder.py#25
SUCCESS, WARNINGS, FAILURE, SKIPPED, EXCEPTION, RETRY, CANCELLED = range(7)

class BuildbotListener(ListenerService):
    """Listens for messages from Buildbot and responds appropriately.
    Currently handles the following types of events:
     * Build started (build.$builder.$buildnum.started)
     * Build finished (build.$builder.$buildnum.log_uploaded)
    """
    def __init__(self, tc_worker_group, tc_worker_id, pulse_queue_basename, pulse_exchange, *args, **kwargs):
        self.tc_worker_group = tc_worker_group
        self.tc_worker_id = tc_worker_id
        events = (
            ListenerServiceEvent(
                queue_name="%s/started" % pulse_queue_basename,
                exchange=pulse_exchange,
                routing_key="build.*.*.started",
                callback=self.handleStarted,
            ),
            ListenerServiceEvent(
                queue_name="%s/log_uploaded" % pulse_queue_basename,
                exchange=pulse_exchange,
                routing_key="build.*.*.log_uploaded",
                callback=self.handleFinished,
            ),
        )

        super(BuildbotListener, self).__init__(*args, events=events, **kwargs)

    def handleStarted(self, data, msg):
        """When a Build starts in Buildbot we claim the task in
        Taskcluster, which will move it into the "running" state there. We
        also update the BBB database with the claim time which triggers the
        Reflector to start reclaiming it periodically."""
        log.debug("Handling started event: %s", data)
        msg.ack()
        # TODO: Error handling?
        buildnumber = data["payload"]["build"]["number"]
        buildername = data["payload"]["build"]["builderName"]
        master = data["_meta"]["master_name"]
        incarnation = data["_meta"]["master_incarnation"]
        for brid in self.buildbot_db.getBuildRequests(buildnumber, buildername, master, incarnation):
            brid = brid[0]
            try:
                task = self.bbb_db.getTaskFromBuildRequest(brid)
            except TaskNotFound:
                log.debug("Task not found for brid %s, nothing to do.", brid)
                continue
            log.info("Claiming %s", task.taskId)
            # Taskcluster requires runId to be an int, but it comes to us as a long.
            claim = self.tc_queue.claimTask(task.taskId, int(task.runId), {
                "workerGroup": self.tc_worker_group,
                "workerId": self.tc_worker_id,
            })
            log.debug("Got claim: %s", claim)
            self.bbb_db.updateTakenUntil(brid, parseDateString(claim["takenUntil"]))

    def handleFinished(self, data, msg):
        """When a Build finishes in Buildbot we pass along the final state of
        it to the Task(s) associated with it in Taskcluster.

        It's important to note that we track the "build.foo.log_uploaded" event
        instead of "build.foo.finished". This is because only the former
        contains all of the BuildRequest ids that the Build satisfied.
        """
        log.debug("Handling finished event: %s", data)
        msg.ack()
        # Get the request_ids from the properties
        try:
            properties = dict((key, (value, source)) for (key, value, source) in data["payload"]["build"]["properties"])
        except KeyError:
            log.error("Couldn't get job properties")
            return

        request_ids = properties.get("request_ids")
        if not request_ids:
            log.error("Couldn't get request ids from %s", data)
            return

        # Sanity check
        assert request_ids[1] == "postrun.py"

        try:
            results = data["payload"]["build"]["results"]
        except KeyError:
            log.error("Couldn't find job results")
            return

        # For each request, get the taskId and runId
        for brid in request_ids[0]:
            try:
                task = self.bbb_db.getTaskFromBuildRequest(brid)
                taskid = task.taskId
                runid = int(task.runId)
            except TaskNotFound:
                log.debug("Task not found for brid %s, nothing to do.", brid)
                continue

            log.debug("brid %i : taskId %s : runId %i", brid, taskid, runid)

            # Attach properties as artifacts
            log.info("Attaching properties to task %s", taskid)
            expires = arrow.now().replace(weeks=1).isoformat()
            createJsonArtifact(self.tc_queue, taskid, runid, "properties.json", properties, expires)

            log.info("Buildbot results are %s", results)
            if results == SUCCESS:
                log.info("Marking task %s as completed", taskid)
                self.tc_queue.reportCompleted(taskid, runid)
                self.bbb_db.deleteBuildRequest(brid)
            # Eventually we probably need to set something different here.
            elif results in (WARNINGS, FAILURE):
                log.info("Marking task %s as failed", taskid)
                self.tc_queue.reportFailed(taskid, runid)
                self.bbb_db.deleteBuildRequest(brid)
            # Should never be set for builds, but just in case...
            elif results == SKIPPED:
                pass
            elif results == EXCEPTION:
                log.info("Marking task %s as malformed payload exception", taskid)
                self.tc_queue.reportException(taskid, runid, {"reason": "malformed-payload"})
                self.bbb_db.deleteBuildRequest(brid)
            elif results == RETRY:
                log.info("Marking task %s as malformed payload exception and rerunning", taskid)
                self.tc_queue.reportException(taskid, runid, {"reason": "malformed-payload"})
                self.tc_queue.rerunTask(taskid)
            elif results == CANCELLED:
                log.info("Marking task %s as cancelled", taskid)
                self.tc_queue.cancelTask(taskid)
                self.bbb_db.deleteBuildRequest(brid)


class Reflector(ServiceBase):
    """Reflects Task state into Taskcluster based on the state of the
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
        self.running = True
        while self.running:
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
                    result = self.tc_queue.reclaimTask(t.taskId, int(t.runId))
                    # Update our own db with the new claim time.
                    self.bbb_db.updateTakenUntil(t.buildrequestId, parseDateString(result["takenUntil"]))
                    log.info("Task %s now takenUntil %s", t.taskId, result['takenUntil'])
                except TaskclusterRestFailure, e:
                    if e.superExc.response.status_code == 409:
                        # Conflict; it's expired
                        log.exception("couldn't reclaim task %s: HTTP 409; deleting", t.taskId)
                        # TODO: probably should cancel the job in buildbot?
                        self.bbb_db.deleteBuildRequest(t.buildrequestId)
                    else:
                        log.error("Couldn't reclaim task: %s", e.superExc)


class TCListener(ListenerService):
    """Listens for messages from Taskcluster and responds appropriately.
    Currently handles the following types of events:
     * Task pending (exchange/taskcluster-queue/v1/task-pending)
     * Task cancelled (exchange/taskcluster-queue/v1/task-exception, with appropriate reason)

    Because ListenerService uses MozillaPulse, which only supports listening on
    a single exchange, one instance of this class is required for each exchange
    that needs to be watched."""

    def __init__(self, pulse_queue_basename, pulse_exchange_basename, worker_type,
                 allowed_builders=(), *args, **kwargs):
        self.allowed_builders = allowed_builders
        events = (
            ListenerServiceEvent(
                queue_name="%s/task-pending" % pulse_queue_basename,
                exchange="%s/task-pending" % pulse_exchange_basename,
                routing_key="*.*.*.*.*.*.%s.#" % worker_type,
                callback=self.handlePending,
            ),
            ListenerServiceEvent(
                queue_name="%s/task-exception" % pulse_queue_basename,
                exchange="%s/task-exception" % pulse_exchange_basename,
                routing_key="*.*.*.*.*.*.%s.#" % worker_type,
                callback=self.handleException,
            ),
        )
        super(TCListener, self).__init__(*args, events=events, **kwargs)

    def handlePending(self, data, msg):
        """When a Task becomes pending in Taskcluster it may be because the
        Task was just created, or a new Run for an existing Task was created.
        In the case of the former, this method creates a new BuildRequest in
        Buildbot and creates a new row in the BBB database to track the task.
        In the case of the latter, this method updates the existing row in the
        BBB database to start tracking the new Run."""

        msg.ack()
        taskid = data["status"]["taskId"]
        runid = data["status"]["runs"][-1]["runId"]

        tc_task = self.tc_queue.task(taskid)
        our_task = self.bbb_db.getTask(taskid)

        # If the buildername in the payload of the Task doesn't match any of
        # allowed patterns, we can't do anything!
        buildername = tc_task["payload"].get("buildername")
        for allowed in self.allowed_builders:
            if re.match(allowed, buildername):
                log.debug("Builder %s matches an allowed pattern", buildername)
                break
        else:
            log.info("Builder %s does not match any pattern, rejecting it", buildername)
            # malformed-payload is the most accurate TC status for this situation
            # but we can't use reportException for cancelling - so this will show
            # up as "cancelled" on TC.
            self.tc_queue.cancelTask(taskid)
            # If this Task is already in our database, we should delete it
            # because the Task has been cancelled.
            if our_task:
                # TODO: Should we kill the running Build?
                self.bbb_db.deleteBuildRequest(our_task.buildrequestId)
            return

        # If the task already exists in the BBB database we just need to
        # update our runId. If we created a new BuildRequest for it we'd end
        # up with an extra Build.
        if our_task:
            self.bbb_db.updateRunId(our_task.buildrequestId, runid)
        # If the task doesn't exist we need to insert it into our database.
        # We don't want to claim it yet though, because that will mark the task
        # as running. The BuildbotListener will take care of that when a slave
        # actually picks up the job.
        else:
            brid = self.buildbot_db.injectTask(taskid, runid, tc_task)
            self.bbb_db.createTask(taskid, runid, brid, parseDateString(tc_task["created"]))

    def handleException(self, data, msg):
        # TODO: implement me
        msg.ack()
        pass
