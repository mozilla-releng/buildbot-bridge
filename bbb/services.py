from os import path
import re
import time

import arrow
from jsonschema import Draft4Validator
from taskcluster import scope_match
from taskcluster.exceptions import TaskclusterRestFailure
from requests.exceptions import RequestException
import yaml

from . import schemas
from .servicebase import ListenerService, ServiceBase, ListenerServiceEvent, SelfserveClient, TaskNotFound
from .tcutils import createJsonArtifact
from .timeutils import parseDateString

import logging
log = logging.getLogger(__name__)


# Buildbot status'- these must match http://mxr.mozilla.org/build/source/buildbot/master/buildbot/status/builder.py#25
SUCCESS, WARNINGS, FAILURE, SKIPPED, EXCEPTION, RETRY, CANCELLED = range(7)


def matches_pattern(s, patterns):
    """Returns True if "s" matches any of the given patterns. False otherwise."""
    for pat in patterns:
        if re.match(pat, s):
            return True
    return False


class BuildbotListener(ListenerService):
    """Listens for messages from Buildbot and responds appropriately.
    Currently handles the following types of events:
     * Build started (build.$builder.$buildnum.started)
     * Build finished (build.$builder.$buildnum.log_uploaded)
    """
    def __init__(self, tc_worker_group, tc_worker_id, pulse_queue_basename, pulse_exchange,
                 *args, **kwargs):
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
                log.debug("Task not found for brid %s (%s), nothing to do.", brid, buildername)
                continue
            log.info("Claiming %s", task.taskId)
            try:
                # Taskcluster requires runId to be an int, but it comes to us as a long.
                claim = self.tc_queue.claimTask(task.taskId, int(task.runId), {
                    "workerGroup": self.tc_worker_group,
                    "workerId": self.tc_worker_id,
                })
            except TaskclusterRestFailure as e:
                log.info("Cannot claim task %s run %s, skipping...", task.taskId, int(task.runId))
                log.error("status_code: %s body: %s", e.status_code, e.body)
                continue
            # Once we've claimed the Task we're past the point of no return.
            # Even if something goes wrong after this, we wouldn't want the
            # message to be processed again.
            if not msg.acknowledged:
                msg.ack()
            log.debug("Got claim: %s", claim)
            self.bbb_db.updateTakenUntil(brid, parseDateString(claim["takenUntil"]))

        # If everything went well and the message hasn't been acked, do it. This could
        # happen if the "WEIRD" conditions is hit in every iteration of the loop
        if not msg.acknowledged:
            msg.ack()

    def handleFinished(self, data, msg):
        """When a Build finishes in Buildbot we pass along the final state of
        it to the Task(s) associated with it in Taskcluster.

        It's important to note that we track the "build.foo.log_uploaded" event
        instead of "build.foo.finished". This is because only the former
        contains all of the BuildRequest ids that the Build satisfied.
        """
        log.debug("Handling finished event: %s", data)

        # Get the request_ids from the properties
        try:
            properties = dict((key, (value, source)) for (key, value, source) in data["payload"]["build"]["properties"])
        except KeyError:
            log.error("Couldn't parse job properties from %s , can't proceed", data["payload"]["build"]["properties"])
            msg.ack()
            return

        request_ids = properties.get("request_ids")
        if not request_ids:
            log.error("Couldn't get request ids from %s, can't proceed", data)
            msg.ack()
            return

        # Sanity check
        if request_ids[1] != "postrun.py":
            log.error("WEIRD: Finished event doesn't appear to come from postrun.py, bailing...")
            msg.ack()
            return

        try:
            results = data["payload"]["build"]["results"]
        except KeyError:
            log.error("Couldn't find job results from %s, can't proceed", data["payload"]["build"]["results"])
            msg.ack()
            return

        # For each request, get the taskId and runId
        for brid in request_ids[0]:
            try:
                self._handleFinishedRequest(brid, properties, results)
            except TaskclusterRestFailure as e:
                # the exception object has some non-standard attributes which
                # won't show up in the default stacktrace
                log.error("status_code: %s body: %s", e.status_code, e.body)
            except Exception:
                log.exception("Failed to handle %s", brid)
            finally:
                if not msg.acknowledged:
                    msg.ack()

    def _handleFinishedRequest(self, brid, properties, results):
        try:
            task = self.bbb_db.getTaskFromBuildRequest(brid)
            taskid = task.taskId
            runid = int(task.runId)
        except TaskNotFound:
            log.debug("Task not found for brid %s, nothing to do.", brid)
            return

        log.info("Handling finished task %s run %s brid %s", taskid, runid,
                 brid)
        # Try to claim the task in case if the "started" event comes after
        try:
            self.tc_queue.claimTask(taskid, runid, {
                "workerGroup": self.tc_worker_group,
                "workerId": self.tc_worker_id,
            })
            log.info("Task %s run %s claimed unexpectedly", taskid, runid)
        except TaskclusterRestFailure:
            log.debug("Cannot claim task %s run %s, assuming it is claimed already", taskid, runid)

        # Attach properties as artifacts
        log.info("Attaching properties to task %s", taskid)
        try:
            # Our artifact must expire at or before the task's expiration
            expires = self.tc_queue.task(taskid)['expires']
            createJsonArtifact(self.tc_queue, taskid, runid, "public/properties.json", properties, expires)
        except TaskclusterRestFailure as e:
            log.exception("Caught exception when creating an artifact for %s (Task is probably already completed), not retrying...", taskid)
            # the exception object has some non-standard attributes which
            # won't show up in the default stacktrace
            log.error("status_code: %s body: %s", e.status_code, e.body)

        # Once we've updated Taskcluster with the resolution we're past the
        # point of no return. Even if something goes wrong afterwards we
        # don't want the message to be processed again because Taskcluster
        # will end up returning errors.
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
            log.info("WEIRD: Build result is SKIPPED, this shouldn't be possible...")
        elif results == EXCEPTION:
            log.info("Marking task %s as malformed payload exception", taskid)
            self.tc_queue.reportException(taskid, runid, {"reason": "malformed-payload"})
            self.bbb_db.deleteBuildRequest(brid)
        elif results == RETRY:
            log.info("Marking task %s as malformed payload exception and rerunning", taskid)
            # TODO: can we use worker-shutdown instead of rerunTask here? We used malformed-payload
            # before because TCListener didn't know how to skip build request creation for
            # reruns....
            # using worker-shutdown would probably be better for treeherder, because
            # the buildbot and TC states would line up better.
            self.tc_queue.reportException(taskid, runid, {"reason": "malformed-payload"})
            # TODO: runid might be wrong for the rerun for a period of time because we don't update it
            # until the TCListener gets the task-pending event. Maybe we should update it here too/instead?
            self.tc_queue.rerunTask(taskid)
        elif results == CANCELLED:
            # We could end up in this block for two different reasons:
            # 1) Someone cancels the job through Buildbot. In this case
            #    cancelTask is needed to reflect that state on Taskcluster.
            # 2) Someone cancels the job through Taskcluster. In this case
            #    the TCListener received that event and cancelled the Build
            #    in Buildbot. When that Build finished it still got picked
            #    up by us, and now we're here. The Buildbot and Taskcluster
            #    states are already in sync, so we don't need to do anything.
            # 3) The Task exceeds its deadline, and Taskcluster resolves
            #    it with a deadline-exceeded exception. In this case, the
            #    TCListener receives that event and cancels the running
            #    Build. That events gets picked up us and now we're here.
            #    This is very similar to the cancellation case, except that
            #    there's Buildbot equivalent to "deadline-exceeded", so we
            #    just leave things be with Buildbot calling it CANCELLED
            #    and Taskcluster calling it deadline-exceeded.
            #
            # In all cases we need to delete the BuildRequest from our own
            # database.
            log.info("Marking task %s as cancelled", taskid)
            status = self.tc_queue.status(taskid)["status"]["runs"][runid]
            # If the Task is still running on Taskcluster, cancel it.
            if status.get("state") == "running":
                self.tc_queue.cancelTask(taskid)
            self.bbb_db.deleteBuildRequest(brid)
        else:
            log.info("WEIRD: Got unknown results %s, ignoring it...", results)


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
    def __init__(self, interval, selfserve_url, *args, **kwargs):
        super(Reflector, self).__init__(*args, **kwargs)
        self.interval = interval
        self.selfserve = SelfserveClient(selfserve_url)

    def start(self):
        log.info("Starting reflector")
        self.running = True
        while self.running:
            self.reflectTasks()
            time.sleep(self.interval)

    def _handle_taskcluster_exceptions(self, t, exc):
        status_code = exc.superExc.response.status_code

        if status_code == 409:
            log.warn("Deadline exceeded for task %s run %s, cancelling it",
                     t.taskId, t.runId)
            branch = self.buildbot_db.getBranch(t.buildrequestId).split("/")[-1]
            try:
                for id_ in self.buildbot_db.getBuildIds(t.buildrequestId):
                        self.selfserve.cancelBuild(branch, id_)
                # delete from the DB only if all cancel requests pass
                self.bbb_db.deleteBuildRequest(t.buildrequestId)

            except RequestException:
                log.exception(
                    "Failed to cancel task %s run %s build request ID %s",
                    t.taskId, t.runId, t.buildrequestId)

        elif status_code == 404:
            # Expired tasks are removed from the TC DB
            log.warn("Cannot find task %s run %s in TC, removing it", t.taskId,
                     t.runId)
            self.bbb_db.deleteBuildRequest(t.buildrequestId)

        elif status_code == 403:
            # Bug 1270785. Claiming a completed task returns 403.
            log.warn("Cannot modify task %s run %s in TC, removing it",
                     t.taskId, t.runId)
            self.bbb_db.deleteBuildRequest(t.buildrequestId)
        else:
            log.warn("Unhandled TC status code %s for task %s run %s",
                     status_code, t.taskId, t.runId)

    def reflectTasks(self):
        tasks = list(self.bbb_db.tasks)
        log.info("%s tasks to reflect", len(tasks))
        for i, t in enumerate(tasks):
            log.info("Processing task: %s (%s/%s)", t.taskId, i+1, len(tasks))
            try:
                self._reflectTask(t)
            except TaskclusterRestFailure, e:
                log.exception("Taskcluster exception")
                self._handle_taskcluster_exceptions(t, e)
            except:
                log.exception("Failed to reflect task %s run %s", t.taskId,
                              t.runId)

    def _reflectTask(self, t):
        complete = self.buildbot_db.isBuildRequestComplete(t.buildrequestId)
        nBuilds = self.buildbot_db.getBuildsCount(t.buildrequestId)
        log.debug("Task info: %s", t)
        if complete:
            log.debug("BuildRequest %s is complete", t.buildrequestId)
        else:
            log.debug("BuildRequest %s is NOT complete", t.buildrequestId)

        # If takenUntil isn't set, this task has either never been claimed
        # or got cancelled.
        if not t.takenUntil:
            # If the buildrequest is showing complete, there is a
            # possibility, that the build was completed before takenUntil
            # was updated by BBListener. To avoid this we can try to avoid
            # processing the buildrequest for 5 minutes.
            if arrow.now() < arrow.get(t.processedDate).replace(minutes=5):
                log.debug(
                    "Not cancelling task %s brid %s because it's within 5 minutes after completion.",
                    t.taskId, t.buildrequestId)
                return

            # If the buildrequest is showing complete, it was cancelled
            # before it ever started, so we need to pass that along to
            # taskcluster. Ideally, we'd watch Pulse for notification of
            # this, but our version of Buildbot has a bug that causes it
            # not to send those messages.
            # TODO: This can race with build started events. If the reflector runs
            # before the build started event is processed we'll cancel tasks that
            # are actually running. FIXME!!!!
            if complete:
                log.info("BuildRequest disappeared before starting, cancelling task")
                try:
                    self.tc_queue.cancelTask(t.taskId)
                except TaskclusterRestFailure as e:
                    log.error("status_code: %s body: %s", e.status_code, e.body)
                self.bbb_db.deleteBuildRequest(t.buildrequestId)
                return
            # Otherwise we're just waiting for it to start, nothing to do
            # because it hasn't been claimed at all yet.
            else:
                log.info("Build hasn't started yet, nothing to do")
                return
        # BuildRequest is complete, but hasn't been reaped yet. We should
        # continue claiming this task for now, but the BBListener should
        # come along and get rid of it soon.
        elif complete:
            log.info("BuildRequest %s is done for task %s, run %s. BBListener should process it soon, reclaiming in the meantime",
                     t.buildrequestId, t.taskId, t.runId)
            self.tc_queue.reclaimTask(t.taskId, int(t.runId))
            return

        # Build is running, which means it has already been claimed.
        # We need to renew the claim to make sure Taskcluster doesn't
        # expire it on us.
        else:
            if nBuilds > t.runId + 1:
                log.warn("Too many buildbot builds? runId is %i but we have %i builds", t.runId, nBuilds)

            log.debug("BuildRequest %s is in progress", t.buildrequestId)
            # Reclaiming should only happen if we're less than 5 minutes
            # away from the current claim expiring. Without this, every
            # instance of the Reflector will reclaim each time it runs,
            # which is very spammy in the logs and adds unnecessary load to
            # Taskcluster.
            if arrow.now() > arrow.get(t.takenUntil).replace(minutes=-5):
                log.info("Claim for BuildRequest %s will expire in less than 5min, reclaiming", t.buildrequestId)
                result = self.tc_queue.reclaimTask(t.taskId, int(t.runId))
                # Update our own db with the new claim time.
                self.bbb_db.updateTakenUntil(t.buildrequestId, parseDateString(result["takenUntil"]))
                log.info("Task %s now takenUntil %s", t.taskId, result['takenUntil'])


class TCListener(ListenerService):
    """Listens for messages from Taskcluster and responds appropriately.
    Currently handles the following types of events:
     * Task pending (exchange/taskcluster-queue/v1/task-pending)
     * Task cancelled (exchange/taskcluster-queue/v1/task-exception, with appropriate reason)

    Because ListenerService uses MozillaPulse, which only supports listening on
    a single exchange, one instance of this class is required for each exchange
    that needs to be watched."""

    def __init__(self, pulse_queue_basename, pulse_exchange_basename, worker_type,
                 provisioner_id, worker_group, worker_id, selfserve_url,
                 restricted_builders=(), ignored_builders=(), *args, **kwargs):
        self.restricted_builders = restricted_builders
        self.ignored_builders = ignored_builders
        self.worker_group = worker_group
        self.worker_id = worker_id
        self.selfserve = SelfserveClient(selfserve_url)
        self.payload_schema = Draft4Validator(
            yaml.load(open(path.join(path.dirname(schemas.__file__), "payload.yml")))
        )
        events = (
            ListenerServiceEvent(
                queue_name="%s/task-pending" % pulse_queue_basename,
                exchange="%s/task-pending" % pulse_exchange_basename,
                routing_key="*.*.*.*.*.%s.%s.#" % (provisioner_id, worker_type),
                callback=self.handlePending,
            ),
            ListenerServiceEvent(
                queue_name="%s/task-exception" % pulse_queue_basename,
                exchange="%s/task-exception" % pulse_exchange_basename,
                routing_key="*.*.*.*.*.%s.%s.#" % (provisioner_id, worker_type),
                callback=self.handleException,
            ),
        )
        super(TCListener, self).__init__(*args, events=events, **kwargs)

    def _isAuthorized(self, buildername, scopes):
        """Tests to see if the builder given is restricted, and if so, whether
        or not the scopes given are authorized to use it. Builders that do
        not match the overall restricted builder patterns do not require any
        scopes. Builders that do must have a
        project:releng:buildbot-bridge:builder-name: scope that matches the
        builder name given."""
        requiredscopes = [
            ["buildbot-bridge:builder-name:{}".format(buildername)],
            ["project:releng:buildbot-bridge:builder-name:{}".format(buildername)]
        ]

        for r in self.restricted_builders:
            # If the builder is restricted, check the scopes to see if they
            # are authorized to use it.
            if re.match(r, buildername):
                return scope_match(scopes, requiredscopes)
        # If the builder is unrestricted, no special scopes are required to
        # use it.
        else:
            return True

    def handlePending(self, data, msg):
        """When a Task becomes pending in Taskcluster it may be because the
        Task was just created, or a new Run for an existing Task was created.
        In the case of the former, this method creates a new BuildRequest in
        Buildbot and creates a new row in the BBB database to track the task.
        In the case of the latter, this method updates the existing row in the
        BBB database to start tracking the new Run."""

        log.debug("Handling task-pending event: %s", data)

        taskid = data["status"]["taskId"]
        runid = data["status"]["runs"][-1]["runId"]

        tc_task = self.tc_queue.task(taskid)
        our_task = self.bbb_db.getTask(taskid)

        buildername = tc_task["payload"].get("buildername")
        # If the builder name matches an ignored pattern, we shouldn't do
        # anything. See https://bugzilla.mozilla.org/show_bug.cgi?id=1201861
        # for additional background.
        if matches_pattern(buildername, self.ignored_builders):
            log.info("%s - Buildername %s matches an ignore pattern, doing nothing", taskid, buildername)
            msg.ack()
            return

        scopes = tc_task.get("scopes", [])
        if not self.payload_schema.is_valid(tc_task["payload"]) or not self._isAuthorized(buildername, scopes):
            log.info("%s - Payload is invalid, refusing to create BuildRequest", taskid)
            for e in self.payload_schema.iter_errors(tc_task["payload"]):
                log.debug(e.message)

            # In order to report a malformed-payload on the TAsk, we need to
            # claim it first.
            try:
                self.tc_queue.claimTask(taskid, int(runid), {
                    "workerGroup": self.worker_group,
                    "workerId": self.worker_id,
                })
                self.tc_queue.reportException(taskid, runid, {"reason": "malformed-payload"})
            except TaskclusterRestFailure as e:
                log.error("status_code: %s body: %s", e.status_code, e.body)
            msg.ack()
            # If this Task is already in our database, we should delete it
            # because the Task has been cancelled.
            if our_task:
                # TODO: Should we kill the running Build?
                self.bbb_db.deleteBuildRequest(our_task.buildrequestId)
            return

        # When Buildbot Builds end up in a RETRY state they are automatically
        # retried against the same BuildRequest. The BuildbotListener reflects
        # this into Taskcluster be calling rerunTask, which creates a new Run
        # for the same Task. In these cases, we don't want to do anything
        # except update our own runId. If we created a new BuildRequest for it
        # we'd end up with an extra Build.
        if our_task:
            log.info("%s - updating run id", taskid)
            self.bbb_db.updateRunId(our_task.buildrequestId, runid)
        # If the task doesn't exist we need to insert it into our database.
        # We don't want to claim it yet though, because that will mark the task
        # as running. The BuildbotListener will take care of that when a slave
        # actually picks up the job.
        else:
            log.info("%s - injecting task into bb", taskid)
            brid = self.buildbot_db.injectTask(taskid, runid, tc_task)
            self.bbb_db.createTask(taskid, runid, brid, parseDateString(tc_task["created"]))

        msg.ack()

    def handleException(self, data, msg):
        """Most exceptions from Taskcluster are ignoreable because they are
        caused another part of the Buildbot Bridge. However, when the reason is
        set to "canceled" this may be because a user requested cancellation
        through the Taskcluster API. For these, we need to look for and kill
        any associated Buildbot Builds or BuildRequests. Similarly,
        "deadline-exceeded" exceptions come from Taskcluster, and we need to
        reflect that state back into Buildbot by killing any associated jobs."""

        taskid = data["status"]["taskId"]
        reason = data["status"]["runs"][-1]["reasonResolved"]
        # The only reasons we care about handling are "canceled" and
        # "deadline-exceeded". Any other type of exception would've been
        # generated by another part of us, so we can assume that they were
        # already handled.
        if reason in ("canceled", "deadline-exceeded"):
            log.info("Handling Taskcluster exception (%s) for %s", reason, taskid)
            our_task = self.bbb_db.getTask(taskid)

            # If there's no Task in our database for this event it probably
            # means that someone cancelled the Build or BuildRequest in
            # Buildbot, which caused a Task exception after the BuildbotListener
            # propagated that event to Taskcluster. There's nothing to do in
            # these cases - the Buildbot and Taskcluster states are already in
            # sync.
            if not our_task:
                log.info("No task found in our database, nothing to do.")
                msg.ack()
                return
            brid = our_task.buildrequestId
            buildIds = self.buildbot_db.getBuildIds(brid)
            # The branch in the Buildbot database is the path on the hg server
            # relative to the root. Self serve needs the "short" branch name,
            # which is the last part of the path.
            branch = self.buildbot_db.getBranch(brid).split("/")[-1]

            # If there's already a Build running for the task, kill it!
            # We need to use selfserve for this because it has special magic
            # that knows how to find the buildbot master running the job and
            # hit the "stop" button on its web interface.
            if buildIds:
                # TODO: add a test for multiple build ids
                for id_ in buildIds:
                    log.info("BuildId %d found for task %s, cancelling it.", id_, taskid)
                    self.selfserve.cancelBuild(branch, id_)
            # If there's no Build running yet we can just cancel the
            # BuildRequest.
            else:
                log.info("BuildRequest found for task %s, cancelling it.", taskid)
                self.selfserve.cancelBuildRequest(branch, brid)
                # Because the Build never started there's no reason to keep
                # track of the Task any longer.
                self.bbb_db.deleteBuildRequest(brid)
            msg.ack()
            # In either case we explicitly do not want to delete the task from
            # our own database -- we still need the BuildbotListener to come
            # around and attach JSON artifacts to the Task, which it will only
            # do if the Task still exists in our DB. It will take care of
            # reaping it, too.
        else:
            log.debug("Ignoring Taskcluster Task exception for reason: %s", reason)
            msg.ack()
