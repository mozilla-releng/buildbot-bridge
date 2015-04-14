import arrow

from ..servicebase import ListenerService
from ..tcutils import createJsonArtifact

import logging
log = logging.getLogger(__name__)


# Buildbot status'- these must match http://mxr.mozilla.org/build/source/buildbot/master/buildbot/status/builder.py#25
SUCCESS, WARNINGS, FAILURE, SKIPPED, EXCEPTION, RETRY, CANCELLED = range(7)

class BuildbotListener(ListenerService):
    def __init__(self, tc_worker_group, tc_worker_id, *args, **kwargs):
        self.tc_worker_group = tc_worker_group
        self.tc_worker_id = tc_worker_id
        event_handlers = {
            "started": self.handleStarted,
            "log_uploaded": self.handleFinished,
        }

        super(BuildbotListener, self).__init__(*args, event_handlers=event_handlers, **kwargs)

    def getEvent(self, data, msg):
        return msg.delivery_info["routing_key"].split(".")[-1]

    def handleStarted(self, data, msg):
        # TODO: Error handling?
        buildnumber = data["payload"]["build"]["number"]
        for brid in self.buildbot_db.getBuildRequests(buildnumber):
            brid = brid[0]
            task = self.bbb_db.getTaskFromBuildRequest(brid)
            log.info("Claiming %s", task.taskId)
            claim = self.tc_queue.claimTask(task.taskId, task.runId, {
                "workerGroup": self.tc_worker_group,
                "workerId": self.tc_worker_id,
            })
            log.debug("Got claim: %s", claim)
            self.bbb_db.updateTakenUntil(brid, claim["takenUntil"])

    def handleFinished(self, data, msg):
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
                runid = task.runId
            except ValueError:
                log.error("Couldn't find task for %i", brid)
                continue

            log.debug("brid %i : taskId %s : runId %i", brid, taskid, runid)

            # Attach properties as artifacts
            log.info("Attaching properties to task %s", taskid)
            expires = arrow.now().replace(weeks=1).isoformat()
            createJsonArtifact(self.tc_queue, taskid, runid, "properties.json", properties, expires)

            log.info("Buildbot results are %s", results)
            if results == SUCCESS:
                log.info("Marking task %s as completed", taskid)
                self.tc_queue.reportCompleted(taskid, runid, {"success": True})
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

