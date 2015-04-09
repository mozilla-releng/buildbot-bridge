from ..servicebase import ListenerService

import logging
log = logging.getLogger(__name__)


class TCListener(ListenerService):
    def __init__(self, *args, **kwargs):
        self.eventHandlers = {
            "task-pending": self.handlePending,
            "task-exception": self.handleCancellation,
        }

    def getEvent(self, exchange):
        return exchange.split("/")[-1].replace("-", "_")

    def receivedMessage(self, data, msg):
        log.info("Received message on %s", data["_meta"]["routing_key"])
        log.debug("Got %s %s", data, msg)

        taskId = data["status"]["taskId"]
        runId = data["status"]["runs"][-1]["runId"]

        event = data["exchange"].split("/")[-1]
        log.info("Handling event: %s", event)
        self.eventHandlers(event)(taskId, runId)
        # TODO: Should we ack here even if there was an exception? Retrying
        # the same message over and over again may not work.
        msg.ack()

    def handlePending(self, taskId, runId):
        ourTask = self.bbb_db.getTask(taskId)
        # If the task already exists in the bridge database we just need to
        # update our runId. If we created a new BuildRequest for it we'd end
        # up with an extra Build.
        # If we already know about this task, it means that this is 
        if ourTask:
            self.bbb_db.updateRunId(ourTask.buildrequestId, runId)
        # If the task doesn't exist we need to insert it into our database.
        # We don't want to claim it yet though, because that will mark the task
        # as running. The BuildbotListener will take care of that when a slave
        # actually picks up the job.
        else:
            tcTask = self.tc_queue.task(taskId)
            brid = self.buildbot_db.injectTask(taskId, tcTask)
            self.bbb_db.createTask(taskId, runId, brid, tcTask["created"])

    def handleCancellation(self, taskId, runId):
        # TODO: implement me
        pass
