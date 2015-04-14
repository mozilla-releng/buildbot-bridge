from ..servicebase import ListenerService

import logging
log = logging.getLogger(__name__)


class TCListener(ListenerService):
    """Listens for messages from Taskcluster and responds appropriately.
    Currently handles the following types of events:
     * Task pending (exchange/taskcluster-queue/v1/task-pending)
     * Task cancelled (exchange/taskcluster-queue/v1/task-exception, with appropriate reason)

    Because ListenerService uses MozillaPulse, which only supports listening on
    a single exchange, one instance of this class is required for each exchange
    that needs to be watched."""

    def __init__(self, *args, **kwargs):
        event_handlers = {
            "task-pending": self.handlePending,
            "task-exception": self.handleCancellation,
        }
        super(TCListener, self).__init__(*args, event_handlers=event_handlers, **kwargs)

    def getEvent(self, data, msg):
        """Events from Taskcluster are deriverd from the last segment of the
        exchange name."""
        return msg.delivery_info["exchange"].split("/")[-1]

    def handlePending(self, data, msg):
        """When a Task becomes pending in Taskcluster it may be because the
        Task was just created, or a new Run for an existing Task was created.
        In the case of the former, this method creates a new BuildRequest in
        Buildbot and creates a new row in the BBB database to track the task.
        In the case of the latter, this method updates the existing row in the
        BBB database to start tracking the new Run."""

        taskid = data["status"]["taskId"]
        runid = data["status"]["runs"][-1]["runId"]

        our_task = self.bbb_db.getTask(taskid)
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
            tc_task = self.tc_queue.task(taskid)
            brid = self.buildbot_db.injectTask(taskid, tc_task)
            self.bbb_db.createTask(taskid, runid, brid, tc_task["created"])

    def handleCancellation(self, taskid, runid):
        # TODO: implement me
        pass
