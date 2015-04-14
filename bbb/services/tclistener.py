from ..servicebase import ListenerService

import logging
log = logging.getLogger(__name__)


class TCListener(ListenerService):
    def __init__(self, *args, **kwargs):
        event_handlers = {
            "task-pending": self.handlePending,
            "task-exception": self.handleCancellation,
        }
        super(TCListener, self).__init__(*args, event_handlers=event_handlers, **kwargs)

    def getEvent(self, data, msg):
        return msg.delivery_info["exchange"].split("/")[-1]

    def handlePending(self, data, msg):
        taskid = data["status"]["taskId"]
        runid = data["status"]["runs"][-1]["runId"]

        our_task = self.bbb_db.getTask(taskid)
        # If the task already exists in the bridge database we just need to
        # update our runId. If we created a new BuildRequest for it we'd end
        # up with an extra Build.
        # If we already know about this task, it means that this is 
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
