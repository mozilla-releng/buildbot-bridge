from mock import Mock, patch
import unittest

from arrow import Arrow

from ..dbutils import makeSchedulerDb
from ...services.tclistener import TCListener
from ...tcutils import makeTaskId


class TestTCListener(unittest.TestCase):
    def setUp(self):
        self.tclistener = TCListener(
            bbb_db="sqlite:///:memory:",
            buildbot_db="sqlite:///:memory:",
            tc_credentials={
                "credentials": {
                    "clientId": "fake",
                    "accessToken": "fake",
                }
            },
            pulse_user="fake",
            pulse_password="fake",
            exchange="fake",
            topic="fake",
        )
        makeSchedulerDb(self.tclistener.buildbot_db.db)
        # Replace the TaskCluster Queue object with a Mock because we never
        # want to actually talk to TC, just check if the calls that would've
        # been made are correct
        self.tclistener.tc_queue = Mock()
        self.tasks = self.tclistener.bbb_db.tasks_table
        self.buildbot_db = self.tclistener.buildbot_db = Mock()

    @patch("arrow.now")
    def testHandlePendingNewTask(self, fake_now):
        taskId = makeTaskId()
        data = {"status": {
            "taskId": taskId,
            "runs": [
                {"runId": 0},
            ],
        }}

        processedDate = fake_now.return_value = Arrow(2015, 4, 1)
        self.tclistener.tc_queue.task.return_value = {"created": 50}
        self.buildbot_db.injectTask.return_value = 1
        self.tclistener.handlePending(data, {})

        self.assertEquals(self.tclistener.tc_queue.task.call_count, 1)
        self.assertEquals(self.buildbot_db.injectTask.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEquals(len(bbb_state), 1)
        self.assertEquals(bbb_state[0].buildrequestId, 1)
        self.assertEquals(bbb_state[0].taskId, taskId)
        self.assertEquals(bbb_state[0].runId, 0)
        self.assertEquals(bbb_state[0].createdDate, 50)
        self.assertEquals(bbb_state[0].processedDate, processedDate.timestamp)
        self.assertEquals(bbb_state[0].takenUntil, None)

    def testHandlePendingUpdateRunId(self):
        taskId = makeTaskId()
        self.tasks.insert().execute(
            buildRequestId=1,
            taskId=taskId,
            runId=0,
            createdDate=23,
            processedDate=34,
            takenUntil=None
        )
        data = {"status": {
            "taskId": taskId,
            "runs": [
                {"runId": 0},
                {"runId": 1},
            ],
        }}

        self.tclistener.handlePending(data, {})
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEquals(len(bbb_state), 1)
        self.assertEquals(bbb_state[0].buildrequestId, 1)
        self.assertEquals(bbb_state[0].taskId, taskId)
        self.assertEquals(bbb_state[0].runId, 1)
        self.assertEquals(bbb_state[0].createdDate, 23)
        self.assertEquals(bbb_state[0].processedDate, 34)
        self.assertEquals(bbb_state[0].takenUntil, None)
