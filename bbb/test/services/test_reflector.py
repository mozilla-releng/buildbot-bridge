from mock import Mock
import unittest

import sqlalchemy as sa

from ..dbutils import makeSchedulerDb
from ...services.reflector import Reflector
from ...tcutils import makeTaskId


class TestReflector(unittest.TestCase):
    def setUp(self):
        self.reflector = Reflector(
            bbb_db="sqlite:///:memory:",
            buildbot_db="sqlite:///:memory:",
            tc_config={
                "credentials": {
                    "clientId": "fake",
                    "accessToken": "fake",
                }
            },
            interval=5,
        )
        makeSchedulerDb(self.reflector.buildbot_db.db)
        # Replace the TaskCluster Queue object with a Mock because we never
        # want to actually talk to TC, just check if the calls that would've
        # been made are correct
        self.reflector.tc_queue = Mock()
        self.tasks = self.reflector.bbb_db.tasks_table
        self.buildbot_db = self.reflector.buildbot_db.db

    def testReclaimRunningTask(self):
        taskid = makeTaskId()
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at)
    VALUES (2, 0, "foo", 15);
"""))
        self.tasks.insert().execute(
            buildrequestId=2,
            taskId=taskid,
            runId=0,
            createdDate=12,
            processedDate=17,
            takenUntil=200,
        )

        self.reflector.tc_queue.reclaimTask.return_value = {"takenUntil": 300}
        self.reflector.reflectTasks()

        self.assertEquals(self.reflector.tc_queue.reclaimTask.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEquals(len(bbb_state), 1)
        self.assertEquals(bbb_state[0].takenUntil, 300)

    def testPendingTask(self):
        taskid = makeTaskId()
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at)
    VALUES (0, 0, "foo", 20);
"""))
        self.tasks.insert().execute(
            buildrequestId=0,
            taskId=taskid,
            runId=0,
            createdDate=20,
            processedDate=25,
            takenUntil=None,
        )

        self.reflector.reflectTasks()

        # Pending tasks shouldn't have any state changed by the reflector
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEquals(len(bbb_state), 1)
        self.assertEquals(bbb_state[0].buildrequestId, 0)
        self.assertEquals(bbb_state[0].taskId, taskid)
        self.assertEquals(bbb_state[0].runId, 0)
        self.assertEquals(bbb_state[0].createdDate, 20)
        self.assertEquals(bbb_state[0].processedDate, 25)
        self.assertEquals(bbb_state[0].takenUntil, None)

    def testCancelledFromBuildbot(self):
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at, complete)
    VALUES (3, 0, "foo", 30, 1);
"""))
        self.tasks.insert().execute(
            buildrequestId=3,
            taskId=makeTaskId(),
            runId=0,
            createdDate=20,
            processedDate=25,
            takenUntil=None,
        )

        self.reflector.reflectTasks()

        # Tasks that are cancelled from Buildbot should have that reflected
        # in TC, and be removed from our DB.
        self.assertEquals(self.reflector.tc_queue.cancelTask.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEquals(len(bbb_state), 0)