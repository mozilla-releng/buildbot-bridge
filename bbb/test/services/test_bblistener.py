from mock import Mock, patch
import unittest

import sqlalchemy as sa

from ..dbutils import makeSchedulerDb
from ...services.bblistener import BuildbotListener, SUCCESS, WARNINGS, \
    FAILURE, EXCEPTION, RETRY, CANCELLED
from ...tcutils import makeTaskId


class TestBuildbotListener(unittest.TestCase):
    def setUp(self):
        self.bblistener = BuildbotListener(
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
            tcWorkerGroup="workwork",
            tcWorkerId="workwork",
        )
        makeSchedulerDb(self.bblistener.buildbot_db.db)
        # Replace the TaskCluster Queue object with a Mock because we never
        # want to actually talk to TC, just check if the calls that would've
        # been made are correct
        self.bblistener.tc_queue = Mock()
        self.tasks = self.bblistener.bbb_db.tasks_table
        self.buildbot_db = self.bblistener.buildbot_db.db

    def testHandleStartedOneBuildRequest(self):
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at)
    VALUES (4, 0, "foo", 50);"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO builds
    (id, number, brid, start_time)
    VALUES (0, 2, 4, 60);"""))
        self.tasks.insert().execute(
            buildrequestId=4,
            taskId=makeTaskId(),
            runId=0,
            createdDate=50,
            processedDate=60,
            takenUntil=None
        )
        data = {"payload": {"build": {"number": 2}}}
        self.bblistener.tc_queue.claimTask.return_value = {"takenUntil": 100}
        self.bblistener.handleStarted(data, {})

        self.assertEquals(self.bblistener.tc_queue.claimTask.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEquals(len(bbb_state), 1)
        self.assertEquals(bbb_state[0].takenUntil, 100)

    def testHandleStartedMultipleBuildRequests(self):
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at)
    VALUES (2, 0, "foo", 20), (3, 0, "foo", 30);"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO builds
    (id, number, brid, start_time)
    VALUES (0, 3, 2, 40), (1, 3, 3, 40);"""))
        self.tasks.insert().execute(
            buildrequestId=2,
            taskId=makeTaskId(),
            runId=0,
            createdDate=20,
            processedDate=35,
            takenUntil=None
        )
        self.tasks.insert().execute(
            buildrequestId=3,
            taskId=makeTaskId(),
            runId=0,
            createdDate=30,
            processedDate=35,
            takenUntil=None
        )
        data = {"payload": {"build": {"number": 3}}}
        self.bblistener.tc_queue.claimTask.return_value = {"takenUntil": 80}
        self.bblistener.handleStarted(data, {})

        self.assertEquals(self.bblistener.tc_queue.claimTask.call_count, 2)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEquals(len(bbb_state), 2)
        self.assertEquals(bbb_state[0].takenUntil, 80)
        self.assertEquals(bbb_state[1].takenUntil, 80)

    # TODO: tests for some error cases, like failing to contact TC, or maybe db operations failing

    # Passing new=Mock prevents patch from passing the patched object,
    # which we have no need of, to this method.
    @patch("requests.put", new=Mock)
    def _handleFinishedTest(self, results):
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at, complete, complete_at, results)
    VALUES (1, 0, "foo", 5, 1, 30, 0);"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO builds
    (id, number, brid, start_time, finish_time)
    VALUES (0, 0, 1, 15, 30);"""))
        self.tasks.insert().execute(
            buildrequestId=1,
            taskId=makeTaskId(),
            runId=0,
            createdDate=5,
            processedDate=10,
            takenUntil=100,
        )

        data = {"payload": {"build": {
            "properties": (
                ("request_ids", (1,), "postrun.py"),
            ),
            "results": results,
        }}}
        self.bblistener.tc_queue.createArtifact.return_value = {
            "storageType": "s3",
            "putUrl": "http://foo.com",
        }
        self.bblistener.handleFinished(data, {})

    def testHandleFinishedSuccess(self):
        self._handleFinishedTest(SUCCESS)

        self.assertEquals(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEquals(self.bblistener.tc_queue.reportCompleted.call_count, 1)
        # Build and Task are done - should be deleted from our db.
        self.assertEquals(self.tasks.count().execute().fetchone()[0], 0)

    def testHandleFinishedWarnings(self):
        self._handleFinishedTest(WARNINGS)

        self.assertEquals(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEquals(self.bblistener.tc_queue.reportFailed.call_count, 1)
        # Build and Task are done - should be deleted from our db.
        self.assertEquals(self.tasks.count().execute().fetchone()[0], 0)

    def testHandleFinishedFailed(self):
        self._handleFinishedTest(FAILURE)

        self.assertEquals(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEquals(self.bblistener.tc_queue.reportFailed.call_count, 1)
        # Build and Task are done - should be deleted from our db.
        self.assertEquals(self.tasks.count().execute().fetchone()[0], 0)

    def testHandleFinishedException(self):
        self._handleFinishedTest(EXCEPTION)

        self.assertEquals(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEquals(self.bblistener.tc_queue.reportException.call_count, 1)
        # Build and Task are done - should be deleted from our db.
        self.assertEquals(self.tasks.count().execute().fetchone()[0], 0)

    def testHandleFinishedRetry(self):
        self._handleFinishedTest(RETRY)

        self.assertEquals(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEquals(self.bblistener.tc_queue.reportException.call_count, 1)
        # Unlike other status, the BuildRequest is NOT finished if a RETRY is hit
        self.assertEquals(self.tasks.count().execute().fetchone()[0], 1)

    def testHandleFinishedCancelled(self):
        self._handleFinishedTest(CANCELLED)

        self.assertEquals(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEquals(self.bblistener.tc_queue.cancelTask.call_count, 1)
        # Build and Task are done - should be deleted from our db.
        self.assertEquals(self.tasks.count().execute().fetchone()[0], 0)
