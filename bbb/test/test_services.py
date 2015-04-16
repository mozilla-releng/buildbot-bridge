from mock import Mock, patch
import unittest

from arrow import Arrow
import sqlalchemy as sa

from .dbutils import makeSchedulerDb
from ..services import BuildbotListener, Reflector, TCListener, SUCCESS, \
    WARNINGS, FAILURE, EXCEPTION, RETRY, CANCELLED
from ..tcutils import makeTaskId


class TestBuildbotListener(unittest.TestCase):
    def setUp(self):
        self.bblistener = BuildbotListener(
            bbb_db="sqlite:///:memory:",
            buildbot_db="sqlite:///:memory:",
            tc_config={
                "credentials": {
                    "clientId": "fake",
                    "accessToken": "fake",
                }
            },
            pulse_host="fake",
            pulse_user="fake",
            pulse_password="fake",
            pulse_queue_basename="fake",
            pulse_exchange="fake",
            tc_worker_group="workwork",
            tc_worker_id="workwork",
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
        self.bblistener.handleStarted(data, Mock())

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
        self.bblistener.handleStarted(data, Mock())

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
        self.bblistener.handleFinished(data, Mock())

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


class TestTCListener(unittest.TestCase):
    def setUp(self):
        self.tclistener = TCListener(
            bbb_db="sqlite:///:memory:",
            buildbot_db="sqlite:///:memory:",
            tc_config={
                "credentials": {
                    "clientId": "fake",
                    "accessToken": "fake",
                }
            },
            pulse_host="fake",
            pulse_user="fake",
            pulse_password="fake",
            pulse_queue_basename="fake",
            pulse_exchange_basename="fake",
            worker_type="fake",
            allowed_builders=(
                ".*good.*",
            ),
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
        taskid = makeTaskId()
        data = {"status": {
            "taskId": taskid,
            "runs": [
                {"runId": 0},
            ],
        }}

        processed_date = fake_now.return_value = Arrow(2015, 4, 1)
        self.tclistener.tc_queue.task.return_value = {
            "created": 50,
            "payload": {
                "buildername": "builder good name",
            },
        }
        self.buildbot_db.injectTask.return_value = 1
        self.tclistener.handlePending(data, Mock())

        self.assertEquals(self.tclistener.tc_queue.task.call_count, 1)
        self.assertEquals(self.buildbot_db.injectTask.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEquals(len(bbb_state), 1)
        self.assertEquals(bbb_state[0].buildrequestId, 1)
        self.assertEquals(bbb_state[0].taskId, taskid)
        self.assertEquals(bbb_state[0].runId, 0)
        self.assertEquals(bbb_state[0].createdDate, 50)
        self.assertEquals(bbb_state[0].processedDate, processed_date.timestamp)
        self.assertEquals(bbb_state[0].takenUntil, None)

    def testHandlePendingUpdateRunId(self):
        taskid = makeTaskId()
        self.tasks.insert().execute(
            buildRequestId=1,
            taskId=taskid,
            runId=0,
            createdDate=23,
            processedDate=34,
            takenUntil=None
        )
        data = {"status": {
            "taskId": taskid,
            "runs": [
                {"runId": 0},
                {"runId": 1},
            ],
        }}
        self.tclistener.tc_queue.task.return_value = {
            "created": 20,
            "payload": {
                "buildername": "builder good name",
            },
        }

        self.tclistener.handlePending(data, Mock())
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEquals(len(bbb_state), 1)
        self.assertEquals(bbb_state[0].buildrequestId, 1)
        self.assertEquals(bbb_state[0].taskId, taskid)
        self.assertEquals(bbb_state[0].runId, 1)
        self.assertEquals(bbb_state[0].createdDate, 23)
        self.assertEquals(bbb_state[0].processedDate, 34)
        self.assertEquals(bbb_state[0].takenUntil, None)

    def testHandlePendingDisallowedBuilder(self):
        taskid = makeTaskId()
        data = {"status": {
            "taskId": taskid,
            "runs": [
                {"runId": 0},
            ],
        }}

        self.buildbot_db.injectTask.return_value = 2
        self.tclistener.tc_queue.task.return_value = {
            "created": 20,
            "payload": {
                "buildername": "builder bad name",
            },
        }
        self.tclistener.handlePending(data, Mock())

        self.assertEquals(self.tclistener.tc_queue.cancelTask.call_count, 1)
