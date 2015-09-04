from mock import Mock, patch
import unittest

import arrow
import sqlalchemy as sa
from taskcluster.exceptions import TaskclusterRestFailure

from .dbutils import makeSchedulerDb
from ..services import BuildbotListener, Reflector, TCListener, SUCCESS, \
    WARNINGS, FAILURE, EXCEPTION, RETRY, CANCELLED
from ..tcutils import makeTaskId


class TestBuildbotListener(unittest.TestCase):
    def setUp(self):
        self.bblistener = BuildbotListener(
            bbb_db="sqlite:///:memory:",
            buildbot_db="sqlite:///:memory:",
            buildbot_db_init_func=makeSchedulerDb,
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
            allowed_builders=(
                ".*good.*",
            ),
        )
        # Replace the TaskCluster Queue object with a Mock because we never
        # want to actually talk to TC, just check if the calls that would've
        # been made are correct
        self.bblistener.tc_queue = Mock()
        self.tasks = self.bblistener.bbb_db.tasks_table
        self.buildbot_db = self.bblistener.buildbot_db.db

    def testHandleStartedOneBuildRequest(self):
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at, claimed_by_name, claimed_by_incarnation)
    VALUES (4, 0, "good", 50, "a", "b");"""))
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
        data = {
            "payload": {
                "build": {
                    "number": 2,
                    "builderName": "good",
                }
            },
            "_meta": {
                "master_name": "a",
                "master_incarnation": "b",
            },
        }
        self.bblistener.tc_queue.claimTask.return_value = {"takenUntil": 100}
        self.bblistener.handleStarted(data, Mock())

        self.assertEqual(self.bblistener.tc_queue.claimTask.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 1)
        self.assertEqual(bbb_state[0].takenUntil, 100)

    def testHandleStartedMultipleBuildRequests(self):
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at, claimed_by_name, claimed_by_incarnation)
    VALUES (2, 0, "good", 20, "a", "b"), (3, 0, "good", 30, "a", "b");"""))
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
        data = {
            "payload": {
                "build": {
                    "number": 3,
                    "builderName": "good",
                },
            },
            "_meta": {
                "master_name": "a",
                "master_incarnation": "b",
            },
        }
        self.bblistener.tc_queue.claimTask.return_value = {"takenUntil": 80}
        self.bblistener.handleStarted(data, Mock())

        self.assertEqual(self.bblistener.tc_queue.claimTask.call_count, 2)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 2)
        self.assertEqual(bbb_state[0].takenUntil, 80)
        self.assertEqual(bbb_state[1].takenUntil, 80)

    def testHandleStartedDisallowedBuilder(self):
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at, claimed_by_name, claimed_by_incarnation)
    VALUES (4, 0, "bad", 50, "a", "b");"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO builds
    (id, number, brid, start_time)
    VALUES (0, 2, 4, 60);"""))

        data = {
            "payload": {
                "build": {
                    "number": 2,
                    "builderName": "bad",
                }
            },
            "_meta": {
                "master_name": "a",
                "master_incarnation": "b",
            },
        }
        self.bblistener.handleStarted(data, Mock())

        self.assertEqual(self.bblistener.tc_queue.claimTask.call_count, 0)

    # TODO: tests for some error cases, like failing to contact TC, or maybe db operations failing

    # Passing new=Mock prevents patch from passing the patched object,
    # which we have no need of, to this method.
    @patch("requests.put", new=Mock)
    def _handleFinishedTest(self, results):
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at, complete, complete_at, results)
    VALUES (1, 0, "good", 5, 1, 30, 0);"""))
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
            "builderName": "good",
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

        self.assertEqual(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEqual(self.bblistener.tc_queue.reportCompleted.call_count, 1)
        # Build and Task are done - should be deleted from our db.
        self.assertEqual(self.tasks.count().execute().fetchone()[0], 0)

    def testHandleFinishedWarnings(self):
        self._handleFinishedTest(WARNINGS)

        self.assertEqual(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEqual(self.bblistener.tc_queue.reportFailed.call_count, 1)
        # Build and Task are done - should be deleted from our db.
        self.assertEqual(self.tasks.count().execute().fetchone()[0], 0)

    def testHandleFinishedFailed(self):
        self._handleFinishedTest(FAILURE)

        self.assertEqual(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEqual(self.bblistener.tc_queue.reportFailed.call_count, 1)
        # Build and Task are done - should be deleted from our db.
        self.assertEqual(self.tasks.count().execute().fetchone()[0], 0)

    def testHandleFinishedException(self):
        self._handleFinishedTest(EXCEPTION)

        self.assertEqual(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEqual(self.bblistener.tc_queue.reportException.call_count, 1)
        # Build and Task are done - should be deleted from our db.
        self.assertEqual(self.tasks.count().execute().fetchone()[0], 0)

    def testHandleFinishedRetry(self):
        self._handleFinishedTest(RETRY)

        self.assertEqual(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEqual(self.bblistener.tc_queue.reportException.call_count, 1)
        # Unlike other status, the BuildRequest is NOT finished if a RETRY is hit
        self.assertEqual(self.tasks.count().execute().fetchone()[0], 1)

    def testHandleFinishedCancelled(self):
        self._handleFinishedTest(CANCELLED)

        self.assertEqual(self.bblistener.tc_queue.createArtifact.call_count, 1)
        self.assertEqual(self.bblistener.tc_queue.cancelTask.call_count, 1)
        # Build and Task are done - should be deleted from our db.
        self.assertEqual(self.tasks.count().execute().fetchone()[0], 0)

    def testHandleFinishedIgnoredBuilder(self):
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at, complete, complete_at, results)
    VALUES (1, 0, "bad", 5, 1, 30, 0);"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO builds
    (id, number, brid, start_time, finish_time)
    VALUES (0, 0, 1, 15, 30);"""))

        data = {"payload": {"build": {
            "builderName": "bad",
            "properties": (
                ("request_ids", (1,), "postrun.py"),
            ),
            "results": SUCCESS,
        }}}
        self.bblistener.handleFinished(data, Mock())

        self.assertEqual(self.bblistener.tc_queue.createArtifact.call_count, 0)
        self.assertEqual(self.bblistener.tc_queue.cancelTask.call_count, 0)
        self.assertEqual(self.bblistener.tc_queue.reportException.call_count, 0)
        self.assertEqual(self.bblistener.tc_queue.reportFailed.call_count, 0)
        self.assertEqual(self.bblistener.tc_queue.reportCompleted.call_count, 0)


class TestReflector(unittest.TestCase):
    def setUp(self):
        self.reflector = Reflector(
            bbb_db="sqlite:///:memory:",
            buildbot_db="sqlite:///:memory:",
            buildbot_db_init_func=makeSchedulerDb,
            tc_config={
                "credentials": {
                    "clientId": "fake",
                    "accessToken": "fake",
                }
            },
            interval=5,
            selfserve_url="fake",
        )
        # Replace the TaskCluster Queue object with a Mock because we never
        # want to actually talk to TC, just check if the calls that would've
        # been made are correct
        self.reflector.tc_queue = Mock()
        self.reflector.selfserve = Mock()
        self.tasks = self.reflector.bbb_db.tasks_table
        self.buildbot_db = self.reflector.buildbot_db.db

    @patch("arrow.now")
    def testReclaimRunningTask(self, fake_now):
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

        fake_now.return_value = arrow.get(150)
        self.reflector.tc_queue.reclaimTask.return_value = {"takenUntil": 1000}
        self.reflector.reflectTasks()

        self.assertEqual(self.reflector.tc_queue.reclaimTask.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 1)
        self.assertEqual(bbb_state[0].takenUntil, 1000)

    @patch("arrow.now")
    def testDontReclaimTaskTooSoon(self, fake_now):
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
            takenUntil=1000,
        )

        # Because we're testing that reclaims don't happen too soon, we need
        # to make sure "now" is within 5 minutes of the takenUntil time.
        fake_now.return_value = arrow.get(500)
        self.reflector.reflectTasks()

        self.assertEqual(self.reflector.tc_queue.reclaimTask.call_count, 0)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 1)
        self.assertEqual(bbb_state[0].takenUntil, 1000)

    @patch("arrow.now")
    def testReclaimExpiredTask(self, fake_now):
        taskid = makeTaskId()
        self.buildbot_db.execute(sa.text("""
INSERT INTO sourcestamps
    (id, branch) VALUES (0, "foo");
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildsets
    (id, sourcestampid, submitted_at) VALUES (0, 0, 2);
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at)
    VALUES (2, 0, "foo", 15);
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO builds
    (id, number, brid, start_time)
    VALUES (2, 0, 2, 30);
"""))
        self.tasks.insert().execute(
            buildrequestId=2,
            taskId=taskid,
            runId=0,
            createdDate=12,
            processedDate=17,
            takenUntil=200,
        )

        fake_now.return_value = arrow.get(150)
        super_exc = Mock()
        super_exc.response.status_code = 409
        self.reflector.tc_queue.reclaimTask.side_effect = TaskclusterRestFailure("fail", super_exc, 409)
        self.reflector.reflectTasks()

        self.assertEqual(self.reflector.tc_queue.reclaimTask.call_count, 1)
        self.assertEqual(self.reflector.selfserve.cancelBuild.call_count, 1)

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
        self.assertEqual(len(bbb_state), 1)
        self.assertEqual(bbb_state[0].buildrequestId, 0)
        self.assertEqual(bbb_state[0].taskId, taskid)
        self.assertEqual(bbb_state[0].runId, 0)
        self.assertEqual(bbb_state[0].createdDate, 20)
        self.assertEqual(bbb_state[0].processedDate, 25)
        self.assertEqual(bbb_state[0].takenUntil, None)

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
        self.assertEqual(self.reflector.tc_queue.cancelTask.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 0)


class TestTCListener(unittest.TestCase):
    def setUp(self):
        self.tclistener = TCListener(
            bbb_db="sqlite:///:memory:",
            buildbot_db="sqlite:///:memory:",
            buildbot_db_init_func=makeSchedulerDb,
            tc_config={
                "credentials": {
                    "clientId": "fake",
                    "accessToken": "fake",
                }
            },
            selfserve_url="fake",
            pulse_host="fake",
            pulse_user="fake",
            pulse_password="fake",
            pulse_queue_basename="fake",
            pulse_exchange_basename="fake",
            worker_type="fake",
            provisioner_id="fake",
            worker_group="fake",
            worker_id="fake",
            allowed_builders=(
                ".*good.*",
            ),
            ignored_builders=(
                ".*ignored.*",
            ),
        )
        # Replace the TaskCluster Queue object with a Mock because we never
        # want to actually talk to TC, just check if the calls that would've
        # been made are correct
        self.tclistener.tc_queue = Mock()
        self.tclistener.selfserve = Mock()
        self.tasks = self.tclistener.bbb_db.tasks_table
        self.buildbot_db = self.tclistener.buildbot_db.db

    @patch("arrow.now")
    def testHandlePendingNewTask(self, fake_now):
        taskid = makeTaskId()
        data = {"status": {
            "taskId": taskid,
            "runs": [
                {"runId": 0},
            ],
        }}

        processed_date = fake_now.return_value = arrow.Arrow(2015, 4, 1)
        self.tclistener.tc_queue.task.return_value = {
            "created": 50,
            "payload": {
                "buildername": "builder good name",
                "properties": {
                    "product": "foo",
                },
                "sourcestamp": {
                    "branch": "http://foo.com/blah",
                },
            },
        }
        self.tclistener.handlePending(data, Mock())

        self.assertEqual(self.tclistener.tc_queue.task.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 1)
        self.assertEqual(bbb_state[0].buildrequestId, 1)
        self.assertEqual(bbb_state[0].taskId, taskid)
        self.assertEqual(bbb_state[0].runId, 0)
        self.assertEqual(bbb_state[0].createdDate, 50)
        self.assertEqual(bbb_state[0].processedDate, processed_date.timestamp)
        self.assertEqual(bbb_state[0].takenUntil, None)

        buildrequests = self.tclistener.buildbot_db.buildrequests_table.select().execute().fetchall()
        self.assertEqual(buildrequests[0].id, 1)
        self.assertEqual(buildrequests[0].buildername, "builder good name")
        self.assertEqual(buildrequests[0].priority, 0)
        properties = self.tclistener.buildbot_db.buildset_properties_table.select().execute().fetchall()
        self.assertItemsEqual(properties, [
            (1, u"taskId", u'["{}", "bbb"]'.format(taskid)),
            (1, u"product", u'["foo", "bbb"]'),
        ])

    @patch("arrow.now")
    def testHandlePendingNewTaskWithHighPriority(self, fake_now):
        taskid = makeTaskId()
        data = {"status": {
            "taskId": taskid,
            "runs": [
                {"runId": 0},
            ],
        }}

        processed_date = fake_now.return_value = arrow.Arrow(2015, 4, 1)
        self.tclistener.tc_queue.task.return_value = {
            "created": 55,
            "priority": "high",
            "payload": {
                "buildername": "i'm a good builder",
                "properties": {
                    "product": "foo",
                },
                "sourcestamp": {
                    "branch": "http://foo.com/doit",
                },
            },
        }
        self.tclistener.handlePending(data, Mock())

        self.assertEqual(self.tclistener.tc_queue.task.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 1)
        self.assertEqual(bbb_state[0].buildrequestId, 1)
        self.assertEqual(bbb_state[0].taskId, taskid)
        self.assertEqual(bbb_state[0].runId, 0)
        self.assertEqual(bbb_state[0].createdDate, 55)
        self.assertEqual(bbb_state[0].processedDate, processed_date.timestamp)
        self.assertEqual(bbb_state[0].takenUntil, None)

        buildrequests = self.tclistener.buildbot_db.buildrequests_table.select().execute().fetchall()
        self.assertEqual(buildrequests[0].id, 1)
        self.assertEqual(buildrequests[0].buildername, "i'm a good builder")
        self.assertEqual(buildrequests[0].priority, 1)
        properties = self.tclistener.buildbot_db.buildset_properties_table.select().execute().fetchall()
        self.assertItemsEqual(properties, [
            (1, u"taskId", u'["{}", "bbb"]'.format(taskid)),
            (1, u"product", u'["foo", "bbb"]'),
        ])

    def testHandlePendingNewTaskWithoutProduct(self):
        """Tests that new tasks that don't set product don't create
           build requests, and resolve the task with a malformed-payload
           error."""

        taskid = makeTaskId()
        data = {"status": {
            "taskId": taskid,
            "runs": [
                {"runId": 0},
            ],
        }}

        self.tclistener.tc_queue.task.return_value = {
            "created": 50,
            "payload": {
                "buildername": "builder good name",
                "sourcestamp": {
                    "branch": "http://foo.com/blah",
                },
            },
        }
        self.tclistener.handlePending(data, Mock())

        self.assertEqual(self.tclistener.tc_queue.task.call_count, 1)
        self.assertEqual(self.tasks.count().execute().fetchone()[0], 0)
        self.assertEqual(self.tclistener.buildbot_db.buildrequests_table.count().execute().fetchone()[0], 0)
        # bug 1197291: buildbot bridge should claim & report exception instead
        # of canceling tasks with bad payloads
        self.assertEqual(self.tclistener.tc_queue.claimTask.call_count, 1)
        self.assertEqual(self.tclistener.tc_queue.reportException.call_count, 1)
        self.assertIn({"reason": "malformed-payload"}, self.tclistener.tc_queue.reportException.call_args[0])

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
                "properties": {
                    "product": "foo",
                },
                "sourcestamp": {
                    "branch": "https://hg.mozilla.org/integration/mozilla-inbound/",
                    "revision": "abcdef123456",
                },
            },
        }

        self.tclistener.handlePending(data, Mock())
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 1)
        self.assertEqual(bbb_state[0].buildrequestId, 1)
        self.assertEqual(bbb_state[0].taskId, taskid)
        self.assertEqual(bbb_state[0].runId, 1)
        self.assertEqual(bbb_state[0].createdDate, 23)
        self.assertEqual(bbb_state[0].processedDate, 34)
        self.assertEqual(bbb_state[0].takenUntil, None)

    def testHandlePendingDisallowedBuilder(self):
        taskid = makeTaskId()
        data = {"status": {
            "taskId": taskid,
            "runs": [
                {"runId": 0},
            ],
        }}

        self.tclistener.tc_queue.task.return_value = {
            "created": 20,
            "payload": {
                "buildername": "builder bad name",
                "sourcestamp": {
                    "branch": "https://hg.mozilla.org/integration/mozilla-inbound/",
                    "revision": "abcdef123456",
                },
            },
        }
        self.tclistener.handlePending(data, Mock())

        self.assertEqual(self.tclistener.tc_queue.claimTask.call_count, 1)
        self.assertEqual(self.tclistener.tc_queue.reportException.call_count, 1)
        self.assertIn({"reason": "malformed-payload"}, self.tclistener.tc_queue.reportException.call_args[0])

    def testHandlePendingIgnoredBuilder(self):
        taskid = makeTaskId()
        data = {"status": {
            "taskId": taskid,
            "runs": [
                {"runId": 0},
            ],
        }}

        self.tclistener.tc_queue.task.return_value = {
            "created": 20,
            "payload": {
                "buildername": "builder ignored name",
                "sourcestamp": {
                    "branch": "https://hg.mozilla.org/integration/mozilla-inbound/",
                    "revision": "abcdef123456",
                },
            },
        }
        self.tclistener.handlePending(data, Mock())

        self.assertEqual(self.tclistener.tc_queue.claimTask.call_count, 0)
        self.assertEqual(self.tclistener.tc_queue.reportException.call_count, 0)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 0)

    def testHandleExceptionCancellationBuildStarted(self):
        taskid = makeTaskId()
        self.buildbot_db.execute(sa.text("""
INSERT INTO sourcestamps
    (id, branch) VALUES (0, "foo");
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildsets
    (id, sourcestampid, submitted_at) VALUES (0, 0, 2);
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at)
    VALUES (0, 0, "good", 5);
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO builds
    (id, number, brid, start_time)
    VALUES (0, 0, 0, 40);"""))
        self.tasks.insert().execute(
            buildrequestId=0,
            taskId=taskid,
            runId=0,
            createdDate=3,
            processedDate=7,
            takenUntil=80,
        )

        data = {"status": {
            "taskId": taskid,
            "runs": [
                {
                    "runId": 0,
                    "state": "exception",
                    "reasonResolved": "canceled",
                },
            ],
        }}

        self.tclistener.handleException(data, Mock())

        self.assertEqual(self.tclistener.selfserve.cancelBuild.call_count, 1)
        # BBB State shouldn't be deleted, because the BuildbotListener
        # still needs to handle adding artifacts.
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 1)

    def testHandleExceptionCancellationBuildNotStarted(self):
        taskid = makeTaskId()
        self.buildbot_db.execute(sa.text("""
INSERT INTO sourcestamps
    (id, branch) VALUES (0, "foo");
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildsets
    (id, sourcestampid, submitted_at) VALUES (0, 0, 2);
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at)
    VALUES (0, 0, "good", 5);
"""))
        self.tasks.insert().execute(
            buildrequestId=0,
            taskId=taskid,
            runId=0,
            createdDate=3,
            processedDate=7,
            takenUntil=80,
        )

        data = {"status": {
            "taskId": taskid,
            "runs": [
                {
                    "runId": 0,
                    "state": "exception",
                    "reasonResolved": "canceled",
                },
            ],
        }}

        self.tclistener.handleException(data, Mock())

        self.assertEqual(self.tclistener.selfserve.cancelBuildRequest.call_count, 1)
        # bbb state should go away - there will be no buildFinished event
        # (because the Build never started!)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 0)

    def testHandleExceptionDeadlineExceededBuildStarted(self):
        taskid = makeTaskId()
        self.buildbot_db.execute(sa.text("""
INSERT INTO sourcestamps
    (id, branch) VALUES (0, "bar");
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildsets
    (id, sourcestampid, submitted_at) VALUES (0, 0, 2);
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at)
    VALUES (0, 0, "good", 5);
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO builds
    (id, number, brid, start_time)
    VALUES (0, 0, 0, 40);"""))
        self.tasks.insert().execute(
            buildrequestId=0,
            taskId=taskid,
            runId=0,
            createdDate=3,
            processedDate=7,
            takenUntil=80,
        )

        data = {"status": {
            "taskId": taskid,
            "runs": [
                {
                    "runId": 0,
                    "state": "exception",
                    "reasonResolved": "canceled",
                },
            ],
        }}

        self.tclistener.handleException(data, Mock())

        self.assertEqual(self.tclistener.selfserve.cancelBuild.call_count, 1)
        # BBB State shouldn't be deleted, because the BuildbotListener
        # still needs to handle adding artifacts.
        # TODO: Will the BBListener be able to add artifacts since the Task is
        # already resolved?
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 1)

    def testHandleExceptionDeadlineExceeded(self):
        taskid = makeTaskId()
        self.buildbot_db.execute(sa.text("""
INSERT INTO sourcestamps
    (id, branch) VALUES (0, "bar");
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildsets
    (id, sourcestampid, submitted_at) VALUES (0, 0, 2);
"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at)
    VALUES (0, 0, "good", 5);
"""))
        self.tasks.insert().execute(
            buildrequestId=0,
            taskId=taskid,
            runId=0,
            createdDate=3,
            processedDate=7,
            takenUntil=80,
        )

        data = {"status": {
            "taskId": taskid,
            "runs": [
                {
                    "runId": 0,
                    "state": "exception",
                    "reasonResolved": "deadline-exceeded",
                },
            ],
        }}

        self.tclistener.handleException(data, Mock())

        self.assertEqual(self.tclistener.selfserve.cancelBuildRequest.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 0)

    def testHandleExceptionOtherReason(self):
        self.tasks.insert().execute(
            buildrequestId=0,
            taskId=makeTaskId(),
            runId=0,
            createdDate=3,
            processedDate=7,
            takenUntil=80,
        )
        data = {"status": {
            "taskId": makeTaskId(),
            "runs": [
                {
                    "runId": 0,
                    "state": "exception",
                    "reasonResolved": "malformed-payload",
                },
            ],
        }}

        self.tclistener.handleException(data, Mock())

        # This event should be ignored by the handler, so we need to verify
        # that none of our state has changed.
        self.assertEqual(self.tclistener.selfserve.cancelBuild.call_count, 0)
        self.assertEqual(self.tclistener.selfserve.cancelBuildRequest.call_count, 0)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEqual(len(bbb_state), 1)
