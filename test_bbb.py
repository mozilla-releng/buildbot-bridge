import mock
import unittest

import arrow

from bbb import BuildbotBridge, inject_task

def getBBB():
    return BuildbotBridge({
        "buildbot_scheduler_db": "sqlite:///:memory:",
        "bbb_db": "sqlite:///:memory:",
        "taskcluster_credentials": {
            "clientId": "fake",
            "accessToken": "fake",
        },
        "taskcluster_worker_group": "bbb-test",
        "taskcluster_worker_id": "bbb-test",
    })


class TestInjector(unittest.TestCase):
    @mock.patch("bbb.inject_task")
    @mock.patch("arrow.now")
    @mock.patch("taskcluster.Queue")
    def testInjectTask(self, queue, now, inject_task):
        fakeNow = arrow.Arrow(2015, 1, 2, 3, 4, 5)
        brid = 23
        now.return_value = fakeNow
        inject_task.return_value = brid

        bbb = getBBB()
        bbb.taskcluster_queue.getTask.return_value = {
            "payload": {
                "buildername": "foo"
            },
            "created": 10,
        }
        bbb.taskcluster_queue.claimTask.return_value = {
            "takenUntil": 20, 
        }
        data = {
            "status": {
                "taskId": 1,
                "runs": [
                    {
                        "runId": 2,
                    },
                ],
            },
        }
        bbb.receivedTCMessage(data, mock.Mock())

        self.assertEquals(bbb.taskcluster_queue.claimTask.call_count, 1)
        # In an ideal world we'd test that inject_task inserts the right things
        # into the database. But doing that is very time consuming, and not
        # the tricky part of the bridge, so we're just going to assume that it works
        self.assertEquals(inject_task.call_count, 1)

        ret = bbb.tasks_table.select().execute().fetchall()
        self.assertEqual(len(ret), 1, "Wrong number of tasks created")
        ret = ret[0]
        self.assertEqual(ret.taskId, "1")
        self.assertEqual(ret.runId, 2)
        self.assertEqual(ret.buildrequestId, brid)
        self.assertEqual(ret.createdDate, 10)
        self.assertEqual(ret.processedDate, fakeNow.timestamp)
        self.assertEqual(ret.takenUntil, 20)

    @mock.patch("taskcluster.Queue")
    def testInjectTaskMessageAckedIfInjectionFails(self, queue):
        bbb = getBBB()
        bbb.taskcluster_queue.getTask.side_effect = Exception("busted!")
        data = {
            "status": {
                "taskId": 1,
                "runs": [
                    {
                        "runId": 2,
                    },
                ],
            },
        }
        msg = mock.Mock()
        self.assertRaises(Exception, bbb.receivedTCMessage, data, msg)

        self.assertEquals(msg.ack.call_count, 1)
