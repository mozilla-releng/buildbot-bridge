#!/usr/bin/env python
import json
import time

import arrow
import taskcluster
import sqlalchemy as sa
import requests
from mozillapulse.config import PulseConfiguration
from mozillapulse.consumers import GenericConsumer
from redo import retrier

import logging
log = logging.getLogger(__name__)


def parseDateString(datestring):
    """
    Parses a date string like 2015-02-13T19:33:37.075719Z and returns a unix epoch time
    """
    return arrow.get(datestring).timestamp


def create_sourcestamp(db, sourcestamp={}):
    q = sa.text("""INSERT INTO sourcestamps
                (`branch`, `revision`, `patchid`, `repository`, `project`)
            VALUES
                (:branch, :revision, NULL, :repository, :project)
            """)
    branch = sourcestamp.get('branch')
    revision = sourcestamp.get('revision')
    repository = sourcestamp.get('repository', '')
    project = sourcestamp.get('project', '')

    r = db.execute(q, branch=branch, revision=revision, repository=repository, project=project)
    ssid = r.lastrowid
    log.info("Created sourcestamp %s", ssid)

    # TODO: Create change objects, files, etc.
    return ssid


def create_buildset_properties(db, buildsetid, properties):
    q = sa.text("""INSERT INTO buildset_properties
            (`buildsetid`, `property_name`, `property_value`)
            VALUES
            (:buildsetid, :key, :value)
            """)
    props = {}
    props.update(((k, json.dumps((v, "bbb"))) for (k, v) in properties.iteritems()))
    for key, value in props.items():
        db.execute(q, buildsetid=buildsetid, key=key, value=value)
        log.info("Created buildset_property %s=%s", key, value)


def inject_task(db, taskId, task, payload):
    # Create a sourcestamp if necessary
    sourcestamp = payload.get('sourcestamp', {})

    sourcestampid = create_sourcestamp(db, sourcestamp)

    # Create a buildset
    q = sa.text("""INSERT INTO buildsets
        (`external_idstring`, `reason`, `sourcestampid`, `submitted_at`, `complete`, `complete_at`, `results`)
        VALUES
        (:idstring, :reason, :sourcestampid, :submitted_at, 0, NULL, NULL)""")

    # TODO: submitted_at should be now, or the original task?
    # using orginal task's date for now
    submitted_at = parseDateString(task['created'])
    r = db.execute(q,
                   idstring="taskId:{}".format(taskId),
                   reason="Created by BBB for task {0}".format(taskId),
                   sourcestampid=sourcestampid,
                   submitted_at=submitted_at,
                   )

    buildsetid = r.lastrowid
    log.info("Created buildset %i", buildsetid)

    # Create properties
    properties = payload.get('properties', {})
    # Always create a property for the taskId
    properties['taskId'] = taskId
    create_buildset_properties(db, buildsetid, properties)

    # Create the buildrequest
    buildername = payload['buildername']
    priority = payload.get('priority', 0)
    q = sa.text("""INSERT INTO buildrequests
            (`buildsetid`, `buildername`, `submitted_at`, `priority`,
                `claimed_at`, `claimed_by_name`, `claimed_by_incarnation`,
                `complete`, `results`, `complete_at`)
            VALUES
            (:buildsetid, :buildername, :submitted_at, :priority, 0, NULL, NULL, 0, NULL, NULL)""")
    log.info(q)
    r = db.execute(
        q,
        buildsetid=buildsetid,
        buildername=buildername,
        submitted_at=submitted_at,
        priority=priority)
    log.info("Created buildrequest %s: %i", buildername, r.lastrowid)
    return r.lastrowid


def create_bbb_db(db):
    meta = sa.MetaData(db)
    tasks = sa.Table('tasks', meta,
                     sa.Column('buildrequestId', sa.Integer, primary_key=True),
                     sa.Column('taskId', sa.String(32), index=True),
                     sa.Column('runId', sa.Integer),
                     sa.Column('createdDate', sa.Integer),  # When the task was submitted to TC
                     sa.Column('processedDate', sa.Integer),  # When we put it into BB
                     sa.Column('takenUntil', sa.Integer, index=True),  # How long until our claim needs to be renewed
                     )
    meta.create_all(db)
    return tasks


class BuildbotBridge(object):
    default_config = {
        'taskcluster_pulse_exchange': 'exchange/taskcluster-queue/v1/task-pending',
        'taskcluster_pulse_topic': '*.*.*.*.*.*.test-buildbot.#',
        'buildbot_pulse_exchange': 'exchange/build',
        'buildbot_pulse_topic': '#',
    }

    def __init__(self, config):
        self.config = self.default_config.copy()
        self.config.update(config)

        self.taskcluster_queue = taskcluster.Queue({
            'credentials': {
                'clientId': self.config['taskcluster_credentials']['clientId'].encode('ascii'),
                'accessToken': self.config['taskcluster_credentials']['accessToken'].encode('ascii'),
            }}
        )

        self.buildbot_db = sa.create_engine(self.config['buildbot_scheduler_db'])
        self.bbb_db = sa.create_engine(self.config['bbb_db'])
        self.tasks_table = create_bbb_db(self.bbb_db)

    def getTask(self, taskId):
        log.info("fetching task %s", taskId)
        task = self.taskcluster_queue.getTask(taskId)
        log.debug("task: %s", task)
        return task

    def createJsonArtifact(self, taskId, runId, name, data, expires):
        data = json.dumps(data)
        resp = self.taskcluster_queue.createArtifact(taskId, runId, name, {
            "storageType": "s3",
            "contentType": "application/json",
            "expires": expires,
        })
        log.debug("got %s", resp)
        assert resp['storageType'] == 's3'
        putUrl = resp['putUrl']
        log.debug("uploading to %s", putUrl)
        for _ in retrier():
            try:
                resp = requests.put(putUrl, data=data, headers={
                    'Content-Type': 'application/json',
                    'Content-Length': len(data),
                })
                log.debug("got %s %s", resp, resp.headers)
                return
            except Exception:
                log.debug("error submitting to s3", exc_info=True)
                continue
        else:
            log.error("couldn't upload artifact to s3")
            raise IOError("couldn't upload artifact to s3")

    def getTaskId(self, brid):
        row = self.tasks_table.select(self.tasks_table.c.buildrequestId == brid).execute().fetchone()
        if not row:
            raise ValueError("Couldn't find row for brid %i", brid)
        return row.taskId, row.runId

    def deleteBuildrequest(self, brid):
        self.tasks_table.delete(self.tasks_table.c.buildrequestId == brid).execute()

    def updateRunId(self, brid, runId):
        self.tasks_table.update(self.tasks_table.c.buildrequestId == brid).values(runId=runId).execute()

    def start_injector(self):
        # TODO: Look for any pending work with the poller; or rely on durable
        # queues?
        pulse_config = PulseConfiguration(
            user=self.config['pulse_user'],
            password=self.config['pulse_password'],
        )
        pulse_exchange = self.config['taskcluster_pulse_exchange']
        pulse_topic = self.config['taskcluster_pulse_topic']

        self.pulse_consumer = GenericConsumer(pulse_config, exchange=pulse_exchange)
        self.pulse_consumer.configure(topic=pulse_topic, callback=self.receivedTCMessage)
        log.info("listening for pulse messages...")
        self.pulse_consumer.listen()

    def start_reaper(self):
        pulse_config = PulseConfiguration(
            user=self.config['pulse_user'],
            password=self.config['pulse_password'],
        )
        pulse_exchange = self.config['buildbot_pulse_exchange']
        pulse_topic = self.config['buildbot_pulse_topic']

        self.pulse_consumer = GenericConsumer(pulse_config, exchange=pulse_exchange)
        self.pulse_consumer.configure(topic=pulse_topic, callback=self.receivedBBMessage)
        log.info("listening for pulse messages on %s/%s...", pulse_exchange, pulse_topic)
        self.pulse_consumer.listen()

    def receivedTCMessage(self, data, msg):
        log.debug("got %s %s", data, msg)
        taskId = data['status']['taskId']
        runId = data['status']['runs'][-1]['runId']
        try:
            task = self.getTask(taskId)
            log.info("claiming %s", taskId)
            claim = self.taskcluster_queue.claimTask(taskId, runId, {
                'workerGroup': 'test-buildbot-bridge',
                'workerId': 'test-buildot-id',
            })
            log.debug("claim: %s", claim)

            buildrequestId = inject_task(self.buildbot_db, taskId, task, task['payload'])
            self.tasks_table.insert().values(
                taskId=taskId,
                runId=runId,
                buildrequestId=buildrequestId,
                createdDate=parseDateString(task['created']),
                processedDate=arrow.now().timestamp,
                takenUntil=parseDateString(claim['takenUntil'])
            ).execute()
        except:
            log.exception("problem handling task; re-queuing")
            self.taskcluster_queue.reportException(taskId, runId, {"reason": "worker-shutdown"})
            raise
        msg.ack()

    def receivedBBMessage(self, data, msg):
        log.debug("got %s %s", data, msg)
        # Get the request_ids from the properties
        try:
            properties = dict((key, (value, source)) for (key, value, source) in data['payload']['build']['properties'])
        except KeyError:
            log.error("couldn't get job properties")
            msg.ack()
            return

        request_ids = properties.get('request_ids')
        if not request_ids:
            log.error("couldn't get request ids from %s", data)
            msg.ack()
            return

        # Sanity check
        assert request_ids[1] == 'postrun.py'

        try:
            results = data['payload']['build']['results']
        except KeyError:
            log.error("coudn't find job results")
            msg.ack()
            return

        # For each request, get the taskId and runId
        for brid in request_ids[0]:
            try:
                taskId, runId = self.getTaskId(brid)
            except ValueError:
                log.error("Couldn't find task for %i", brid)
                continue

            log.info("brid %i : taskId %s : runId %i", brid, taskId, runId)

            # Attach properties as artifacts
            log.info("attaching properties to task %s", taskId)
            expires = arrow.now().replace(weeks=1).isoformat()
            self.createJsonArtifact(taskId, runId, "properties.json", properties, expires)

            # If this isn't a retry, mark the task as done
            # buildbot status 5 == RETRY
            if results != 5:
                if results == 0:
                    log.info("marking task %s as completed", taskId)
                    self.taskcluster_queue.reportCompleted(taskId, runId, {'success': True})
                else:
                    log.info("marking task %s as failed", taskId)
                    self.taskcluster_queue.reportFailed(taskId, runId)

                self.deleteBuildrequest(brid)

            # If this is a retry, mark it as failed, and call retryTask
            else:
                log.info("marking task %s as failed", taskId)
                self.taskcluster_queue.reportFailed(taskId, runId)
                log.info("re-running task %s", taskId)
                status = self.taskcluster_queue.rerunTask(taskId)
                # Update DB with runId?
                self.updateRunId(brid, len(status['runs']) - 1)

        msg.ack()

    def start_reclaimer(self):
        while True:
            self.reclaimTasks()
            time.sleep(60)

    def reclaimTasks(self):
        """
        Re-claim tasks in taskcluster that are still active in buildbot
        Update TC from buildbot status
        """
        tasks_table = self.tasks_table
        buildbot_db = self.buildbot_db
        for t in self.tasks_table.select().execute().fetchall():
            # Get the buildbot status
            buildrequest = buildbot_db.execute(sa.text("select * from buildrequests where id=:buildrequestId"),
                                               buildrequestId=t.buildrequestId).fetchone()
            builds = buildbot_db.execute(sa.text("select * from builds where brid=:buildrequestId"),
                                         buildrequestId=t.buildrequestId).fetchall()
            log.debug("Task info: %s", t)
            log.debug("Buildrequest: %s", buildrequest)
            if buildrequest.complete:
                # The reaper should be handling deleting tasks
                # TODO: have a max time here?
                log.info("buildrequest %i is done", t.buildrequestId)
                continue

            # TODO: Probably need to handle retries better here since the
            # buildrequest won't be complete
            if len(builds) > t.runId + 1:
                log.warn("too many buildbot builds? runId is %i but we have %i builds", t.runId, len(builds))

            log.debug("Incomplete; should re-claim")
            try:
                result = self.taskcluster_queue.reclaimTask(t.taskId, t.runId)
            except taskcluster.exceptions.TaskclusterRestFailure, e:
                if e.superExc.response.status_code == 409:
                    # Conflict; it's expired
                    log.warn("couldn't reclaim task %s: HTTP 409; deleting", t.taskId)
                    self.deleteBuildrequest(t.buildrequestId)
                else:
                    log.error("Couldn't reclaim task: %s", e.superExc)
                continue

            # Update takenUntil
            tasks_table.update(tasks_table.c.buildrequestId == t.buildrequestId).values(takenUntil=parseDateString(result['takenUntil']))
            log.info("task %s now takenUntil %s", t.taskId, result['takenUntil'])


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.set_defaults(
        loglevel=logging.INFO,
    )
    parser.add_argument("-v", "--verbose", dest="loglevel", action="store_const", const=logging.DEBUG)
    parser.add_argument("-q", "--quiet", dest="loglevel", action="store_const", const=logging.WARN)
    parser.add_argument("-c", "--config", dest="config", required=True)
    # actions
    # injector: the thing that takes taskcluster tasks and puts them into
    # buildbot
    parser.add_argument("--injector", dest="action", action="store_const", const="injector",
                        help="run the taskcluster -> buildbot injector")
    # reclaimer: the thing that reclaims in-progress buildbot jobs
    parser.add_argument("--reclaimer", dest="action", action="store_const", const="reclaimer",
                        help="run the taskcluster task reclaimer")
    # reaper: the thing that handles buildbot jobs finishing (or retrying)
    parser.add_argument("--reaper", dest="action", action="store_const", const="reaper",
                        help="run the buildbot task reaper")

    args = parser.parse_args()

    if not args.action:
        parser.error("one of the actions is required")

    # Set the default logging to WARNINGS
    logging.basicConfig(level=logging.WARN, format="%(asctime)s - %(name)s - %(message)s")
    # Set our logger to the specified level
    log.setLevel(args.loglevel)

    config = json.load(open(args.config))

    bbb = BuildbotBridge(config)

    log.info("Running %s", args.action)
    action = getattr(bbb, 'start_{}'.format(args.action))
    action()

if __name__ == '__main__':
    main()
