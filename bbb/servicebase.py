import json

import arrow
from mozillapulse.config import PulseConfiguration
from mozillapulse.consumers import GenericConsumer
import sqlalchemy as sa
from taskcluster import Queue

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

def create_tasks_table(db):
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


class BBBDb(object):
    def __init__(self, uri):
        self.db = sa.create_engine(uri)
        self.tasks_table = create_tasks_table(self.bbb_db)

    def getTask(self, taskId):
        return self.tasks_table.select(self.tasks_table.c.taskId == taskId).execute().fetchone()

    def createTask(self, taskId, runId, brid, createdDate):
        self.tasks_table.insert().values(
            taskId=taskId,
            runId=runId,
            buildrequestId=brid,
            createdDate=createdDate,
            processedDate=arrow.now().timestamp,
        ).execute()

    def deleteBuildRequest(self, brid):
        self.tasks_table.delete(self.tasks_table.c.buildrequestId == brid).execute()

    def updateRunId(self, brid, runId):
        self.tasks_table.update(self.tasks_table.c.buildrequestId == brid).values(runId=runId).execute()


class BuildbotDb(object):
    def __init__(self, uri):
        self.db = sa.create_engine(uri)

    def injectTask(self, taskId, task):
        payload = task["payload"]
        # Create a sourcestamp if necessary
        sourcestamp = payload.get('sourcestamp', {})

        sourcestampid = create_sourcestamp(self.db, sourcestamp)

        # Create a buildset
        q = sa.text("""INSERT INTO buildsets
            (`external_idstring`, `reason`, `sourcestampid`, `submitted_at`, `complete`, `complete_at`, `results`)
            VALUES
            (:idstring, :reason, :sourcestampid, :submitted_at, 0, NULL, NULL)""")

        # TODO: submitted_at should be now, or the original task?
        # using orginal task's date for now
        submitted_at = parseDateString(task['created'])
        r = self.db.execute(
            q,
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
        create_buildset_properties(self.db, buildsetid, properties)

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
        r = self.db.execute(
            q,
            buildsetid=buildsetid,
            buildername=buildername,
            submitted_at=submitted_at,
            priority=priority
        )
        log.info("Created buildrequest %s: %i", buildername, r.lastrowid)
        return r.lastrowid


class ServiceBase(object):
    """A base for all BBB services that manages access to the buildbot db,
       bbb db, and taskcluster."""
    def __init__(self, bbb_db, buildbot_db, tc_credentials):
        self.bbb_db = BBBDb(bbb_db)
        self.buildbot_db = BuildbotDb(buildbot_db)
        self.tc_queue = Queue(tc_credentials)

    def start(self):
        raise NotImplementedError()


class ListenerService(object):
    """A base for BBB services that run in response to events from Pulse."""
    def __init__(self, user, password, exchange, topic, eventHandlers, *args, **kwargs):
        super(ListenerService, self).__init__(*args, **kwargs)

        self.pulse_config = PulseConfiguration(
            user=user,
            password=password,
            # TODO: remove me
            ssl=False
        )
        self.exchange = exchange
        self.topic = topic
        self.eventHandlers = eventHandlers
        self.pulse_consumer = GenericConsumer(self.pulse_config, durable=True, exchange=exchange)
        self.pulse_consumer.configure(topic=topic, callback=self.receivedMessage)

    def start(self):
        log.info("Listening for Pulse messages")
        log.debug("Exchange is %s", self.exchange)
        log.debug("Topic is %s", self.topic)
        self.pulse_consumer.listen()

    def receivedMessage(self, data, msg):
        log.info("Received message on %s", data["_meta"]["routing_key"])
        log.debug("Got %s %s", data, msg)

        event = self.getEvent(data)

        log.info("Handling event: %s", event)
        self.eventHandlers(event)(data, msg)
        # TODO: Should we ack here even if there was an exception? Retrying
        # the same message over and over again may not work.
        msg.ack()

    def getEvent(self, *args, **kwargs):
        raise NotImplementedError()
