import json

import arrow
from mozillapulse.config import PulseConfiguration
from mozillapulse.consumers import GenericConsumer
import sqlalchemy as sa
from taskcluster import Queue

from .timeutils import parseDateString

import logging
log = logging.getLogger(__name__)


class BBBDb(object):
    def __init__(self, uri):
        self.db = sa.create_engine(uri)
        metadata = sa.MetaData(self.db)
        self.tasks_table = sa.Table('tasks', metadata,
            sa.Column('buildrequestId', sa.Integer, primary_key=True),
            sa.Column('taskId', sa.String(32), index=True),
            sa.Column('runId', sa.Integer),
            sa.Column('createdDate', sa.Integer),  # When the task was submitted to TC
            sa.Column('processedDate', sa.Integer),  # When we put it into BB
            sa.Column('takenUntil', sa.Integer, index=True),  # How long until our claim needs to be renewed
        )
        metadata.create_all(self.db)

    def getTask(self, taskId):
        log.info("Fetching task %s", taskId)
        task = self.tasks_table.select(self.tasks_table.c.taskId == taskId).execute().fetchone()
        log.debug("Task: %s", task)
        return task

    def getTaskFromBuildRequest(self, brid):
        task = self.tasks_table.select(self.tasks_table.c.buildrequestId == brid).execute().fetchone()
        if not task:
            raise ValueError("Couldn't find task for brid %i", brid)
        return task

    def getAllTasks(self):
        log.debug("Fetching all tasks")
        tasks = self.tasks_table.select().execute().fetchall()
        log.debug("Tasks: %s", tasks)
        return tasks

    def createTask(self, taskId, runId, brid, createdDate):
        log.info("Creating task %s", taskId)
        log.debug("Task info: runId: %s, brid: %s, created: %s", runId, brid, createdDate)
        self.tasks_table.insert().values(
            taskId=taskId,
            runId=runId,
            buildrequestId=brid,
            createdDate=createdDate,
            processedDate=arrow.now().timestamp,
        ).execute()

    def deleteBuildRequest(self, brid):
        log.info("Deleting task with brid %s", brid)
        self.tasks_table.delete(self.tasks_table.c.buildrequestId == brid).execute()

    def updateRunId(self, brid, runId):
        log.info("Updating task with brid %s to runId %s", brid, runId)
        self.tasks_table.update(self.tasks_table.c.buildrequestId == brid).values(runId=runId).execute()

    def updateTakenUntil(self, brid, takenUntil):
        log.debug("Updating task with brid %s to takenUntil %s", brid, takenUntil)
        self.tasks_table.update(self.tasks_table.c.buildrequestId == brid).values(takenUntil=takenUntil).execute()


class BuildbotDb(object):
    def __init__(self, uri):
        self.db = sa.create_engine(uri)

    def getBuildRequest(self, brid):
        return self.db.execute(sa.text("select * from buildrequests where id=:brid", brid=brid)).fetchone()

    def getBuildRequests(self, buildnumber):
        return self.db.execute(
            sa.text("select buildrequests.id from buildrequests join builds ON buildrequests.id=builds.brid where builds.number=:buildnumber"),
            buildnumber=buildnumber,
        ).fetchall()

    def getBuilds(self, brid):
        return self.db.execute(sa.text("select * from builds where brid=:brid", brid=brid)).fetchall()

    def createSourceStamp(self, sourcestamp={}):
        q = sa.text("""INSERT INTO sourcestamps
                    (`branch`, `revision`, `patchid`, `repository`, `project`)
                VALUES
                    (:branch, :revision, NULL, :repository, :project)
                """)
        branch = sourcestamp.get('branch')
        revision = sourcestamp.get('revision')
        repository = sourcestamp.get('repository', '')
        project = sourcestamp.get('project', '')

        r = self.db.execute(q, branch=branch, revision=revision, repository=repository, project=project)
        ssid = r.lastrowid
        log.info("Created sourcestamp %s", ssid)

        # TODO: Create change objects, files, etc.
        return ssid

    def createBuildSetProperties(self, buildsetid, properties):
        q = sa.text("""INSERT INTO buildset_properties
                (`buildsetid`, `property_name`, `property_value`)
                VALUES
                (:buildsetid, :key, :value)
                """)
        props = {}
        props.update(((k, json.dumps((v, "bbb"))) for (k, v) in properties.iteritems()))
        for key, value in props.items():
            self.db.execute(q, buildsetid=buildsetid, key=key, value=value)
            log.info("Created buildset_property %s=%s", key, value)

    def injectTask(self, taskId, task):
        payload = task["payload"]
        # Create a sourcestamp if necessary
        sourcestamp = payload.get('sourcestamp', {})

        sourcestampid = self.createSourceStamp(self.db, sourcestamp)

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
        self.createBuildSetProperties(self.db, buildsetid, properties)

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


class ListenerService(ServiceBase):
    """A base for BBB services that run in response to events from Pulse."""
    def __init__(self, pulse_user, pulse_password, exchange, topic, eventHandlers, *args, **kwargs):
        super(ListenerService, self).__init__(*args, **kwargs)

        self.pulse_config = PulseConfiguration(
            user=pulse_user,
            password=pulse_password,
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
        #self.pulse_consumer.listen()

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
