from collections import namedtuple
import json

import arrow
from kombu import Connection, Queue, Exchange
import sqlalchemy as sa
import taskcluster

from .timeutils import parseDateString

import logging
log = logging.getLogger(__name__)


ListenerServiceEvent = namedtuple("ListenerServiceEvent", ("exchange", "routing_key", "callback", "queue_name"))


class BBBDb(object):
    """Wrapper object for creation of and access to Buildbot Bridge database."""
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

    @property
    def tasks(self):
        log.debug("Fetching all tasks")
        tasks = self.tasks_table.select().execute().fetchall()
        log.debug("Tasks: %s", tasks)
        return tasks

    def getTask(self, taskid):
        log.info("Fetching task %s", taskid)
        task = self.tasks_table.select(self.tasks_table.c.taskId == taskid).execute().fetchone()
        log.debug("Task: %s", task)
        return task

    def getTaskFromBuildRequest(self, brid):
        task = self.tasks_table.select(self.tasks_table.c.buildrequestId == brid).execute().fetchone()
        if not task:
            raise ValueError("Couldn't find task for brid %i", brid)
        return task

    def createTask(self, taskid, runid, brid, created_date):
        log.info("Creating task %s", taskid)
        log.debug("Task info: runId: %s, brid: %s, created: %s", runid, brid, created_date)
        self.tasks_table.insert().values(
            taskId=taskid,
            runId=runid,
            buildrequestId=brid,
            createdDate=created_date,
            processedDate=arrow.now().timestamp,
        ).execute()

    def deleteBuildRequest(self, brid):
        log.info("Deleting task with brid %s", brid)
        self.tasks_table.delete(self.tasks_table.c.buildrequestId == brid).execute()

    def updateRunId(self, brid, runid):
        log.info("Updating task with brid %s to runId %s", brid, runid)
        self.tasks_table.update(self.tasks_table.c.buildrequestId == brid).values(runId=runid).execute()

    def updateTakenUntil(self, brid, taken_until):
        log.debug("Updating task with brid %s to takenUntil %s", brid, taken_until)
        self.tasks_table.update(self.tasks_table.c.buildrequestId == brid).values(takenUntil=taken_until).execute()


class BuildbotDb(object):
    """Wrapper object for access to preexisting Buildbot scheduler database."""
    def __init__(self, uri):
        self.db = sa.create_engine(uri)

    def getBuildRequest(self, brid):
        return self.db.execute(sa.text("select * from buildrequests where id=:brid"), brid=brid).fetchone()

    def getBuildRequests(self, buildnumber):
        return self.db.execute(
            sa.text("select buildrequests.id from buildrequests join builds ON buildrequests.id=builds.brid where builds.number=:buildnumber"),
            buildnumber=buildnumber,
        ).fetchall()

    def getBuilds(self, brid):
        return self.db.execute(sa.text("select * from builds where brid=:brid"), brid=brid).fetchall()

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

        sourcestampid = self.createSourceStamp(sourcestamp)

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
        self.createBuildSetProperties(buildsetid, properties)

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
    def __init__(self, bbb_db, buildbot_db, tc_config):
        self.bbb_db = BBBDb(bbb_db)
        self.buildbot_db = BuildbotDb(buildbot_db)
        self.tc_queue = taskcluster.Queue(tc_config)
        self.running = False

    def start(self):
        raise NotImplementedError()

    def stop(self):
        self.running = False


class ListenerService(ServiceBase):
    """A base for BBB services that run in response to events from Pulse."""
    def __init__(self, pulse_host, pulse_user, pulse_password, events, *args, **kwargs):
        super(ListenerService, self).__init__(*args, **kwargs)

        self.pulse_host = pulse_host
        self.pulse_user = pulse_user
        self.pulse_password = pulse_password
        self.events = events

    def start(self):
        log.info("Listening for Pulse messages")
        self.running = True

        connection = Connection(
            hostname=self.pulse_host,
            userid=self.pulse_user,
            password=self.pulse_password,
            # TODO: should be true
            ssl=False,
        )
        consumers = []
        for event in self.events:
            log.debug("Setting up queue on exchange: %s with routing_key: %s", event.exchange, event.routing_key)
            e = Exchange(name=event.exchange, type="topic")
            q = Queue(
                name=event.queue_name,
                exchange=e,
                routing_key=event.routing_key,
                durable=True,
                exclusive=False,
                auto_delete=False
            )
            c = connection.Consumer(
                queues=[q],
                callbacks=[event.callback]
            )
            c.consume()
            consumers.append(c)

        try:
            # XXX: drain_events only returns after receiving a message. Is
            # there a way we can have it return regularly to be non-blocking?
            # Its timeout parameter seems to break receiving of messages.
            # Maybe it doesn't matter if we can't shut down gracefully since
            # messages will be reprocessed next time.
            while self.running:
                connection.drain_events()
        finally:
            for c in consumers:
                c.close()
            connection.close()
