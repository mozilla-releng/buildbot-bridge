from collections import namedtuple
import json
import time
from urlparse import urlparse
from contextlib import contextmanager

import arrow
from kombu import Connection, Queue, Exchange
import requests
import sqlalchemy as sa
import taskcluster

from .timeutils import parseDateString

import logging
log = logging.getLogger(__name__)


ListenerServiceEvent = namedtuple("ListenerServiceEvent", ("exchange", "routing_key", "callback", "queue_name"))


@contextmanager
def lock_table(db, table_name):

    try:
        if "mysql" in db.url.get_backend_name():
            db.execute(sa.text("LOCK TABLE {} WRITE;".format(table_name)))
        yield
    finally:
        if "mysql" in db.url.get_backend_name():
            db.execute(sa.text("UNLOCK TABLES;"))


class SelfserveClient(object):
    def __init__(self, base_uri):
        self.base_uri = base_uri

    def _do_request(self, method, url):
        # The private BuildAPI interface we use doesn't require auth but it
        # _does_ require REMOTE_USER to be set.
        # https://bugzilla.mozilla.org/show_bug.cgi?id=1156810 has additional
        # background on this.
        url = "%s/%s" % (self.base_uri, url)
        log.debug("Making %s request to %s", method, url)
        r = requests.request(method, url, headers={"X-Remote-User": "buildbot-bridge"})
        r.raise_for_status()

    def cancelBuild(self, branch, id_):
        url = "%s/build/%s" % (branch, id_)
        self._do_request("DELETE", url)

    def cancelBuildRequest(self, branch, brid):
        # TODO: these (and maybe builds) will get 404s if cancellation happens
        # too soon after scheduling. not sure why, maybe it takes time for buildapi
        # or something else to become aware of their existence
        url = "%s/request/%s" % (branch, brid)
        self._do_request("DELETE", url)


class TaskNotFound(Exception):
    pass


class BBBDb(object):
    """Wrapper object for creation of and access to Buildbot Bridge database."""
    def __init__(self, uri):
        # pool_size is set to 1 because each Bridge service is single threaded, so
        # there's no reason to use more than one connection per app.
        # pool_recycle is set to 60 to avoid MySQL timing out connections after
        # awhile. Without it we'd get mysql disconnections after long idle periods.
        # pool_timeout will cause SQLAlchemy to wait longer before giving up on new
        # connections to the server. It's not strictly necessary, but it doesn't hurt.
        #
        # MySQLdb's default cursor doesn't let you stream rows as you receive them.
        # Even if you use SQLAlchemy iterators, _all_ rows will be read before any
        # are returned. The SSCursor lets you stream data, but has the side effect
        # of not allowing multiple queries on the same Connection at the same time.
        # That's OK for us because each service is single threaded, but it means
        # we must ensure that "close" is called whenever we do queries. "fetchall"
        # does this automatically, so we always use it.
        if "mysql" in uri:
            from MySQLdb.cursors import SSCursor
            self.db = sa.create_engine(uri, pool_size=1, pool_recycle=60, pool_timeout=60,
                                       connect_args={"cursorclass": SSCursor})
        else:
            self.db = sa.create_engine(uri, pool_size=1, pool_recycle=60)

        metadata = sa.MetaData(self.db)
        self.tasks_table = sa.Table(
            'tasks', metadata,
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
        for t in self.tasks_table.select().execute():
            log.debug("Yielding %s", t)
            yield t

    def getTask(self, taskid):
        log.info("task %s: fetching task from bbbdb", taskid)
        task = self.tasks_table.select(self.tasks_table.c.taskId == taskid).execute().fetchall()
        if task:
            task = task[0]
        log.debug("task %s: %s", taskid, task)
        return task

    def getTaskFromBuildRequest(self, brid):
        task = self.tasks_table.select(self.tasks_table.c.buildrequestId == brid).execute().fetchall()
        if not task:
            raise TaskNotFound("Couldn't find task for brid %i", brid)
        return task[0]

    def createTask(self, taskid, runid, brid, created_date):
        log.info("task %s: creating task", taskid)
        log.debug("task %s: runId: %s, brid: %s, created: %s", taskid, runid, brid, created_date)
        self.tasks_table.insert().values(
            taskId=taskid,
            runId=runid,
            buildrequestId=brid,
            createdDate=created_date,
            processedDate=arrow.now().timestamp,
        ).execute()

    def deleteBuildRequest(self, brid):
        log.info("buildrequest %s: deleting task from bbbdb", brid)
        self.tasks_table.delete(self.tasks_table.c.buildrequestId == brid).execute()

    def updateRunId(self, brid, runid):
        log.info("buildrequest %s: updating runId to %s", brid, runid)
        self.tasks_table.update(self.tasks_table.c.buildrequestId == brid).values(runId=runid).execute()

    def updateTakenUntil(self, brid, taken_until):
        log.debug("buildrequest %s: updating takenUntil to %s", brid, taken_until)
        self.tasks_table.update(self.tasks_table.c.buildrequestId == brid).values(takenUntil=taken_until).execute()


class BuildbotDb(object):
    """Wrapper object for access to preexisting Buildbot scheduler database."""
    def __init__(self, uri, init_func=None):
        # pool_size is set to 1 because each Bridge service is single threaded, so
        # there's no reason to use more than one connection per app.
        # pool_recycle is set to 60 to avoid MySQL timing out connections after
        # awhile. Without it we'd get mysql disconnections after long idle periods.
        # pool_timeout will cause SQLAlchemy to wait longer before giving up on new
        # connections to the server. It's not strictly necessary, but it doesn't hurt.
        #
        # MySQLdb's default cursor doesn't let you stream rows as you receive them.
        # Even if you use SQLAlchemy iterators, _all_ rows will be read before any
        # are returned. The SSCursor lets you stream data, but has the side effect
        # of not allowing multiple queries on the same Connection at the same time.
        # That's OK for us because each service is single threaded, but it means
        # we must ensure that "close" is called whenever we do queries. "fetchall"
        # does this automatically, so we always use it.
        if "mysql" in uri:
            from MySQLdb.cursors import SSCursor
            self.db = sa.create_engine(uri, pool_size=1, pool_recycle=60, pool_timeout=60,
                                       connect_args={"cursorclass": SSCursor})
        else:
            self.db = sa.create_engine(uri, pool_size=1, pool_recycle=60)

        if init_func:
            init_func(self.db)

        metadata = sa.MetaData(self.db)
        metadata.reflect()
        self.buildrequests_table = metadata.tables["buildrequests"]
        self.builds_table = metadata.tables["builds"]
        self.sourcestamps_table = metadata.tables["sourcestamps"]
        self.buildset_properties_table = metadata.tables["buildset_properties"]
        self.buildsets_table = metadata.tables["buildsets"]

    def isBuildRequestComplete(self, brid):
        q = sa.select([self.buildrequests_table.c.complete])\
              .where(self.buildrequests_table.c.id == brid)
        return bool(self.db.execute(q).fetchall()[0][0])

    def getBuildRequests(self, buildnumber, buildername, claimed_by_name, claimed_by_incarnation):
        now = time.time()
        # TODO: Using complete=0 sucks a bit. If builds complete before we process
        # the build started event, this query doesn't work.
        q = sa.select([self.buildrequests_table.c.id])\
              .where(self.builds_table.c.number == buildnumber)\
              .where(self.buildrequests_table.c.id == self.builds_table.c.brid)\
              .where(self.buildrequests_table.c.complete == 0)\
              .where(self.buildrequests_table.c.buildername == buildername)\
              .where(self.buildrequests_table.c.claimed_by_name == claimed_by_name)\
              .where(self.buildrequests_table.c.claimed_by_incarnation == claimed_by_incarnation)
        ret = self.db.execute(q).fetchall()
        log.debug("getBuildRequests Query took %f seconds", time.time() - now)
        return ret

    def getBuildsCount(self, brid):
        return self.builds_table.count().where(self.builds_table.c.brid == brid).execute().fetchall()[0][0]

    def getBuildIds(self, brid):
        q = sa.select([self.builds_table.c.id])\
              .where(self.builds_table.c.brid == brid)
        return [row[0] for row in self.db.execute(q).fetchall()]

    def getBranch(self, brid):
        q = sa.select([self.sourcestamps_table.c.branch])\
              .where(self.sourcestamps_table.c.id == self.buildsets_table.c.sourcestampid)\
              .where(self.buildsets_table.c.id == self.buildrequests_table.c.buildsetid)\
              .where(self.buildrequests_table.c.id == brid)
        r = self.db.execute(q).fetchall()
        if r:
            return r[0][0]
        else:
            return None

    def createSourceStamp(self, sourcestamp={}):
        branch = sourcestamp.get('branch').rstrip("/")
        # Branches from Taskcluster usually come in as a full URL.
        # Sourcestamps need the "short" version of the branch, which is
        # path component of the URL. Eg: "integration/mozilla-inbound"
        if "://" in branch:
            branch = urlparse(branch).path.strip("/")
        revision = sourcestamp.get('revision')
        repository = sourcestamp.get('repository', '')
        project = sourcestamp.get('project', '')

        q = self.sourcestamps_table.insert().values(
            branch=branch,
            revision=revision,
            repository=repository,
            project=project,
        )
        r = self.db.execute(q)
        ssid = r.lastrowid
        log.info("Created sourcestamp %s", ssid)

        # TODO: Create change objects, files, etc.
        return ssid

    def createBuildSetProperties(self, buildsetid, properties):
        q = self.buildset_properties_table.insert().values(
            buildsetid=buildsetid
        )
        props = {}
        props.update(((k, json.dumps((v, "bbb"))) for (k, v) in properties.iteritems()))
        for key, value in props.items():
            self.db.execute(q, property_name=key, property_value=value)
            log.info("Created buildset_property %s=%s", key, value)

    def injectTask(self, taskid, runid, task):
        payload = task["payload"]
        # Create a sourcestamp if necessary
        sourcestamp = payload.get('sourcestamp', {})

        sourcestampid = self.createSourceStamp(sourcestamp)

        # TODO: submitted_at should be now, or the original task?
        # using orginal task's date for now
        submitted_at = parseDateString(task['created'])
        q = self.buildsets_table.insert().values(
            external_idstring="taskId:{}".format(taskid),
            reason="Created by BBB for task {0}".format(taskid),
            sourcestampid=sourcestampid,
            submitted_at=submitted_at,
            complete=0,
        )
        r = self.db.execute(q)

        buildsetid = r.lastrowid
        log.info("Created buildset %i", buildsetid)

        # Create properties
        properties = payload.get('properties', {})
        # Always create a property for the taskId
        properties['taskId'] = taskid
        self.createBuildSetProperties(buildsetid, properties)

        # Create the buildrequest
        buildername = payload['buildername']
        priority = 0
        if task.get("priority") == "high":
            priority = 1
        q = self.buildrequests_table.insert().values(
            buildsetid=buildsetid,
            buildername=buildername,
            submitted_at=submitted_at,
            priority=priority,
            claimed_at=0,
            complete=0,
        )
        r = self.db.execute(q)
        log.info("buildrequest %s: created for builder %s", r.lastrowid, buildername)
        return r.lastrowid


class ServiceBase(object):
    """A base for all BBB services that manages access to the buildbot db,
       bbb db, and taskcluster."""
    def __init__(self, bbb_db, buildbot_db, tc_config, buildbot_db_init_func=None):
        self.bbb_db = BBBDb(bbb_db)
        self.buildbot_db = BuildbotDb(buildbot_db, buildbot_db_init_func)
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
            ssl=True,
            # Kombu doesn't support the port correctly for amqp with ssl...
            port=5671,
        )
        consumers = []
        for event in self.events:
            log.debug("Setting up queue on exchange: %s with routing_key: %s", event.exchange, event.routing_key)
            # Passive exchanges must be used, otherwise kombu will try to
            # create the exchange (which we don't want, we're consuming
            # an existing one!)
            e = Exchange(name=event.exchange, type="topic", passive=True)
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
