import sqlalchemy as sa

import logging
log = logging.getLogger(__name__)


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


class ServiceBase(object):
    """A base for all BBB services that manages access to the buildbot db,
       bbb db, and taskcluster."""
    def __init__(self, bbb_db, buildbot_db, tc_credentials):
        self.bbb_db = sa.create_engine(task_db)
        self.buildbot_db = sa.create_engine(task_db)
        self.tc_queue = Queue(tc_credentials)

        self.tasks_table = create_tasks_table(self.bbb_db)

    def deleteBuildrequest(self, brid):
        self.tasks_table.delete(self.tasks_table.c.buildrequestId == brid).execute()

    def updateRunId(self, brid, runId):
        self.tasks_table.update(self.tasks_table.c.buildrequestId == brid).values(runId=runId).execute()

    def start(self):
        raise NotImplementedError()


class ListenerService(object):
    """A base for BBB services that run in response to events from Pulse."""
    def __init__(self, user, password, exchange, topic, *args, **kwargs):
        super(ListenerService, self).__init__(*args, **kwargs)

        self.pulse_config = PulseConfiguration(
            user=user,
            password=password,
            # TODO: remove me
            ssl=False
        )
        self.exchange = exchange
        self.topic = topic
        self.pulse_consumer = GenericConsumer(self.pulse_config, durable=True, exchange=exchange)
        self.pulse_consumer.configure(topic=topic, callback=self.receivedMessage)

    def start(self):
        log.info("Listening for Pulse messages")
        log.debug("Exchange is %s", self.exchange)
        log.debug("Topic is %s", self.topic)
        self.pulse_consumer.listen()

    def receivedMessage(self, *args):
        raise NotImplementedError()
