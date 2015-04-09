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


class BuildbotBridge(object):
    default_config = {
        'taskcluster_pulse_exchange': 'exchange/taskcluster-queue/v1/task-pending',
        'taskcluster_pulse_topic': '*.*.*.*.*.*.test-buildbot.#',
        'buildbot_pulse_exchange': 'exchange/build',
        'buildbot_pulse_topic': '#',
        "pulse_host": "pulse.mozilla.org",
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
        task = self.taskcluster_queue.task(taskId)
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
            host=self.config["pulse_host"],
            # TODO: remove me
            ssl=False
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
            host=self.config["pulse_host"],
            # TODO: remove me
            ssl=False
        )
        pulse_exchange = self.config['buildbot_pulse_exchange']
        pulse_topic = self.config['buildbot_pulse_topic']

        self.pulse_consumer = GenericConsumer(pulse_config, exchange=pulse_exchange)
        self.pulse_consumer.configure(topic=pulse_topic, callback=self.receivedBBMessage)
        log.info("listening for pulse messages on %s/%s...", pulse_exchange, pulse_topic)
        self.pulse_consumer.listen()

    def receivedBBMessage(self, data, msg):
        log.debug("got %s %s", data, msg)
        event = data["_meta"]["routing_key"].split(".")[-1]
        # TODO: is this accurate? are there any log_uploaded specific properties
        # that matter? maybe log_url?
        # We can't use the "finished" event because "log_uploaded" contains extra
        # information in properties that we want to pass along
        if event not in ("started", "log_uploaded"):
            log.debug("Skipping event because it's not started or log_uploaded")
            return

        if event == "started":
            try:
                msg.ack()
                buildnumber = data["payload"]["build"]["number"]
                brids = self.buildbot_db.execute(
                    sa.text("select buildrequests.id from buildrequests join builds ON buildrequests.id=builds.brid where builds.number=:buildnumber"),
                    buildnumber=buildnumber
                ).fetchall()

                for brid in brids:
                    brid = brid[0]
                    taskId, runId = self.getTaskId(brid)
                    log.info("claiming %s", taskId)
                    claim = self.taskcluster_queue.claimTask(taskId, runId, {
                        "workerGroup": self.config["taskcluster_worker_group"],
                        "workerId": self.config["taskcluster_worker_id"],
                    })
                    log.debug("claim: %s", claim)
                    self.tasks_table.update(self.tasks_table.c.buildrequestId==brid).values(
                        takenUntil=claim["takenUntil"]
                    ).execute()
            except:
                log.exception("problem claiming task; can't proceed")
                # XXX: Reporting an exception if we were unable to claim may lead
                # to inconsistent state between buildbot and TC. No matter what
                # happens with the claim, Buildbot will retry the build, so if
                # we report an exception here in that case, TC will think the
                # build has had an exception even though it hasn't. However,
                # the reaper _should_ fix the status after the build has completed.
                self.taskcluster_queue.reportException(taskId, runId, {"reason": "malformed-payload"})
                raise

        elif event == "log_uploaded":
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

                # SUCCESS
                if results == 0:
                    log.info("marking task %s as completed", taskId)
                    self.taskcluster_queue.reportCompleted(taskId, runId, {'success': True})
                    self.deleteBuildrequest(brid)
                # WARNINGS or FAILURE
                # Eventually we probably need to set something different here.
                elif results in (1, 2):
                    log.info("marking task %s as failed", taskId)
                    self.taskcluster_queue.reportFailed(taskId, runId)
                    self.deleteBuildrequest(brid)
                # SKIPPED - not a valid Build status
                elif results == 3:
                    pass
                # EXCEPTION
                elif results == 4:
                    log.info("marking task %s as malformed payload exception", taskId)
                    self.taskcluster_queue.reportException(taskId, runId, {"reason": "malformed-payload"})
                    self.deleteBuildrequest(brid)
                # RETRY
                elif results == 5:
                    log.info("marking task %s as malformed payload exception and rerunning", taskId)
                    self.taskcluster_queue.reportException(taskId, runId, {"reason": "malformed-payload"})
                    self.taskcluster_queue.rerunTask(taskId)
                # CANCELLED
                elif results == 6:
                    log.info("marking task %s as cancelled", taskId)
                    self.taskcluster_queue.cancelTask(taskId)
                    self.deleteBuildrequest(brid)

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
            # If takenUntil isn't set, this task has either never been claimed
            # or got cancelled.
            if not t.takenUntil:
                # If the buildrequest is showing complete, it was cancelled
                # before it ever started, so we need to pass that along to
                # taskcluster. Ideally, we'd watch Pulse for ontification of
                # this, but our version of Buildbot has a bug that causes it
                # not to send those messages.
                if buildrequest.complete:
                    log.debug("Buildrequest disappeared, cancelling task.")
                    self.taskcluster_queue.cancelTask(t.taskId)
                    self.deleteBuildrequest(t.buildrequestId)
                    return
                # Otherwise we're just waiting for it to start, nothing to do.
                else:
                    log.debug("Build hasn't started, doing nothing.")
            elif buildrequest.complete:
                # TODO: delete from tasks table?
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
    while True:
        try:
            action()
        except KeyboardInterrupt:
            raise
        except:
            log.exception("Caught exception:")

if __name__ == '__main__':
    main()
