#!/usr/bin/env python

import json

from bbb.services.bblistener import BuildbotListener
from bbb.services.reclaimer import Reclaimer
from bbb.services.tclistener import TCListener

import logging
log = logging.getLogger(__name__)


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser
    parser.set_defaults(
        loglevel=logging.INFO
    )
    parser.add_argument("-v", "--verbose", dest="loglevel", action="store_const", const=logging.DEBUG)
    parser.add_argument("-q", "--quiet", dest="loglevel", action="store_const", const=logging.WARN)
    parser.add_argument("-c", "--config", dest="config", required=True)
    parser.add_argument("service", dest="service", nargs=1, choices=["bblistener", "reclaimer", "tclistener"], required=True)

    args = parser.parse_args()

    logging.basicConfig(level=logging.WARN, format="%(asctime)s - %(name)s - %(message)s")
    log.setLevel(args.loglevel)

    config = json.load(open(args.config))

    kwargs = {
        "bbb_db": config["bbb_db"],
        "buildbot_db": config["buildbot_scheduler_db"],
        "tc_credentials": config["taskcluster_credentials"]
    }
    if args.action == "bblistener":
        service = BuildbotListener(
            pulse_user=config["pulse_user"],
            pulse_password=config["pulse_password"],
            exchange=config["buildbot_pulse_exchange"],
            topic=config["buildbot_pulse_topic"],
            tcWorkerGroup=config["taskcluster_worker_group"],
            tcWorkerId=config["taskcluster_worker_id"],
            **kwargs
        )
    elif args.action == "reclaimer":
        service = Reclaimer(
            interval=config["reclaim_interval"]
            **kwargs
        )
    elif args.action == "tclistener":
        service = TCListener(
            pulse_user=config["pulse_user"],
            pulse_password=config["pulse_password"],
            exchange=config["taskcluster_pulse_exchange"],
            topic=config["taskcluster_pulse_topic"],
            **kwargs
        )
    else:
        raise ValueError("Couldn't find service to run!")
    log.info("Running %s service", args.action)
    # TODO: If we're not going to run with supervisor or something similar,
    # this should probably daemonize instead.
    while True:
        try:
            service.start()
        except KeyboardInterrupt:
            raise
        except:
            log.exception("Caught exception:")


if __name__ == "__main__":
    main()
