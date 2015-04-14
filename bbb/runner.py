import json

from .services.bblistener import BuildbotListener
from .services.reflector import Reflector
from .services.tclistener import TCListener

import logging
log = logging.getLogger(__name__)


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.set_defaults(
        loglevel=logging.INFO
    )
    parser.add_argument("-v", "--verbose", dest="loglevel", action="store_const", const=logging.DEBUG)
    parser.add_argument("-q", "--quiet", dest="loglevel", action="store_const", const=logging.WARN)
    parser.add_argument("-c", "--config", dest="config", required=True)
    parser.add_argument("service", nargs=1, choices=["bblistener", "reflector", "tclistener"])

    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel, format="%(asctime)s - %(name)s - %(message)s")
    logging.getLogger("bbb").setLevel(args.loglevel)

    config = json.load(open(args.config))

    kwargs = {
        "bbb_db": config["bbb_db"],
        "buildbot_db": config["buildbot_scheduler_db"],
        "tc_config": config["taskcluster_queue_config"],
    }
    if args.service[0] == "bblistener":
        service = BuildbotListener(
            pulse_user=config["pulse_user"],
            pulse_password=config["pulse_password"],
            exchange=config["buildbot_pulse_exchange"],
            topic=config["buildbot_pulse_topic"],
            applabel=config["buildbot_pulse_applabel"],
            tc_worker_group=config["taskcluster_worker_group"],
            tc_worker_id=config["taskcluster_worker_id"],
            **kwargs
        )
    elif args.service[0] == "reflector":
        service = Reflector(
            interval=config["reclaim_interval"],
            **kwargs
        )
    elif args.service[0] == "tclistener":
        service = TCListener(
            pulse_user=config["pulse_user"],
            pulse_password=config["pulse_password"],
            exchange=config["taskcluster_pulse_exchange"],
            topic=config["taskcluster_pulse_topic"],
            applabel=config["taskcluster_pulse_applabel"],
            **kwargs
        )
    else:
        raise ValueError("Couldn't find service to run!")
    log.info("Running %s service", args.service[0])
    # TODO: If we're not going to run with supervisor or something similar,
    # this should probably daemonize instead.
    while True:
        try:
            service.start()
        except KeyboardInterrupt:
            raise
        except:
            log.exception("Caught exception:")
            # TODO: Sleep here? The Reflector reloads rapidly if it hits an Exception
