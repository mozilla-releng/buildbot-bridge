import json
from signal import signal, SIGTERM

from .services import BuildbotListener, TCListener, Reflector

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
        kwargs.update(config["bblistener"])
        service = BuildbotListener(
            pulse_host="pulse.mozilla.org",
            pulse_user=config["pulse_user"],
            pulse_password=config["pulse_password"],
            pulse_queue_basename=config["pulse_queue_basename"],
            **kwargs
        )
    elif args.service[0] == "reflector":
        kwargs.update(config["reflector"])
        service = Reflector(
            **kwargs
        )
    elif args.service[0] == "tclistener":
        kwargs.update(config["tclistener"])
        service = TCListener(
            pulse_host="pulse.mozilla.org",
            pulse_user=config["pulse_user"],
            pulse_password=config["pulse_password"],
            pulse_queue_basename=config["pulse_queue_basename"],
            **kwargs
        )
    else:
        raise ValueError("Couldn't find service to run!")

    def handle_sigterm(*args):
        log.info("Caught SIGTERM, shutting down...")
        # Calling service.stop() should let the service finish whatever it's
        # currently doing and then stop looping.
        service.stop()

    signal(SIGTERM, handle_sigterm)

    log.info("Running %s service", args.service[0])
    # TODO: If we're not going to run with supervisor or something similar,
    # this should probably daemonize instead.
    service.start()
