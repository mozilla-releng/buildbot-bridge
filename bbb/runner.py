import json
from signal import signal, SIGTERM

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
    parser.add_argument("-c", "--config", dest="config", default="config.ini", required=True)
    parser.add_argument("service", nargs=1, choices=["bblistener", "reflector", "tclistener"])

    args = parser.parse_args()

    config = json.load(open(args.config))

    logfile = config[args.service[0]]["logfile"]
    if not logfile:
        logfile = "%s.log" % args.service
    debug_logfile = "debug-%s" % logfile

    logging_format = "%(asctime)s - %(name)s - %(message)s"
    logging.basicConfig(filename=logfile, level=args.loglevel, format=logging_format)

    debug_log = logging.FileHandler(filename=debug_logfile)
    debug_log.setLevel(logging.DEBUG)
    formatter = logging.Formatter(logging_format)
    debug_log.setFormatter(formatter)
    logging.getLogger('').addHandler(debug_log)

    logging.getLogger("bbb").setLevel(args.loglevel)

    # These need to be imported after setting up logging, otherwise they won't
    # inherit the settings properly.
    from .services import BuildbotListener, TCListener, Reflector

    kwargs = {
        "bbb_db": config["bbb_db"],
        "buildbot_db": config["buildbot_scheduler_db"],
        "tc_config": config["taskcluster_queue_config"],
    }
    if args.service[0] == "bblistener":
        service = BuildbotListener(
            pulse_host="pulse.mozilla.org",
            pulse_user=config["pulse_user"],
            pulse_password=config["pulse_password"],
            pulse_queue_basename=config["pulse_queue_basename"],
            pulse_exchange=config["bblistener"]["pulse_exchange"],
            tc_worker_group=config["tc_worker_group"],
            tc_worker_id=config["tc_worker_id"],
            **kwargs
        )
    elif args.service[0] == "reflector":
        service = Reflector(
            interval=config["reflector"]["interval"],
            selfserve_url=config["selfserve_url"],
            **kwargs
        )
    elif args.service[0] == "tclistener":
        service = TCListener(
            pulse_host="pulse.mozilla.org",
            pulse_user=config["pulse_user"],
            pulse_password=config["pulse_password"],
            pulse_queue_basename=config["pulse_queue_basename"],
            pulse_exchange_basename=config["tclistener"]["pulse_exchange_basename"],
            selfserve_url=config["selfserve_url"],
            worker_type=config["tclistener"]["worker_type"],
            provisioner_id=config["tclistener"]["provisioner_id"],
            worker_group=config["tc_worker_group"],
            worker_id=config["tc_worker_id"],
            restricted_builders=config["restricted_builders"],
            ignored_builders=config["ignored_builders"],
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
    try:
        service.start()
    except KeyboardInterrupt:
        service.stop()
