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
