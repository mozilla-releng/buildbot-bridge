"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will cause
  problems: the code will get executed twice:

  - When you run `python -mbbb` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``bbb.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``bbb.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import asyncio
import logging

import click
import yaml

import bbb
import bbb.db
import bbb.reflector
import bbb.selfserve
import bbb.taskcluster

log = logging.getLogger(__name__)


@click.command()
@click.option('--config', type=click.File('rb'), required=True,
              help='YAML Config file')
def reflector(config):
    cfg = yaml.safe_load(config)

    log_level = logging.INFO
    if cfg["bbb"].get("verbose"):
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level, format="%(name)s - %(message)s")
    logging.getLogger("taskcluster").setLevel(logging.WARNING)

    if cfg["bbb"].get("dry-run"):
        log.info("DRY RUN MODE")
        bbb.DRY_RUN = True

    if cfg["bbb"].get("dry-run-reclaim"):
        log.info("RECLAIM DRY RUN MODE")
        bbb.DRY_RUN_RECLAIM = True

    reclaim_threshold = cfg["bbb"].get("reclaim-threshold")
    if reclaim_threshold:
        log.info("RECLAIM_THRESHOLD: %s", reclaim_threshold)
        bbb.RECLAIM_THRESHOLD = reclaim_threshold

    bbb.db.init(bridge_uri=cfg["bbb"]["uri"], buildbot_uri=cfg["bb"]["uri"])
    bbb.taskcluster.init(
        options={"credentials": cfg["taskcluster"]["credentials"]})
    bbb.selfserve.init(cfg["selfserve"]["api_root"])

    loop = asyncio.get_event_loop()
    while True:
        loop.run_until_complete(bbb.reflector.main_loop(
            cfg["bbb"]["poll_interval"]))
