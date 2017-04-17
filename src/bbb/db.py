import logging
import sqlalchemy as sa
from sqlalchemy_aio import ASYNCIO_STRATEGY
from statsd import StatsClient

import bbb

_bbb_tasks = None
_bb_requests = None
_bb_db = None
_bbb_db = None
_bb_sourcestamps = None
_bb_buildsets = None
_bb_builds = None

log = logging.getLogger(__name__)
statsd = StatsClient(prefix='bbb.db')


def init(bridge_uri, buildbot_uri):
    global _bbb_tasks, _bb_requests, _bb_db, _bbb_db, _bb_sourcestamps, \
        _bb_buildsets, _bb_builds

    _bbb_db = _get_engine(bridge_uri)
    _bb_db = _get_engine(buildbot_uri)

    _bbb_tasks = _get_bbb_tasks_table(_bbb_db)

    _bb_requests = _get_bb_requests_table(_bb_db)
    _bb_sourcestamps = _get_bb_sourcestamps_table(_bb_db)
    _bb_buildsets = _get_bb_buildsets_table(_bb_db)
    _bb_builds = _get_bb_builds_table(_bb_db)


@statsd.timer('delete_task_by_request_id')
async def delete_task_by_request_id(id_):
    if bbb.DRY_RUN:
        log.info("DRY RUN: delete_task_by_request_id(%s)", id_)
        return
    await _bbb_db.execute(_bbb_tasks.delete(_bbb_tasks.c.buildrequestId == id_))


@statsd.timer('fetch_all_tasks')
async def fetch_all_tasks():
    res = await _bbb_db.execute(_bbb_tasks.select().order_by(
        _bbb_tasks.c.takenUntil.desc()))
    return await res.fetchall()


@statsd.timer('get_cancelled_build_requests')
async def get_cancelled_build_requests(build_request_ids):
    q = _bb_requests.select(_bb_requests.c.id).where(
        _bb_requests.c.id.in_(build_request_ids)).where(
        _bb_requests.c.complete == 1).where(
        _bb_requests.c.claimed_at == 0)
    res = await _bb_db.execute(q)
    records = await res.fetchall()
    return [r[0] for r in records]


@statsd.timer('update_taken_until')
async def update_taken_until(request_id, taken_until):
    if bbb.DRY_RUN:
        log.info("DRY RUN: update_taken_until(%s, %s)",
                 request_id, taken_until)
        return
    await _bbb_db.execute(
        _bbb_tasks.update(_bbb_tasks.c.buildrequestId == request_id).values(
            takenUntil=taken_until))


@statsd.timer('get_branch')
async def get_branch(request_id):
    q = sa.select([_bb_sourcestamps.c.branch]).where(
        _bb_sourcestamps.c.id == _bb_buildsets.c.sourcestampid).where(
        _bb_buildsets.c.id == _bb_requests.c.buildsetid).where(
        _bb_requests.c.id == request_id)
    res = await _bb_db.execute(q)
    records = await res.first()
    return records[0].split("/")[-1]


@statsd.timer('get_build_id')
async def get_build_id(request_id):
    q = sa.select([_bb_builds.c.id]).where(_bb_builds.c.brid == request_id)
    res = await _bb_db.execute(q)
    records = await res.fetchall()
    return [r[0] for r in records]


@statsd.timer('get_build_request')
async def get_build_request(request_id):
    q = sa.select([_bb_requests]).where(_bb_requests.c.id == request_id)
    res = await _bb_db.execute(q)
    return await res.first()


def _get_engine(uri):
    if "mysql" in uri:
        db = sa.create_engine(uri, pool_size=1, pool_recycle=60,
                              pool_timeout=60, strategy=ASYNCIO_STRATEGY)
    else:
        db = sa.create_engine(uri, pool_size=1, pool_recycle=60,
                              strategy=ASYNCIO_STRATEGY)
    return db


def _get_bbb_tasks_table(engine):
    return sa.Table(
        'tasks',
        sa.MetaData(engine),
        sa.Column('buildrequestId', sa.Integer, primary_key=True),
        sa.Column('taskId', sa.String(32), index=True),
        sa.Column('runId', sa.Integer),
        sa.Column('createdDate', sa.Integer,
                  doc="When the task was submitted to TC"),
        sa.Column('processedDate', sa.Integer, doc="When we put it into BB"),
        sa.Column('takenUntil', sa.Integer, index=True,
                  doc="How long until our claim needs to be renewed")
    )


def _get_bb_requests_table(engine):
    return sa.Table(
        'buildrequests',
        sa.MetaData(engine),
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('buildsetid', sa.Integer),
        sa.Column('buildername', sa.String(256)),
        sa.Column('priority', sa.Integer),
        sa.Column('claimed_at', sa.Integer),
        sa.Column('claimed_by_name', sa.String(256)),
        sa.Column('claimed_by_incarnation', sa.String(256)),
        sa.Column('complete', sa.Integer),
        sa.Column('results', sa.Integer),
        sa.Column('submitted_at', sa.Integer),
        sa.Column('complete_at', sa.Integer)
    )


def _get_bb_sourcestamps_table(engine):
    return sa.Table(
        'sourcestamps',
        sa.MetaData(engine),
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('branch', sa.String(256)),
        sa.Column('revision', sa.String(256)),
        sa.Column('patchid', sa.Integer),
        sa.Column('repository', sa.String),
        sa.Column('project', sa.String)
    )


def _get_bb_buildsets_table(engine):
    return sa.Table(
        'buildsets',
        sa.MetaData(engine),
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('external_idstring', sa.String(256)),
        sa.Column('reason', sa.String(256)),
        sa.Column('sourcestampid', sa.Integer),
        sa.Column('submitted_at', sa.Integer),
        sa.Column('complete', sa.Integer),
        sa.Column('complete_at', sa.Integer),
        sa.Column('results', sa.Integer),
    )


def _get_bb_builds_table(engine):
    return sa.Table(
        'builds',
        sa.MetaData(engine),
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('number', sa.Integer),
        sa.Column('brid', sa.Integer),
        sa.Column('start_time', sa.Integer),
        sa.Column('finish_time', sa.Integer),
    )
