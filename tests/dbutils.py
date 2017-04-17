import asyncio
from sqlalchemy.schema import CreateTable

import bbb.db

async def make_bbb_db():
    await bbb.db._bbb_db.execute(CreateTable(bbb.db._bbb_tasks))


async def make_bb_db():
    await asyncio.wait([
        bbb.db._bb_db.execute(CreateTable(bbb.db._bb_requests)),
        bbb.db._bb_db.execute(CreateTable(bbb.db._bb_sourcestamps)),
        bbb.db._bb_db.execute(CreateTable(bbb.db._bb_buildsets)),
        bbb.db._bb_db.execute(CreateTable(bbb.db._bb_builds)),
    ])


async def create_dbs():
    await asyncio.wait([make_bb_db(), make_bbb_db()])
