import sqlalchemy as sa

meta = sa.MetaData()
tasks = sa.Table(
    'tasks', meta,
    sa.Column('buildrequestId', sa.Integer, primary_key=True),
    sa.Column('taskId', sa.String(32), index=True),
    sa.Column('runId', sa.Integer),
    sa.Column('createdDate', sa.Integer),
    sa.Column('processedDate', sa.Integer),
    sa.Column('takenUntil', sa.Integer, index=True),
)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    tasks.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    tasks.drop()
