import sqlalchemy as sa

def makeSchedulerDb(db):
    db.execute(sa.text("""
CREATE TABLE buildrequests (
                `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                `buildsetid` INTEGER NOT NULL,
                `buildername` VARCHAR(256) NOT NULL,
                `priority` INTEGER NOT NULL default 0,
                `claimed_at` INTEGER default 0,
                `claimed_by_name` VARCHAR(256) default NULL,
                `claimed_by_incarnation` VARCHAR(256) default NULL,
                `complete` INTEGER default 0,
                `results` SMALLINT,
                `submitted_at` INTEGER NOT NULL,
                `complete_at` INTEGER
            );
"""))
    db.execute(sa.text("""
CREATE TABLE builds (
                `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                `number` INTEGER NOT NULL,
                `brid` INTEGER NOT NULL,
                `start_time` INTEGER NOT NULL,
                `finish_time` INTEGER
            );
"""))
    db.execute(sa.text("""
CREATE TABLE sourcestamps (
                `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                `branch` VARCHAR(256) default NULL,
                `revision` VARCHAR(256) default NULL,
                `patchid` INTEGER default NULL,
                `repository` TEXT not null default '',
                `project` TEXT not null default ''
            );
"""))
    db.execute(sa.text("""
CREATE TABLE buildset_properties (
    `buildsetid` INTEGER NOT NULL,
    `property_name` VARCHAR(256) NOT NULL,
    `property_value` VARCHAR(1024) NOT NULL
);
"""))
    db.execute(sa.text("""
CREATE TABLE buildsets (
                `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                `external_idstring` VARCHAR(256),
                `reason` VARCHAR(256),
                `sourcestampid` INTEGER NOT NULL,
                `submitted_at` INTEGER NOT NULL,
                `complete` SMALLINT NOT NULL default 0,
                `complete_at` INTEGER,
                `results` SMALLINT
            );
"""))

