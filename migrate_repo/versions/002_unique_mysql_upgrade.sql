ALTER TABLE tasks MODIFY COLUMN buildrequestId INT;
ALTER TABLE tasks DROP PRIMARY KEY;
-- Run the same statement again to set NULL
ALTER TABLE tasks MODIFY COLUMN buildrequestId INT NULL;
CREATE INDEX ix_tasks_buildrequestId ON tasks (buildrequestId);
ALTER TABLE tasks MODIFY COLUMN taskId VARCHAR(32) NOT NULL;
ALTER TABLE tasks MODIFY COLUMN runId INT NOT NULL;
ALTER TABLE tasks ADD CONSTRAINT UNIQUE uc_tasks_taskId_runId (taskId, runId);
