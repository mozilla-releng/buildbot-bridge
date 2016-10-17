import json
import slugid

from redo import retrier
import requests

import logging
log = logging.getLogger(__name__)


def createJsonArtifact(queue, taskid, runid, name, data, expires):
    """Creates a Taskcluster artifact for the given taskid and runid, and
    uploads the artifact contents to location provided."""

    data = json.dumps(data)
    resp = queue.createArtifact(taskid, runid, name, {
        "storageType": "s3",
        "contentType": "application/json",
        "expires": expires,
    })
    log.debug("task %s: run %s: createArtifact returned %s", taskid, runid, resp)
    if resp.get("storageType") != "s3":
        raise ValueError("Can't upload artifact for task %s with runid %s because storageType is wrong" % (taskid, runid))
    assert resp["storageType"] == "s3"
    put_url = resp["putUrl"]
    log.debug("task %s: run %s: Uploading to %s", taskid, runid, put_url)
    for _ in retrier():
        try:
            resp = requests.put(put_url, data=data, headers={
                "Content-Type": "application/json",
                "Content-Length": len(data),
            })
            log.debug("task %s: run %s: Got %s %s", taskid, runid, resp, resp.headers)
            return
        except Exception:
            log.debug("task %s: run %s: Error submitting to s3", taskid, runid, exc_info=True)
            continue
    else:
        log.error("task %s: run %s: couldn't upload artifact to s3", taskid, runid)
        raise IOError("couldn't upload artifact to s3")


def makeTaskId():
    """Used in testing to generate task ids without talking to TaskCluster."""
    return slugid.nice()


def createReferenceArtifact(queue, taskid, runid, name, url, expires,
                            content_type):
    """Creates a Taskcluster reference artifact for the given taskid/runid"""
    queue.createArtifact(taskid, runid, name, {
        "storageType": "reference",
        "contentType": content_type,
        "expires": expires,
        "url": url
    })
