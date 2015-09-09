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
    log.debug("Got %s", resp)
    if resp.get("storageType") != "s3":
        raise ValueError("Can't upload artifact for task %s with runid %s because storageType is wrong" % (taskid, runid))
    assert resp["storageType"] == "s3"
    put_url = resp["putUrl"]
    log.debug("Uploading to %s", put_url)
    for _ in retrier():
        try:
            resp = requests.put(put_url, data=data, headers={
                "Content-Type": "application/json",
                "Content-Length": len(data),
            })
            log.debug("Got %s %s", resp, resp.headers)
            return
        except Exception:
            log.debug("Error submitting to s3", exc_info=True)
            continue
    else:
        log.error("couldn't upload artifact to s3")
        raise IOError("couldn't upload artifact to s3")


def makeTaskId():
    """Used in testing to generate task ids without talking to TaskCluster."""
    return slugid.nice()
