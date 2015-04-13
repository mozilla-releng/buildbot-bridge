from base64 import b64encode
import json
from uuid import uuid4

from redo import retrier
import requests

import logging
log = logging.getLogger(__name__)


def createJsonArtifact(queue, taskId, runId, name, data, expires):
    data = json.dumps(data)
    resp = queue.createArtifact(taskId, runId, name, {
        "storageType": "s3",
        "contentType": "application/json",
        "expires": expires,
    })
    log.debug("Got %s", resp)
    assert resp["storageType"] == "s3"
    putUrl = resp["putUrl"]
    log.debug("Uploading to %s", putUrl)
    for _ in retrier():
        try:
            resp = requests.put(putUrl, data=data, headers={
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
    return b64encode(uuid4().bytes).replace("+", "-").replace("/", "-").rstrip("=")

