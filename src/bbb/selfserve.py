import logging
import aiohttp
from statsd import StatsClient

import bbb
log = logging.getLogger(__name__)
statsd = StatsClient(prefix='bbb.selfserve')

_api_root = None
_session = None
_tcp_limit = 10


def init(api_root, tcp_limit=_tcp_limit):
    global _api_root, _session, _tcp_limit
    _api_root = api_root
    _tcp_limit = tcp_limit
    conn = aiohttp.TCPConnector(limit=_tcp_limit)
    _session = aiohttp.ClientSession(connector=conn)


async def _do_request(method, url):
    # The private BuildAPI interface we use doesn't require auth but it
    # _does_ require REMOTE_USER to be set.
    # https://bugzilla.mozilla.org/show_bug.cgi?id=1156810 has additional
    # background on this.
    url = "%s/%s" % (_api_root, url)
    log.debug("Making %s request to %s", method, url)
    r = await _session.request(method, url,
                               headers={"X-Remote-User": "buildbot-bridge"})
    r.raise_for_status()


@statsd.timer('cancel_build')
async def cancel_build(branch, build_id):
    url = "%s/build/%s" % (branch, build_id)
    log.info("Cancelling build: %s", url)
    if bbb.DRY_RUN:
        log.info("DRY RUN: cancel_build(%s, %s)", branch, build_id)
        return
    await _do_request("DELETE", url)
