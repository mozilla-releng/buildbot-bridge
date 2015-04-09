import arrow


def parseDateString(datestring):
    """
    Parses a date string like 2015-02-13T19:33:37.075719Z and returns a unix epoch time
    """
    return arrow.get(datestring).timestamp
