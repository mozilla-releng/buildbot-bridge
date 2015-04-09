from ..servicebase import ListenerService

import logging
log = logging.getLogger(__name__)


class BuildbotListener(ListenerService):
    def __init__(self, *args, **kwargs):
        eventHandlers = {
            "started": self.handleStarted,
            "log_uploaded": self.handleFinished,
        }
        super(BuildbotListener, self).__init__(*args, eventHandlers=eventHandlers, **kwargs)

    def getEvent(self, data):
        return data["_meta"]["routing_key"].split(".")[-1]

    def handleStarted(self, data, msg):
        # TODO: Implement me
        pass

    def handleFinished(self, data, msg):
        # TODO: Implement me
        pass
