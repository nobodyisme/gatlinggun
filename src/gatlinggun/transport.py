from contextlib import contextmanager
import json
import logging

from kazoo.client import KazooClient
from queue import FilteredLockingQueue

from errors import ConnectionError, InvalidDataError

logger = logging.getLogger('gatlinggun')


class ZkTransport(object):

    def __init__(self, host='127.0.0.1:2181', group=0, timeout=10, interval=2):
        self.group = group
        self.client = KazooClient(host)
        self.client.start()
        self.q = FilteredLockingQueue(self.client, '/cache', self.__filter)
        self.timeout = timeout
        self.interval = interval

    def __filter(self, data):
        try:
            d = json.loads(data)
            print 'Parsed task data: %s' % (str(d))
            if self.group in d['dgroups']:
                return True
        except:
            pass
        return False

    @contextmanager
    def item(self):
        try:
            task = self.q.get(self.timeout)
            yield task
            self.q.consume()
        except ConnectionError:
            # in case of connection error we should retry the task execution
            raise
        except InvalidDataError:
            # in case of invalid data we can safely consume the item
            self.q.consume()
            raise
        except Exception as e:
            # can we try to unlock the task?
            self.q.unlock()
            raise

    def put(self, data):
        self.q.put(data)
