from contextlib import contextmanager
import json
import logging

from kazoo.client import KazooClient
from queue import FilteredLockingQueue


logger = logging.getLogger('gatlinggun')


class ZkTransport(object):

    def __init__(self, host, group, timeout=10, interval=2):
        self.group = group
        self.client = KazooClient(host)
        # what for is the 'start'?
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
            print "Consuming task"
            self.q.consume()
        except Exception as e:
            # maybe pass exception for invalid records in queue
            # can we try to unlock the task?
            raise

    def put(self, data):
        self.q.put(data)
