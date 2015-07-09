from contextlib import contextmanager
from time import sleep

from kazoo.client import KazooClient
from kazoo.exceptions import SessionExpiredError
import msgpack

from queue import FilteredLockingQueue
from errors import ConnectionError, InvalidDataError


class ZkTransport(object):

    CONSUME_RETRIES = 2
    SESSION_RESTORE_PAUSE = 0.5

    def __init__(self, groups, path, host='127.0.0.1:2181', timeout=10, interval=2):
        self.groups = groups
        self.client = KazooClient(host)
        self.client.start()
        self.q = FilteredLockingQueue(self.client, path, self.__filter)
        self.timeout = timeout
        self.interval = interval

    def __filter(self, data):
        try:
            d = msgpack.unpackb(data)
            if d['group'] in self.groups:
                return True
        except:
            pass
        return False

    @contextmanager
    def item(self):
        try:
            if self.groups:
                task = self.q.get(self.timeout)
                yield task
                self.retry(self.q.consume, self.CONSUME_RETRIES)
            else:
                yield
        except ConnectionError:
            # in case of connection error we should retry the task execution
            raise
        except InvalidDataError:
            # in case of invalid data we can safely consume the item
            self.retry(self.q.consume, self.CONSUME_RETRIES)
            raise
        except Exception as e:
            self.q.unlock()
            raise

    def retry(self, func, retries):
        for i in xrange(retries):
            try:
                func()
                break
            except SessionExpiredError:
                # trying to restore session
                sleep(self.SESSION_RESTORE_PAUSE)
                continue
        else:
            raise SessionExpiredError

    def put(self, data):
        self.q.put(data)
