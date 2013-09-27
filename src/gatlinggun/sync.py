import json
import logging
import time
import threading

import elliptics
import msgpack
from cocaine.services import Service

from gatlinggun.gun import Gun


class PeriodThread(threading.Thread):

    def __init__(self, job, period, daemon=True, *args, **kwargs):
        super(PeriodThread, self).__init__(*args, **kwargs)
        self.daemon = daemon
        self.period = period
        self.job = job

    def run(self):
        while True:
            try:
                self.job()
            except Exception as e:
                logging.error('Job failed: %s' % e)
                pass
            time.sleep(self.period)


class Synchronizer(object):

    def __init__(self, node, group, transport):
        self.service = Service('mastermind')
        self.node = node
        self.group = group
        self.transport = transport

    def sync_keys(self):
        logging.info('Group %s: keys sync started' % self.group)

        keys = self.service.enqueue('get_cached_keys_by_group', msgpack.packb(self.group)).get()
        if not keys:
            return
        logging.info('Syncing keys for group %s' % self.group)

        self.__sync_uploaded_keys(keys)

        logging.info('Group %s: keys sync completed' % self.group)

    def __sync_uploaded_keys(self, keys):
        s = elliptics.Session(self.node)
        s.add_groups([self.group])

        for key in keys:
            try:
                s.lookup(elliptics.Id(key['key']))
            except elliptics.NotFoundError:
                logging.info('Key %s is missing, adding download task' % key['key'])

                # put download key task to task queue
                task = {'key': key['key'],
                        'sgroups': key['sgroups'],
                        'dgroups': [self.group],
                        'action': Gun.DISTRUBUTE_TASK_ACTION}
                self.transport.put(json.dumps(task))
                pass

            except elliptics.TimeoutError:
                logging.info('Sync keys: timeout for key %s' % key['key'])
                pass


if __name__ == '__main__':
    def test():
        print "shit"
    th = PeriodThread(test, 5)
    th.daemon = True
    th.start()
    time.sleep(30)

