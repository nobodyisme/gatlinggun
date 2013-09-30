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
        self.__sync_removed_keys(keys)

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

    ALL_KEYS = elliptics.IteratorRange()
    ALL_KEYS.key_begin = elliptics.Id([0] * 64, 0)
    ALL_KEYS.key_end = elliptics.Id([255] * 64, 0)

    def __sync_removed_keys(self, keys):

        s = elliptics.Session(self.node)
        s.set_ioflags(elliptics.io_flags.nodata)
        s.add_groups([self.group])

        remote_keys = set()
        for key in keys:
            eid = elliptics.Id(key['key'])
            try:
                s.lookup(eid)
            except elliptics.NotFoundError:
                pass
            remote_keys.add(tuple(eid.id))

        remove_keys = []

        iterator = s.start_iterator(elliptics.Id([0] * 64, 0), [self.ALL_KEYS],
                                    elliptics.iterator_types.network,
                                    elliptics.iterator_flags.key_range,
                                    elliptics.Time(0, 0),
                                    elliptics.Time(2 ** 64 - 1, 2 ** 64 - 1))
        for item in iterator:
            if not tuple(item.response.key.id) in remote_keys:
                remove_keys.append(item.response.key)

        for eid in remove_keys:
            try:
                logging.info('Removing local key %s, not found on remote' % repr(eid))
                s.remove(eid)
            except elliptics.NotFoundError:
                logging.info('Tried to remove key %s, but it was not found anymore' % eid)
