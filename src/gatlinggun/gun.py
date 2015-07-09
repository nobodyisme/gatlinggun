import atexit
import math
import os
import os.path
import shutil
import socket
import signal
import traceback

import elliptics

from gatlinggun.errors import ConnectionError, InvalidDataError
from gatlinggun import helpers as h
from logger import logger


class Gun(object):

    READ_CHUNK_SIZE = 500 * 1024 * 1024  # 500 Mb
    # write chunk size is set big enough in effort to write all required data
    # in one elliptics.write_data call (should be set minding wait_timeout config setting)
    WRITE_CHUNK_SIZE = 50 * 1024 * 1024

    WRITE_RETRY_NUM = 5
    READ_RETRY_NUM = 3

    DISTRUBUTE_TASK_ACTION = 'add'
    REMOVE_TASK_ACTION = 'remove'

    def __init__(self, node, cache_path_prefix, tmpdir):
        self.session = elliptics.Session(node)
        self.tmpdir = tmpdir
        try:
            self.hostname = socket.gethostname()
        except Exception as e:
            raise RuntimeError('Failed to get local hostname: {0}'.format(e))

        self.__local_cache_groups = set()

        self.cache_path_prefix = cache_path_prefix
        self.update_local_cache_groups()

        atexit.register(self.clean)
        signal.signal(signal.SIGTERM, lambda signum, stack_frame: exit(1))

    @property
    def local_cache_groups(self):
        return self.__local_cache_groups

    def update_local_cache_groups(self):
        new_cache_groups = set()
        local_addresses = []
        local_ips = h.ips_set(self.hostname)
        logger.info('Local ips: {0}'.format(local_ips))
        for address in self.session.routes.addresses():
            if address.host in local_ips:
                local_addresses.append(address)

        for address in local_addresses:

            try:
                s = self.session.clone()
                s.set_direct_id(address)
                msre = s.monitor_stat(address,
                    categories=elliptics.monitor_stat_categories.backend).get()[0]
                for backend in msre.statistics['backends'].itervalues():
                    if backend['status']['state'] != 1:
                        continue
                    backend_base_path = backend.get('backend', {}).get('config', {}).get('data', None)
                    if backend_base_path and backend_base_path.startswith(self.cache_path_prefix):
                        new_cache_groups.add(backend['backend']['config']['group'])

            except Exception as e:
                logger.error('Failed to fetch monitor stat from address {0}: {1}\n{2}'.format(
                    address, e, traceback.format_exc()))
                continue

        logger.info('Updated list of local cache groups (local addresses: {0}): {1}'.format(
            local_addresses, list(new_cache_groups)))

        self.__local_cache_groups = new_cache_groups

    def process(self, task):
        if not 'action' in task:
            raise InvalidDataError('No action is set for task')
        if task['action'] == self.DISTRUBUTE_TASK_ACTION:
            return self.distribute(task['key'].encode('utf-8'),
                                   from_groups=task['sgroups'],
                                   to_groups=[task['group']])
        elif task['action'] == self.REMOVE_TASK_ACTION:
            return self.remove(task['key'].encode('utf-8'),
                               from_groups=[task['group']])

        raise InvalidDataError('Unknow action: {0}'.format(task['action']))

    def distribute(self, key, from_groups=None, to_groups=None):
        if not from_groups or not to_groups:
            raise InvalidDataError('Groups are not properly defined for key "{0}"'.format(key))
        logger.info('Distributing key {} to groups {} from '
                    'groups {}'.format(key, to_groups, from_groups))

        fname = os.path.join(self.tmpdir, key)

        try:
            # fetch data from source group
            try:
                size, timestamp, user_flags = self.read(from_groups, key, fname)
            except InvalidDataError:
                raise
            except Exception as e:
                raise ConnectionError('Failed to read data for key %s, will be retried (%s)\n%s' % (
                    key, e, traceback.format_exc()))

            logger.info('Data read into tmp file: %s' % fname)

            # distribute data to destination nodes
            logger.info('Distributing fetched data to groups %s' % to_groups)

            for g in to_groups:
                logger.info('Writing key %s to group %s' % (key, g))
                try:
                    self.write([g], key, fname, size, timestamp, user_flags)
                except elliptics.Error:
                    # Group is not available (No such device ot address: -6)
                    raise
                except Exception:
                    raise ConnectionError('Failed to write data for key %s to group %s, will be retried' % (key, g))
        finally:
            try:
                os.unlink(fname)
            except OSError:
                pass

        logger.info('Data was distibuted')

    def read(self, groups, key, fname):
        eid = elliptics.Id(key)

        session = self.session.clone()
        session.ioflags &= ~elliptics.io_flags.nocsum
        session.cflags |= elliptics.command_flags.nolock
        logger.info('Groups: {0}'.format(groups))
        session.add_groups(groups)
        try:
            lookup = session.read_data(eid, offset=0, size=1).get()[0]
        except elliptics.NotFoundError:
            raise InvalidDataError('Key "{0}" is not found on source groups'.format(key))

        size = lookup.total_size
        timestamp = lookup.timestamp
        user_flags = lookup.user_flags

        session = session.clone()
        session.ioflags |= elliptics.io_flags.nocsum
        last_exc = None
        with open(fname, 'wb') as f:
            for i in xrange(int(math.ceil(float(size) / self.READ_CHUNK_SIZE))):
                for retries in xrange(self.READ_RETRY_NUM):
                    try:
                        res = session.read_data(
                            eid, i * self.READ_CHUNK_SIZE, self.READ_CHUNK_SIZE).get()
                        chunk = res[0].data
                        break
                    except Exception as e:
                        last_exc = e
                        logger.info('Error while reading key %s: type %s, msg: %s' % (key, type(e), e))
                        pass
                else:
                    err = 'Failed to read key %s: offset %s / total %s, last exc: %s' % (
                        key, i * self.READ_CHUNK_SIZE, size, last_exc)
                    raise ConnectionError(err)
                f.write(chunk)

        logger.info('Key {0} successfully read from groups {1} and written to '
            'temp file {2}'.format(key, groups, fname))
        return size, timestamp, user_flags

    def write(self, groups, key, fname, size, timestamp, user_flags):
        eid = elliptics.Id(key)

        session = self.session.clone()
        session.add_groups(groups)
        session.user_flags = user_flags
        session.timestamp = timestamp

        last_exc = None
        with open(fname, 'rb') as f:
            data = f.read(size)
            for retries in xrange(self.WRITE_RETRY_NUM):
                try:
                    logger.debug('Writing key %s: len %s' % (key, len(data)))
                    res = session.write_data(eid, data).get()
                    break
                except Exception as e:
                    logger.info('Error while writing key %s: type %s, msg: %s' % (key, type(e), e))
                    pass
            else:
                err = 'Failed to write key %s, len %s, last exc: %s' % (
                    key, len(data), last_exc)
                raise ConnectionError(err)

        logger.info('Key {0} successfully written to group {1}'.format(key, groups))

    def remove(self, key, from_groups=None):

        if not from_groups:
            raise InvalidDataError(
                'Groups are not properly defined for key "{0}"'.format(key))

        logger.info('Removing key %s from groups %s' % (key, from_groups))

        session = self.session.clone()
        session.add_groups(from_groups)
        eid = elliptics.Id(key)

        try:
            session.remove(eid).get()
        except elliptics.NotFoundError:
            # keys are already removed from destination groups
            logger.info('Key {0} is not present in group {1}, skipped'.format(key, from_groups))
            pass
        except elliptics.Error as e:
            # Group is not available (No such device ot address: -6)
            logger.error('Key {0} is not present in group {1}: {2}'.format(key, from_groups, e))
            raise
        except Exception as e:
            logger.error('Key {0}: failed to remove from group {1}: {2}'.format(key, from_groups, e))
            raise ConnectionError('Failed to remove key %s: %s' % (key, e))

    def clean(self):
        try:
            shutil.rmtree(self.tmpdir)
        except Exception:
            pass

    def __del__(self):
        self.clean()
