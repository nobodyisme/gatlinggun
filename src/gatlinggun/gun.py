import math
import os
import os.path
import random
import socket
import signal
import traceback

import elliptics

from gatlinggun.errors import ConnectionError, InvalidDataError, EllipticsError
from gatlinggun import helpers as h
from logger import logger


class Gun(object):

    READ_CHUNK_SIZE = 50 * 1024 * 1024
    WRITE_CHUNK_SIZE = 50 * 1024 * 1024

    WRITE_ATTEMPTS_NUM = 5
    READ_ATTEMPTS_NUM = 3

    DISTRUBUTE_TASK_ACTION = 'add'
    REMOVE_TASK_ACTION = 'remove'

    RETRIABLE_ELLIPTICS_ERRORS = (
        -110,  # connection timeout
        -104,  # connection reset by peer
        -6,    # group is not in route list
    )

    def __init__(self,
                 node,
                 cache_path_prefix,
                 tmp_dir,
                 read_chunk_size=None,
                 write_chunk_size=None,
                 request_id_prefix=None):

        self.session = elliptics.Session(node)
        self.tmp_dir = tmp_dir
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        try:
            self.hostname = socket.gethostname()
        except Exception as e:
            raise RuntimeError('Failed to get local hostname: {0}'.format(e))

        self.__local_cache_groups = set()

        self.cache_path_prefix = cache_path_prefix
        self.update_local_cache_groups()

        self.read_chunk_size = int(read_chunk_size or self.READ_CHUNK_SIZE)
        self.write_chunk_size = int(write_chunk_size or self.WRITE_CHUNK_SIZE)

        self.request_id_prefix = request_id_prefix

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

    # request id is 8 bytes hex number
    REQUEST_ID_BYTES_NUM = 8

    def _reset_request_id(self):
        if not self.request_id_prefix:
            return

        # generating random 8 bytes number, converting to string
        request_id = hex(random.randint(0, 2 ** (self.REQUEST_ID_BYTES_NUM * 8)))

        # removing '0x' prefix and possible 'L' postfix
        request_id = request_id[len('0x'):].rstrip('L')

        # filling with zeros up to 16 chars
        request_id = request_id.zfill(self.REQUEST_ID_BYTES_NUM * 2)

        # replacing first part with configurable request id prefix
        request_id = self.request_id_prefix + request_id[len(self.request_id_prefix):]

        self.session.trace_id = int(request_id, base=16)

    def process(self, task):
        self._reset_request_id()
        if 'action' not in task:
            raise InvalidDataError('No action is set for task')
        if task['action'] == self.DISTRUBUTE_TASK_ACTION:
            return self.distribute(task['key'].encode('utf-8'),
                                   from_groups=task['sgroups'],
                                   to_groups=[task['group']])
        elif task['action'] == self.REMOVE_TASK_ACTION:
            return self.remove(task['key'].encode('utf-8'),
                               from_groups=[task['group']])

        raise InvalidDataError('Unknowm action: {0}'.format(task['action']))

    def distribute(self, key, from_groups=None, to_groups=None):
        if not from_groups or not to_groups:
            raise InvalidDataError('Groups are not properly defined for key "{0}"'.format(key))
        logger.info('Distributing key {} to groups {} from '
                    'groups {}'.format(key, to_groups, from_groups))

        fname = os.path.join(self.tmp_dir, key)

        try:
            # fetch data from source group
            size, timestamp, user_flags = self.read(from_groups, key, fname)

            logger.info('Data read into tmp file: %s' % fname)

            # distribute data to destination nodes
            logger.info('Distributing fetched data to groups %s' % to_groups)

            for g in to_groups:
                logger.info('Writing key %s to group %s' % (key, g))
                self.write([g], key, fname, size, timestamp, user_flags)
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
        session.set_exceptions_policy(elliptics.exceptions_policy.no_exceptions)
        session.set_filter(elliptics.filters.all_final)
        logger.info('Groups: {0}'.format(groups))
        session.add_groups(groups)

        try:
            lookup = self.request(session.read_data(eid, offset=0, size=1))
        except EllipticsError as e:
            logger.error(
                'Key {key_id}: lookup failed: {error}'.format(
                    key_id=key,
                    error=e,
                )
            )
            raise

        size = lookup.total_size
        timestamp = lookup.timestamp
        user_flags = lookup.user_flags

        session = session.clone()
        session.ioflags |= elliptics.io_flags.nocsum

        with open(fname, 'wb') as f:
            for i in xrange(int(math.ceil(float(size) / self.read_chunk_size))):

                offset = i * self.read_chunk_size
                read_attempts = 0

                while True:
                    try:
                        read_attempts += 1
                        result = self.request(
                            session.read_data(
                                eid,
                                offset,
                                self.read_chunk_size
                            )
                        )
                        chunk = result.data
                    except Exception as e:
                        logger.error(
                            'Key {key_id}: error while reading, offset {offset}'.format(
                                key_id=key,
                                offset=offset,
                            )
                        )

                        if read_attempts >= self.READ_ATTEMPTS_NUM:
                            raise

                        if isinstance(e, EllipticsError) and e.elliptics_error_code in (-110,):
                            # retriable error
                            continue

                        raise

                    else:
                        break

                f.write(chunk)

        logger.info(
            'Key {key_id} successfully read from groups {groups} and written to '
            'temp file {file}'.format(
                key_id=key,
                groups=groups,
                file=fname
            )
        )

        return size, timestamp, user_flags

    def write(self, groups, key, fname, size, timestamp, user_flags):
        eid = elliptics.Id(key)

        session = self.session.clone()
        session.add_groups(groups)
        session.user_flags = user_flags
        session.timestamp = timestamp
        session.set_exceptions_policy(elliptics.exceptions_policy.no_exceptions)
        session.set_filter(elliptics.filters.all_final)

        with open(fname, 'rb') as f:
            self.request(session.write_prepare(eid, '', 0, size))
            for i in xrange(int(math.ceil(float(size) / self.write_chunk_size))):

                data = f.read(self.write_chunk_size)

                offset = i * self.write_chunk_size
                write_attempts = 0

                while True:
                    try:
                        logger.debug('Writing key %s: offset %s, size %s, ' % (
                            key, offset, len(data)))
                        write_attempts += 1
                        self.request(session.write_plain(eid, data, offset))
                    except Exception as e:
                        logger.error(
                            'Key {key_id}: error while writing, offset {offset}'.format(
                                key_id=key,
                                offset=offset,
                            )
                        )

                        if write_attempts >= self.WRITE_ATTEMPTS_NUM:
                            raise

                        if isinstance(e, EllipticsError) and e.elliptics_error_code in (-110,):
                            # retriable error
                            continue

                        raise

                    else:
                        break

            self.request(session.write_commit(eid, '', 0, size))

        logger.info('Key {0} successfully written to group {1}'.format(key, groups))

    @staticmethod
    def request(ell_request):
        result_set = ell_request.get()
        positive_results = [
            result
            for result in result_set
            if result.error.code == 0
        ]
        all_results_desc = [
            'group {group}: {error_code}'.format(
                group=result.group_id,
                error_code=result.error.code
            )
            for result in result_set
        ]
        if len(positive_results) > 1:
            logger.error(
                'Elliptics request result returned unexpected number '
                'of positive result objects: {}'.format(len(positive_results))
            )
        logger.error(
            'Request results: {}'.format(', '.join(all_results_desc))
        )
        if not positive_results:
            for result in result_set:
                if result.error.code in Gun.RETRIABLE_ELLIPTICS_ERRORS:
                    raise EllipticsError(result.error.code)

            raise EllipticsError(result_set[-1].error.code)

        return positive_results[0]

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
