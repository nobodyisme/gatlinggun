import functools
import random
import socket
import signal
import time
import traceback

import elliptics

from gatlinggun.errors import ConnectionError, InvalidDataError, EllipticsError
from gatlinggun import helpers as h
from logger import logger


class Gun(object):

    CHUNK_SIZE = 50 * 1024 * 1024

    TIMEOUT = 5  # 5 seconds

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
                 chunk_size=None,
                 attempts=3,
                 timeout=None,
                 request_id_prefix=None):

        self.session = elliptics.newapi.Session(node)
        try:
            self.hostname = socket.gethostname()
        except Exception as e:
            raise RuntimeError('Failed to get local hostname: {0}'.format(e))

        self.__local_cache_groups = set()

        self.cache_path_prefix = cache_path_prefix
        self.update_local_cache_groups()

        self.chunk_size = int(chunk_size or self.CHUNK_SIZE)
        self.attempts = attempts
        self.attempts_interval = 2
        self.attempts_interval_exp = 2
        self.max_attempts_interval = 60

        self.timeout = timeout or self.TIMEOUT

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

        self.copy_key(
            key,
            src_groups=from_groups,
            dst_groups=to_groups,
        )

        logger.info('Data was distibuted')

    def copy_key(self, key, src_groups, dst_groups):
        logger.info(
            'Src groups {src_groups}, dst groups: {dst_groups}, '.format(
                src_groups=src_groups,
                dst_groups=dst_groups,
            )
        )

        read_session = self.session.clone()
        read_session.cflags |= elliptics.command_flags.nolock
        read_session.set_exceptions_policy(elliptics.exceptions_policy.no_exceptions)
        read_session.set_filter(elliptics.filters.all_final)
        read_session.ioflags &= ~elliptics.io_flags.nocsum
        read_session.timeout = self.timeout
        read_session.groups = src_groups

        write_session = self.session.clone()
        write_session.set_exceptions_policy(elliptics.exceptions_policy.no_exceptions)
        write_session.set_filter(elliptics.filters.all_final)
        write_session.timeout = self.timeout
        write_session.groups = dst_groups

        lookup = self._src_lookup(key, session=read_session)
        record_info = lookup.record_info

        # using only a single group that returned first positive result
        # (and with best weight)
        read_session.groups = [lookup.group_id]

        user_flags = self._src_user_flags(key, session=read_session)

        write_session.json_timestamp = record_info.json_timestamp
        write_session.timestamp = record_info.data_timestamp
        write_session.user_flags = user_flags

        self._write_prepare(
            key,
            record_info=record_info,
            session=write_session,
        )

        try:
            json_data = self._read_json(key, session=read_session)
            self._write_json(key, json_data, record_info=record_info, session=write_session)

            chunked_csum = record_info.record_flags & elliptics.record_flags.chunked_csum

            offset = 0
            while offset < record_info.data_size:
                chunk = self._read_data_chunk(
                    key,
                    offset,
                    self.chunk_size,
                    chunked_csum=chunked_csum,
                    record_info=record_info,
                    session=read_session,
                )
                self._write_data_chunk(
                    key,
                    chunk,
                    offset,
                    session=write_session,
                )
                offset += len(chunk)

            self._write_commit(key, record_info, session=write_session)
        except Exception:
            self._cleanup(key, session=write_session)
            # TODO: log error
            raise

        logger.info(
            'Key {key_id} successfully read from groups {src_groups} and written to '
            'groups {dst_groups}'.format(
                key_id=key,
                src_groups=src_groups,
                dst_groups=dst_groups,
            )
        )

    def __retry(func):

        @functools.wraps(func)
        def wrapper(self, key, *args, **kwargs):
            attempts = 0
            interval = self.attempts_interval
            while attempts < self.attempts:
                attempts += 1
                try:
                    return func(self, key, *args, **kwargs)
                except EllipticsError as e:
                    if attempts == self.attempts:
                        logger.error(
                            'Key {key}: elliptics attempts limit reached, '
                            '{attempts}/{max_attempts}, error: {error}'.format(
                                key=key,
                                attempts=attempts,
                                max_attempts=self.attempts,
                                error=e,
                            )
                        )
                        raise
                    if e.elliptics_error_code in Gun.RETRIABLE_ELLIPTICS_ERRORS:
                        logger.error(
                            'Key {key}: elliptics error, attempts '
                            '{attempts}/{max_attempts}, will be retried, error: {error}'.format(
                                key=key,
                                attempts=attempts,
                                max_attempts=self.attempts,
                                error=e,
                            )
                        )
                        time.sleep(interval)
                        interval = min(
                            interval * self.attempts_interval_exp,
                            self.max_attempts_interval
                        )
                        continue
                    logger.error(
                        'Key {key}: elliptics error, attempts '
                        '{attempts}/{max_attempts}, NOT retriable, error: {error}'.format(
                            key=key,
                            attempts=attempts,
                            max_attempts=self.attempts,
                            error=e,
                        )
                    )
                    raise

            raise AssertionError('Not expected to be here')

        return wrapper

    @__retry
    def _src_lookup(self, key, session):
        logger.info(
            'Key {key_id}: performing lookup'.format(
                key_id=key,
            )
        )

        try:
            res = self.any_request(
                session.lookup(
                    elliptics.Id(key)
                )
            )
        except EllipticsError as e:
            logger.error(
                'Key {key_id}: lookup failed: {error}'.format(
                    key_id=key,
                    error=e,
                )
            )
            raise

        return res

    @__retry
    def _src_user_flags(self, key, session):
        logger.info(
            'Key {key_id}: fetching user flags'.format(
                key_id=key,
            )
        )

        try:
            res = self.any_request(
                session.read_data(
                    elliptics.Id(key),
                    offset=0,
                    size=1,
                )
            )
        except EllipticsError as e:
            logger.error(
                'Key {key_id}: fetching user flags failed: {error}'.format(
                    key_id=key,
                    error=e,
                )
            )
            raise

        return res.record_info.user_flags

    @__retry
    def _write_prepare(self, key, record_info, session):
        try:
            self.all_request(
                session.write_prepare(
                    elliptics.Id(key),
                    json='',
                    json_capacity=record_info.json_size,
                    data='',
                    data_offset=0,
                    data_capacity=record_info.data_size,
                )
            )
        except EllipticsError as e:
            logger.error(
                'Key {key_id}: write_prepare failed: {error}'.format(
                    key_id=key,
                    error=e,
                )
            )
            raise

    @__retry
    def _read_json(self, key, session):
        logger.info(
            'Key {key_id}: reading json'.format(
                key_id=key,
            )
        )

        try:
            res = self.any_request(
                session.read_json(elliptics.Id(key))
            )
        except EllipticsError as e:
            logger.error(
                'Key {key_id}: lookup failed: {error}'.format(
                    key_id=key,
                    error=e,
                )
            )
            raise

        return res.json

    @__retry
    def _write_json(self, key, json_data, record_info, session):
        logger.info(
            'Key {key_id}: writing json'.format(
                key_id=key,
            )
        )

        try:
            self.all_request(
                session.write_plain(
                    elliptics.Id(key),
                    json=json_data,
                    data='',
                    data_offset=0,
                )
            )
        except EllipticsError as e:
            logger.error(
                'Key {key_id}: writing json failed: {error}'.format(
                    key_id=key,
                    error=e,
                )
            )
            raise

    @__retry
    def _read_data_chunk(self, key, offset, size, chunked_csum, record_info, session):
        logger.info(
            'Key {key_id}: reading data chunk, offset {offset}, size {size}'.format(
                key_id=key,
                offset=offset,
                size=size,
            )
        )

        prev_timeout = session.timeout
        prev_ioflags = session.ioflags

        if not chunked_csum:
            if offset == 0:
                # assumes that checksum calculation takes 1 seconds for each 5 Mb of data
                session.timeout = (
                    self.timeout +
                    record_info.data_size / (5 * 1024 * 1024)
                )
            else:
                # checksum is fully checked on first chunk reading, not required on next
                # chunks reading
                session.ioflags |= elliptics.io_flags.nocsum
        try:
            res = self.any_request(
                session.read_data(
                    elliptics.Id(key),
                    offset=offset,
                    size=size,
                )
            )
        except EllipticsError as e:
            logger.error(
                'Key {key_id}: reading data chunk failed, offset {offset}: {error}'.format(
                    key_id=key,
                    offset=offset,
                    error=e,
                )
            )
            raise
        finally:
            session.timeout = prev_timeout
            session.ioflags = prev_ioflags

        return res.data

    @__retry
    def _write_data_chunk(self, key, chunk, offset, session):
        logger.info(
            'Key {key_id}: writing data chunk, offset {offset}, size {size}'.format(
                key_id=key,
                offset=offset,
                size=len(chunk),
            )
        )

        try:
            self.all_request(
                session.write_plain(
                    elliptics.Id(key),
                    json='',
                    data=chunk,
                    data_offset=offset,
                )
            )
        except EllipticsError as e:
            logger.error(
                'Key {key_id}: writing data chunk failed, offset {offset}: {error}'.format(
                    key_id=key,
                    offset=offset,
                    error=e,
                )
            )
            raise

    @__retry
    def _write_commit(self, key, record_info, session):
        try:
            self.all_request(
                session.write_commit(
                    elliptics.Id(key),
                    json='',
                    data='',
                    data_offset=record_info.data_size,
                    data_commit_size=record_info.data_size,
                )
            )
        except EllipticsError as e:
            logger.error(
                'Key {key_id}: write commit failed: {error}'.format(
                    key_id=key,
                    error=e,
                )
            )
            raise

    @__retry
    def _cleanup(self, key, session):
        # TODO: remove?
        # do nothing, leave uncommited key
        pass

    @staticmethod
    def any_request(ell_request):
        result_set = ell_request.get()
        all_results_desc = [
            'group {group}: {error_code}'.format(
                group=result.group_id,
                error_code=result.error.code
            )
            for result in result_set
        ]
        logger.debug(
            'Request results: {}'.format(', '.join(all_results_desc))
        )
        positive_results = [
            result
            for result in result_set
            if result.error.code == 0
        ]
        if not positive_results:
            for result in result_set:
                if result.error.code in Gun.RETRIABLE_ELLIPTICS_ERRORS:
                    raise EllipticsError(result.error.code)

            raise EllipticsError(result_set[-1].error.code)

        return positive_results[0]

    @staticmethod
    def all_request(ell_request):
        result_set = ell_request.get()
        all_results_desc = [
            'group {group}: {error_code}'.format(
                group=result.group_id,
                error_code=result.error.code
            )
            for result in result_set
        ]
        logger.debug(
            'Request results: {}'.format(', '.join(all_results_desc))
        )
        retriable_errors = []
        other_errors = []
        for result in result_set:
            if result.error.code in Gun.RETRIABLE_ELLIPTICS_ERRORS:
                retriable_errors.append(result.error.code)
            elif result.error.code:
                other_errors.append(result.error.code)

        if other_errors:
            raise EllipticsError(other_errors[0])
        if retriable_errors:
            raise EllipticsError(retriable_errors[0])

        return result_set[0]

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
