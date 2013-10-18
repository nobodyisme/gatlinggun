import atexit
import itertools
import math
import msgpack
import os
import os.path
import shutil
import socket
import signal
import tempfile

import elliptics

from errors import ConnectionError, InvalidDataError
import inventory
from logger import logger


class Gun(object):

    READ_CHUNK_SIZE = 10 * 1024 * 1024  # 10 Mb
    # write chunk size is set big enough in effort to write all required data
    # in one elliptics.write_data call (should be set minding wait_timeout config setting)
    WRITE_CHUNK_SIZE = 500 * 1024 * 1024

    WRITE_RETRY_NUM = 5
    READ_RETRY_NUM = 3

    DISTRUBUTE_TASK_ACTION = 'add'
    ELIMINATE_TASK_ACTION = 'remove'

    GROUP_UNKNOWN_DC_ID = 'unknown'

    def __init__(self, node, service):
        self.session = elliptics.Session(node)
        self.service = service
        self.host_dc = inventory.get_dc_by_host(socket.gethostname())
        self.tmpdir = tempfile.mkdtemp(prefix='gatlinggun')
        atexit.register(self.clean)
        signal.signal(signal.SIGTERM, lambda signum, stack_frame: exit(1))

    def process(self, task):
        if not 'action' in task:
            raise InvalidDataError('No action is set for task')
        if task['action'] == self.DISTRUBUTE_TASK_ACTION:
            return self.distribute(task['key'].encode('utf-8'),
                                   from_=task['sgroups'],
                                   to_=task['dgroups'])
        elif task['action'] == self.ELIMINATE_TASK_ACTION:
            return self.eliminate(task['key'].encode('utf-8'),
                                  from_=task['dgroups'])

        raise InvalidDataError('Unknow action: %s' % task['action'])

    def distribute(self, key, from_=None, to_=None):
        if not from_ or not to_:
            raise InvalidDataError('Groups are not properly defined for key "%s"' % key)
        logger.info('Distributing key %s for groups %s' % (key, to_))

        fname = os.path.join(self.tmpdir, key)
        # fetch data from source nodes
        logger.info('Source groups %s' % from_)
        selected_groups = self.__preferable_groups(from_)
        logger.info('Fetching data from groups %s' % from_)
        self.session.add_groups(selected_groups)
        try:
            size = self.read(key, fname)
        except InvalidDataError:
            raise
        except elliptics.Error:
            # Group is not available (No such device ot address: -6)
            raise
        except Exception as e:
            raise ConnectionError('Failed to read data for key %s, will be retried (%s)' % (key, e))

        logger.info('Data read into tmp file: %s' % fname)

        # distribute data to destination nodes
        logger.info('Distributing fetched data to groups %s' % to_)
        self.session.add_groups(to_)
        try:
            self.write(key, fname, size)
        except elliptics.Error:
            # Group is not available (No such device ot address: -6)
            raise
        except Exception:
            raise ConnectionError('Failed to write data for key %s, will be retried' % key)
        finally:
            try:
                os.unlink(fname)
            except OSError:
                pass

        logger.info('Data was distibuted')

    def __preferable_groups(self, groups):
        dc_groups = {}
        dc_group_pairs = [(self.__group_dc(g), g) for g in groups]
        logger.info('Groups by dc pairs: %s' % (dc_group_pairs,))
        for dc, gs in itertools.groupby(sorted(dc_group_pairs), lambda x: x[0]):
            dc_groups[dc] = list(gs)

        return dc_groups.get(self.host_dc, groups)

    def __group_dc(self, g):
        try:
            info = self.service.enqueue('get_group_info', msgpack.packb(g)).get()
        except Exception as e:
            logger.info('Failed to get group %s info from mastermind (%s)' % (g, e))
            return self.GROUP_UNKNOWN_DC_ID

        try:
            addr = info['nodes'][0]['addr']
        except (IndexError, KeyError):
            logger.info('Unsupported group %s info structure' % (g,))
            return self.GROUP_UNKNOWN_DC_ID

        try:
            return inventory.get_dc_by_host(addr.split(':')[0])
        except Exception as e:
            logger.info('Failed to get dc data by host for group %s (%s)' % (g, e))
            return self.GROUP_UNKNOWN_DC_ID

    def eliminate(self, key, from_=None):
        if not from_:
            raise InvalidDataError('Groups are not properly defined for key "%s"' % key)

        logger.info('Removing key %s from groups %s' % (key, from_))

        self.session.add_groups(from_)
        self.remove(key)

    def read(self, key, fname):
        eid = elliptics.Id(key)
        try:
            size = self.session.lookup(eid)[2]
        except elliptics.NotFoundError:
            raise InvalidDataError('Key %s is not found on source groups' % key)

        with open(fname, 'wb') as f:
            for i in xrange(int(math.ceil(float(size) / self.READ_CHUNK_SIZE))):
                for retries in xrange(self.READ_RETRY_NUM):
                    try:
                        chunk = self.session.read_data(eid, i * self.READ_CHUNK_SIZE, self.READ_CHUNK_SIZE)
                        break
                    except Exception as e:
                        logger.info('Error while reading key %s: type %s, msg: %s' % (key, type(e), e))
                else:
                    raise ConnectionError('Failed to read key %s: offset %s / total %s' % (key, i * self.READ_CHUNK_SIZE))
                f.write(chunk)

        return size

    def write(self, key, fname, size):
        eid = elliptics.Id(key)

        with open(fname, 'rb') as f:
            for i in xrange(int(math.ceil(float(size) / self.WRITE_CHUNK_SIZE))):
                data = f.read(self.WRITE_CHUNK_SIZE)
                for retries in xrange(self.WRITE_RETRY_NUM):
                    try:
                        logger.debug('Writing key %s: len %s, offset %s' % (key, len(data), i * self.WRITE_CHUNK_SIZE))
                        self.session.write_data(eid, data, i * self.WRITE_CHUNK_SIZE)
                        break
                    except Exception as e:
                        logger.info('Error while writing key %s: type %s, msg: %s' % (key, type(e), e))
                        pass
                else:
                    raise ConnectionError('Failed to write key %s: offset %s / total %s' % (key, i * self.WRITE_CHUNK_SIZE, size))

    def remove(self, key):
        eid = elliptics.Id(key)

        try:
            self.session.remove(eid)
        except elliptics.NotFoundError:
            # keys are already removed from destination groups
            pass
        except elliptics.Error:
            # Group is not available (No such device ot address: -6)
            raise
        except Exception as e:
            raise ConnectionError('Failed to remove key %s: %s' % (key, e))

    def clean(self):
        try:
            shutil.rmtree(self.tmpdir)
        except Exception:
            pass

    def __del__(self):
        self.clean()
