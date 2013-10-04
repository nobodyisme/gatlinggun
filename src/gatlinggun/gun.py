import math
import os
import os.path
import shutil
import tempfile

import elliptics

from errors import ConnectionError, InvalidDataError
from logger import logger

class Gun(object):

    CHUNK_SIZE = 10 * 1024 * 1024  # 10 Mb

    WRITE_RETRY_NUM = 5
    READ_RETRY_NUM = 3

    DISTRUBUTE_TASK_ACTION = 'add'
    ELIMINATE_TASK_ACTION = 'remove'

    def __init__(self, node):
        self.session = elliptics.Session(node)
        self.tmpdir = tempfile.mkdtemp(prefix='gatlinggun')

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
        logger.info('Fetching data from groups %s' % from_)
        self.session.add_groups(from_)
        try:
            size = self.read(key, fname)
        except Exception as e:
            raise ConnectionError('Failed to read data for key %s, will be retried (%s)' % (key, e))

        logger.info('Data read into tmp file: %s' % fname)

        # distribute data to destination nodes
        logger.info('Distributing fetched data to groups %s' % to_)
        self.session.add_groups(to_)
        try:
            self.write(key, fname, size)
        except Exception:
            raise ConnectionError('Failed to write data for key %s, will be retried' % key)
        finally:
            try:
                os.unlink(fname)
            except OSError:
                pass

        logger.info('Data was distibuted')

    def eliminate(self, key, from_=None):
        if not from_:
            raise InvalidDataError('Groups are not properly defined for key "%s"' % key)

        logger.info('Removing key %s from groups %s' % (key, from_))

        self.session.add_groups(from_)
        self.remove(key)

    def read(self, key, fname):
        eid = elliptics.Id(key)
        size = self.session.lookup(eid)[2]

        with open(fname, 'wb') as f:
            for i in xrange(int(math.ceil(float(size) / self.CHUNK_SIZE))):
                for retries in xrange(self.READ_RETRY_NUM):
                    try:
                        chunk = self.session.read_data(eid, i * self.CHUNK_SIZE, self.CHUNK_SIZE)
                        break
                    except Exception as e:
                        logging.info('Error while reading key %s: type %s, msg: %s' % (key, type(e), e))
                else:
                    raise ConnectionError('Failed to read key %s: offset %s / total %s' % (key, i * self.CHUNK_SIZE))
                f.write(chunk)

        return size

    def write(self, key, fname, size):
        eid = elliptics.Id(key)

        with open(fname, 'rb') as f:
            for i in xrange(int(math.ceil(float(size) / self.CHUNK_SIZE))):
                data = f.read(self.CHUNK_SIZE)
                for retries in xrange(self.WRITE_RETRY_NUM):
                    try:
                        logging.debug('Writing key %s: len %s, offset %s' % (key, len(data), i * self.CHUNK_SIZE))
                        self.session.write_data(eid, data, i * self.CHUNK_SIZE)
                        break
                    except Exception as e:
                        logging.info('Error while writing key %s: type %s, msg: %s' % (key, type(e), e))
                        pass
                else:
                    raise ConnectionError('Failed to write key %s: offset %s / total %s' % (key, i * self.CHUNK_SIZE, size))

    def remove(self, key):
        eid = elliptics.Id(key)

        try:
            self.session.remove(eid)
        except elliptics.NotFoundError:
            # keys are already removed from destination groups
            pass
        except Exception as e:
            raise ConnectionError('Failed to remove key %s: %s' % (key, e))

    def __del__(self):
        shutil.rmtree(self.tmpdir)
