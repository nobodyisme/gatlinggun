import logging
import os
import os.path
import shutil
import tempfile

import elliptics

from errors import ConnectionError, InvalidDataError


logger = logging.getLogger('gatlinggun')


class Gun(object):

    def __init__(self, node, group):
        self.session = elliptics.Session(node)
        self.group = group
        self.tmpdir = tempfile.mkdtemp(prefix='gatlinggun')

    def fire(self, key, from_=None, to_=None):
        if not from_ or not to_:
            raise InvalidDataError('Groups are not properly defined for key "%s"' % key)
        logger.info('Processing key %s for groups %s' % (key, to_))

        fname = os.path.join(self.tmpdir, key)
        # fetch data from source nodes
        logger.info('Fetching data from groups %s' % from_)
        self.session.add_groups(from_)
        try:
            self.session.read_file(key, fname)
        except Exception:
            raise ConnectionError('Failed to read data for key %s, will be retried' % key)

        logger.info('Data read into tmp file: %s' % fname)

        # distribute data to destination nodes
        logger.info('Distributing fetched data to groups %s' % to_)
        self.session.add_groups(to_)
        try:
            res = self.session.write_file(key, fname)
        except Exception:
            raise ConnectionError('Failed to write data for key %s, will be retried' % key)
        finally:
            try:
                os.unlink(fname)
            except OSError:
                pass

        logger.info('Data was distibuted')

    def __del__(self):
        shutil.rmtree(self.tmpdir)
