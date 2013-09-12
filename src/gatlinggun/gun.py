import logging

import elliptics


logger = logging.getLogger('gatlinggun')


class Gun(object):

    def __init__(self, node, group):
        self.session = elliptics.Session(node)
        self.group = group

    def fire(self, key, from_=None, to_=None):
        if not from_ or not to_:
            raise ValueError('Failed to process key %s' % key)
        logger.info('Processing key %s for groups %s' % (key, to_))

        # fetch data from source nodes
        logger.info('Fetching data from groups %s' % from_)
        self.session.add_groups(from_)
        data = self.session.read_data(key)

        logger.info('Data read, len: %s' % len(data))

        # distribute data to destination nodes
        logger.info('Distributing fetched data to groups %s' % to_)
        self.session.add_groups(to_)
        res = self.session.write_data(key, data)
        logger.info('Data was distibuted')
