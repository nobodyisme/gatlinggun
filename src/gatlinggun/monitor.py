import logging
import socket

import elliptics
from tornado.concurrent import Future
from tornado import gen
from tornado.web import RequestHandler

from gatlinggun import helpers as h
from gatlinggun import sync


logger = logging.getLogger('monitor')


class Monitor(object):
    """
    Monitors local elliptics node and returns cache statistics
    for local node
    """
    def __init__(self, config):
        self.node = h.make_elliptics_node(config)
        self.session = elliptics.Session(self.node)

        try:
            self.hostname = socket.gethostname()
        except Exception as e:
            raise RuntimeError('Failed to get local hostname: {0}'.format(e))

        self.__local_addresses = []
        self.update_local_addresses()

        self.cache_path_prefix = config.get('elliptics', 'cache_path_prefix')

        sync.PeriodThread(self.update_local_addresses,
                          config.getint('global', 'sync_keys_period')
                          if config.has_option('global', 'sync_keys_period') else
                          10
        ).start()

    def update_local_addresses(self):
        local_addresses = []
        local_ips = h.ips_set(self.hostname)
        logger.info('Local ips: {0}'.format(local_ips))
        for address in self.session.routes.addresses():
            if address.host in local_ips:
                local_addresses.append(address)

        self.__local_addresses = local_addresses

    @gen.coroutine
    def handler(self):

        import time
        from tornado.ioloop import IOLoop

        res = {
            'records_total': 0,
            'records_removed': 0,
            'records_alive': 0,
            'records_removed_size': 0,
            'base_size': 0,
        }

        for address in self.__local_addresses:
            f = Future()
            def cb(results, error):
                if error.code != 0:
                    f.set_exception(error)
                else:
                    f.set_result(results[0])

            session = self.session.clone()
            async_result = session.monitor_stat(address,
                categories=elliptics.monitor_stat_categories.backend)
            async_result.connect(cb)
            result = yield f
            yield gen.Task(IOLoop.instance().add_timeout, time.time() + 5)

            stat = result.statistics

            for b_stat in stat['backends'].itervalues():
                if b_stat['status']['state'] != 1:
                    # backend is not enabled
                    continue
                backend_stats = b_stat['backend']
                if not backend_stats['config']['data'].startswith(self.cache_path_prefix):
                    # backend is not a cache group backend
                    continue

                summary_stats = backend_stats['summary_stats']
                res['records_total'] += summary_stats['records_total']
                res['records_removed'] += summary_stats['records_removed']
                res['records_alive'] += summary_stats['records_total'] - summary_stats['records_removed']
                res['records_removed_size'] += summary_stats['records_removed_size']
                res['base_size'] += summary_stats['base_size']

        raise gen.Return(res)


class MonitorHandler(RequestHandler):

    def initialize(self, monitor):
        self.monitor = monitor

    @gen.coroutine
    def get(self):
        try:
            res = yield self.monitor.handler()
            self.write(res)
        except Exception as e:
            logger.exception(e)
        finally:
            self.finish()

