import socket

import elliptics
import logging


logger = logging.getLogger()


def make_elliptics_node(config):
    elliptics_addresses = config.get('elliptics', 'addresses')
    logger.info('Connection to elliptics addresses {0}'.format(elliptics_addresses))
    c = elliptics.Config()
    c.config.wait_timeout = (config.getint('elliptics', 'wait_timeout')
                             if config.has_option('elliptics', 'wait_timeout') else
                             5)
    logger.debug('Node settings: wait_timeout {0}'.format(c.config.wait_timeout))

    addresses = []
    for address in config.get('elliptics', 'addresses').split(','):
        hostname, _ = address.split(':', 1)
        if not hostname:
            try:
                hostname = socket.gethostname()
                address = hostname + address
            except Exception as e:
                raise RuntimeError('Failed to get local hostname: {0}'.format(e))
        try:
            addresses.append(elliptics.Address.from_host_port_family(address))
        except Exception as e:
            logger.error('Failed to initialize elliptics address: {0}'.format(e))
            continue

    node = elliptics.Node(
        elliptics.Logger(
            config.get('elliptics', 'log'),
            elliptics.log_level.names[config.get('elliptics', 'log_level')]),
        c)

    try:
        node.add_remotes(addresses)
    except Exception as e:
        logger.error('Failed to connect to elliptics remotes: {0}'.format(e))
        raise

    return node


def ips_set(hostname):
    ips = set()
    addrinfo = socket.getaddrinfo(hostname, None)
    for res in addrinfo:
        ips.add(res[4][0])

    return ips
