from gatlinggun.importer import import_object
from gatlinggun.inventory import fake_inventory as inv
from gatlinggun.logger import logger


def init_inventory(config):
    global inv
    try:
        obj_ = config.get('global', 'inventory')
        inv = import_object(obj_)
        logger.info('Inventory object: %s' % inv)
    except Exception as e:
        logger.info('Failed to init inventory object from config: %s' % e)
        pass

get_dc_by_host = lambda host: inv.get_dc_by_host(host)
