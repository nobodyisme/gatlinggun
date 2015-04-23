import copy
import logging

from gatlinggun.importer import import_object


logger = logging.getLogger('gatlinggun')


def setup_logger(log_config):

    formatters = {}
    for formatter, settings in log_config['formatters'].iteritems():
        settings_ = copy.copy(settings)
        class_ = import_object(settings_.pop('class'))
        formatters[formatter] = class_(**settings_)

    handlers = {}
    for handler, settings in log_config['handlers'].iteritems():
        settings_ = copy.copy(settings)
        class_ = import_object(settings_.pop('class'))
        level_ = getattr(logging, settings_.pop('level', 'NOTSET'))
        formatter_ = settings_.pop('formatter', 'default')
        handler_obj = class_(**settings_)
        handler_obj.setLevel(level_)
        handler_obj.setFormatter(formatters[formatter_])
        handlers[handler] = handler_obj

    for logger, settings in log_config['loggers'].iteritems():
        logger = logging.getLogger(logger)
        logger.setLevel(getattr(logging, settings.get('level', 'NOTSET')))
        logger.propagate = settings.get('propagate', True)
        for handler in settings['handler']:
            logger.addHandler(handlers[handler])
