
class ConnectionError(Exception):
    """Network communication error"""
    pass


class InvalidDataError(Exception):
    """Invalid data error"""
    pass


class EmptyQueue(Exception):
    """Empty queue event, not actually an error"""
    pass


class EllipticsError(Exception):
    def __init__(self, elliptics_error_code):
        self.elliptics_error_code = elliptics_error_code

    def __str__(self):
        return 'Elliptics error: {}'.format(self.elliptics_error_code)
