
class ConnectionError(Exception):
    """Network communication error"""
    pass


class InvalidDataError(Exception):
    """Invalid data error"""
    pass


class EmptyQueue(Exception):
    """Empty queue, not actually an error"""
    pass