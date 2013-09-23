
class ConnectionError(Exception):
    """Network communication error"""
    pass


class InvalidDataError(Exception):
    """Invalid data error"""
    pass


class EmptyQueue(Exception):
    """Empty queue event, not actually an error"""
    pass