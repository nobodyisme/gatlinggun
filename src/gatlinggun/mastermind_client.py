import itertools

from cocaine.services import Service
try:
    from cocaine.exceptions import DisconnectionError
except ImportError:
    from cocaine.asio.exceptions import DisconnectionError


class ReconnectableService(object):
    def __init__(self, app_name, mm_addresses):
        self.app_name = app_name
        addresses = []
        for address in mm_addresses.split(','):
            address_parts = address.split(':')
            host = address_parts[0]
            port = len(address_parts) > 1 and address_parts[1] or None
            addresses.append((host, port))
        self.addresses = itertools.cycle(addresses)
        self.host = host
        self.port = port
        self.connect_service()

    def connect_service(self):
        address = self.addresses.next()
        self.backend = Service(self.app_name, host=address[0], port=address[1])

    def enqueue(self, method, data):
        return ReconnectableChain(self, method, data)


class ReconnectableChain(object):
    def __init__(self, service, method, data):
        self.service = service
        self.method = method
        self.data = data
        self._create_chain()

    def _create_chain(self):
        self.chain = self.service.backend.enqueue(self.method, self.data)

    def get(self):
        try:
            return self.chain.get()
        except DisconnectionError:
            sleep(3)
            self.service.connect_service()
            self._create_chain()
            return self.get()
