"""
Zookeeper based queue implementations.
"""
import uuid
from kazoo.exceptions import NoNodeError, NodeExistsError, SessionExpiredError
from kazoo.retry import ForceRetryError
from kazoo.protocol.states import EventType
from kazoo.recipe.queue import BaseQueue


class FilteredLockingQueue(BaseQueue):
    """A distributed queue with priority and locking support.

    Upon retrieving an entry from the queue, the entry gets locked with an
    ephemeral node (instead of deleted). If an error occurs, this lock gets
    released so that others could retake the entry. This adds a little penalty
    as compared to :class:`Queue` implementation.

    The user should call the :meth:`FilteredLockingQueue.get` method first to lock and
    retrieve the next entry. When finished processing the entry, a user should
    call the :meth:`FilteredLockingQueue.consume` method that will remove the entry
    from the queue.

    This queue will not track connection status with ZooKeeper. If a node locks
    an element, then loses connection with ZooKeeper and later reconnects, the
    lock will probably be removed by Zookeeper in the meantime, but a node
    would still think that it holds a lock. The user should check the
    connection status with Zookeeper or call :meth:`FilteredLockingQueue.holds_lock`
    method that will check if a node still holds the lock.

    """
    lock = "/taken"
    entries = "/entries"
    entry = "entry"

    def __init__(self, client, path, filter_func):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The queue path to use in ZooKeeper.
        """
        super(FilteredLockingQueue, self).__init__(client, path)
        self.id = uuid.uuid4().hex.encode()
        self.processing_element = None
        self._lock_path = self.path + self.lock
        self._entries_path = self.path + self.entries
        self.structure_paths = (self._lock_path, self._entries_path)
        self.filter_func = filter_func
        self.items_cache = {}

    def __len__(self):
        """Returns the current length of the queue.

        :returns: queue size (includes locked entries count).
        """
        return super(FilteredLockingQueue, self).__len__()

    def put(self, value, priority=100):
        """Put an entry into the queue.

        :param value: Byte string to put into the queue.
        :param priority:
            An optional priority as an integer with at most 3 digits.
            Lower values signify higher priority.

        """
        self._check_put_arguments(value, priority)
        self._ensure_paths()

        self.client.create(
            "{path}/{prefix}-{priority:03d}-".format(
                path=self._entries_path,
                prefix=self.entry,
                priority=priority),
            value, sequence=True)

    def get(self, timeout=None):
        """Locks and gets an entry from the queue. If a previously got entry
        was not consumed, this method will return that entry.

        :param timeout:
            Maximum waiting time in seconds. If None then it will wait
            untill an entry appears in the queue.
        :returns: A locked entry value or None if the timeout was reached.
        :rtype: bytes
        """
        self._ensure_paths()
        if not self.processing_element is None:
            return self.processing_element[1]
        else:
            return self._inner_get(timeout)

    def holds_lock(self):
        """Checks if a node still holds the lock.

        :returns: True if a node still holds the lock, False otherwise.
        :rtype: bool
        """
        if self.processing_element is None:
            return False
        lock_id, _ = self.processing_element
        lock_path = "{path}/{id}".format(path=self._lock_path, id=lock_id)
        try:
            self.client.sync(lock_path)
            value, stat = self.client.retry(self.client.get, lock_path)
        except (NoNodeError, SessionExpiredError):
            # node has already been removed, probably after session expiration
            self.processing_element = None
            raise
        return value == self.id

    def consume(self):
        """Removes a currently processing entry from the queue.

        :returns: True if element was removed successfully, False otherwise.
        :rtype: bool
        """
        if not self.processing_element is None and self.holds_lock():
            id_, value = self.processing_element
            self.client.retry(self.client.delete,
                "{path}/{id}".format(
                    path=self._entries_path,
                    id=id_))

            self.client.retry(self.client.delete,
                "{path}/{id}".format(
                    path=self._lock_path,
                    id=id_))

            self.processing_element = None
            return True
        else:
            return False

    def unlock(self):
        """Unlocks a currently processing entry.

        :returns: True if element was unlocked successfully, False otherwise.
        :rtype: bool
        """
        if not self.processing_element is None and self.holds_lock():
            id_, value = self.processing_element

            self.client.retry(self.client.delete,
                "{path}/{id}".format(
                    path=self._lock_path,
                    id=id_))

            self.processing_element = None
            return True
        else:
            # either processing_element is None or node is already locked by someone else
            self.processing_element = None
            return False

    def list(self, func=None):
        """Returns list of all entries.
        If func parameter is supplied, it is applied to each result entry
        (via map function).

        :returns: List of all entries in the queue.
        :rtype: list
        """
        items = [self._get_data_cached(item)[0] for item in self.client.retry(self.client.get_children, self._entries_path)]
        if func:
            return map(func, items)
        return items

    def _inner_get(self, timeout):
        flag = self.client.handler.event_object()
        lock = self.client.handler.lock_object()
        canceled = False
        value = []

        def check_for_updates(event):
            if not event is None and event.type != EventType.CHILD:
                return
            with lock:
                if canceled or flag.isSet():
                    return
                items = self.client.retry(self.client.get_children,
                    self._entries_path,
                    check_for_updates)

                taken = self.client.retry(self.client.get_children,
                    self._lock_path,
                    check_for_updates)

                available = self._filter_locked(items, taken)

                items_to_take = []
                for item in available:
                    try:
                        data = self._get_data_cached(item)
                        if self.filter_func(data[0]):
                            items_to_take.append(item)
                    except:
                        pass

                if len(items_to_take) > 0:
                    ret = self._take(items_to_take[0])
                    if not ret is None:
                        # By this time, no one took the task
                        value.append(ret)
                        flag.set()

        check_for_updates(None)
        retVal = None
        flag.wait(timeout)
        with lock:
            canceled = True
            if len(value) > 0:
                # We successfully locked an entry
                self.processing_element = value[0]
                retVal = value[0][1]
        return retVal

    def _get_data_cached(self, item):
        if item in self.items_cache:
            return self.items_cache[item]

        data = self.client.retry(self.client.get,
            "{path}/{id}".format(
                path=self._entries_path,
                id=item))

        self.items_cache[item] = data
        return data

    def _filter_locked(self, values, taken):
        taken = set(taken)
        available = sorted(values)
        return (available if len(taken) == 0 else
            [x for x in available if x not in taken])

    def _take(self, id_):
        try:
            self.client.create(
                "{path}/{id}".format(
                    path=self._lock_path,
                    id=id_),
                self.id,
                ephemeral=True)
            value, stat = self.client.retry(self.client.get,
                "{path}/{id}".format(path=self._entries_path, id=id_))
        except (NoNodeError, NodeExistsError):
            # Item is already consumed or locked
            return None
        return (id_, value)
