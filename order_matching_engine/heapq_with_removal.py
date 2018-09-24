import threading
from heapq import heappush, heappop, heapify


class HeapQueue:
    """
    Min-heap queue:
    lesser number of priority - higher priority
    """

    def __init__(self):
        self._queue = []
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.not_full = threading.Condition(self.mutex)

    def get(self):
        with self.not_empty:
            while not self.size():
                self.not_empty.wait()   # waits for notify
            self.not_full.notify()
            return heappop(self._queue)

    def put(self, priority, timestamp=None, quote=None):
        with self.not_full:
            if quote == 'STOP':
                timestamp = 2000000000
            item = priority, timestamp, quote
            heappush(self._queue, item)
            self.not_empty.notify()

    def delete(self, order_id):
        index = None
        quote = None
        for item in self._queue:
            if item[2]['order_id'] == order_id:
                index = self._queue.index(item)
                quote = item[2]
                break
        if index:
            self._queue.remove(index)
            heapify(self._queue)
            return True, quote
        return False, None

    def size(self):
        return len(self._queue)
