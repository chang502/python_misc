import threading
import csv

class ThreadSafeWriter(object):
    def __init__(self, *args, **kwargs):
        self._writer = csv.writer(*args, **kwargs)
        self._lock = threading.Lock()
        self._count_rows = 0

    def writerow(self, row):
        with self._lock:
            self._count_rows += 1
            return self._writer.writerow(row)

    def writerows(self, rows):
        with self._lock:
            return self._writer.writerows(rows)

    def get_count(self):
        return self._count_rows
