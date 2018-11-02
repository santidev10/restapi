import os

from saas import settings


def get_lock_path(lock_name):
    return os.path.join(settings.BASE_DIR, "locks", lock_name)


class FileLock:
    def __init__(self, lock_name):
        self.lock_name = lock_name

    def acquire(self):
        file_path = get_lock_path(self.lock_name)
        if os.path.exists(file_path):
            raise FileExistsError
        f = open(file_path, "a")
        f.close()

    def release(self):
        file_path = get_lock_path(self.lock_name)
        if os.path.exists(file_path):
            os.remove(file_path)
