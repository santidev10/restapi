from datetime import timedelta
import requests
import os
from time import sleep

from psutil import NoSuchProcess, Process


def get_dates_range(date_from, date_to):
    delta = date_to - date_from
    for i in range(delta.days + 1):
        yield date_from + timedelta(days=i)


def get_google_access_token_info(token):
    url = "https://www.googleapis.com/oauth2/v3/tokeninfo?" \
          "access_token={}".format(token)
    token_info = requests.get(url).json()
    return token_info


class ProcessIsRunningError(Exception):
    pass


def get_process_name(pid):
    return Process(pid).name()


def is_similar_process(pid):
    if not pid.isnumeric():
        return False
    pid_int = int(pid)
    current_pid = os.getpid()
    try:
        return get_process_name(pid_int) == get_process_name(current_pid)
    except NoSuchProcess:
        return False


def create_pid_file(filename):
    if os.path.exists(filename):
        with open(filename, "r") as file:
            old_pid = file.read().strip()
            if is_similar_process(old_pid):
                raise ProcessIsRunningError
        os.remove(filename)

    with open(filename, "a") as file:
        file.write(str(os.getpid()))


def command_single_process_lock(name, max_sleep=3600, sleep_interval=10):
    """
    creates a lock file in the projects directory
    that doesn't allow to run another task with the same name
    another task will sleep every sleep_interval seconds until
    either the lock will be released or max_sleep seconds will passed
    :param name:
    :param max_sleep:
    :param sleep_interval:
    :return:
    """

    def decorator(command):
        def decorated_command(*args, **kwargs):
            f_name = "{}.pid".format(name)
            sleep_time = 0
            while sleep_time < max_sleep:
                try:
                    create_pid_file(f_name)
                except ProcessIsRunningError:
                    sleep_time += sleep_interval
                    sleep(sleep_interval)
                else:
                    break  # success
            else:
                raise Exception(
                    "Lock hasn't been released during the wait period")

            try:
                result = command(*args, **kwargs)
            except Exception:
                raise
            else:
                return result
            finally:
                os.remove(f_name)

        return decorated_command

    return decorator
