from datetime import timedelta
import requests
import os
from time import sleep


def get_dates_range(date_from, date_to):
    delta = date_to - date_from
    for i in range(delta.days + 1):
        yield date_from + timedelta(days=i)


def get_google_access_token_info(token):
    url = "https://www.googleapis.com/oauth2/v3/tokeninfo?" \
          "access_token={}".format(token)
    token_info = requests.get(url).json()
    return token_info


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
            f_name = "~{}.lock".format(name)
            sleep_time = 0
            while sleep_time < max_sleep:
                try:
                    os.open(f_name, os.O_CREAT | os.O_EXCL)
                except FileExistsError:
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
