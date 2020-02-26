from contextlib import contextmanager


@contextmanager
def patch_user_settings(user, **kwargs):
    user_settings_backup = user.aw_settings
    user.aw_settings = {**user_settings_backup, **kwargs}
    user.save()
    yield
    user.aw_settings = user_settings_backup
    user.save()
