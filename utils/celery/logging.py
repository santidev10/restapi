import logging

from billiard.einfo import Traceback
from billiard.einfo import _Frame
from celery.signals import setup_logging
from celery.signals import task_failure

from utils.exception import wrap_exception

__all__ = ["init_celery_logging"]


def init_celery_logging():
    fix_email_logging()


class _FrameFixed(_Frame):
    def __init__(self, *args, **kwargs):
        super(_FrameFixed, self).__init__(*args, **kwargs)
        self.f_back = None


def fix_email_logging():
    Traceback.Frame = _FrameFixed


@task_failure.connect
def on_task_failure(sender, exception, **kwargs):
    logger = logging.getLogger(sender.name)
    wrapped_error = wrap_exception(kwargs.get("args"), kwargs.get("kwargs"), exception)
    logger.exception(wrapped_error)


@setup_logging.connect
def jobinator_setup_logging(loglevel, logfile, format, colorize, **kwargs):
    # fixme: dirty hack. Find better solution
    # empty function is required to make celery logging working.
    pass
