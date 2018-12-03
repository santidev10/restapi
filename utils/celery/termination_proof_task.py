import logging
from abc import ABC

logger = logging.getLogger(__name__)

from celery.app.task import BaseTask

LOGGER_TEMPLATE = "Retry task due to termination. " \
                  "Task: {task}, task id: {task_id}, args: {args}, kwargs: {kwargs}"


class TerminationProofTask(BaseTask, ABC):

    def __call__(self, *args, **kwargs):
        try:
            return super(TerminationProofTask, self).__call__(*args, **kwargs)
        except SystemExit as ex:
            log_message = LOGGER_TEMPLATE.format(task=self,
                                                 task_id=self.request.id,
                                                 args=args,
                                                 kwargs=kwargs)
            logger.info(log_message)
            self.retry(countdown=30)
            raise ex
