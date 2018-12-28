import logging

from django.db import models, IntegrityError, transaction

logger = logging.getLogger(__name__)


class BaseQueryset(models.QuerySet):
    def safe_bulk_create(self, objs, batch_size=None):
        """
        In case of Integrity Error,
        tries to insert all objects one by one
        :param objs:
        :param batch_size:
        :return:
        """
        if transaction.get_connection().in_atomic_block:
            raise IntegrityError(
                "`safe_bulk_create` is invoked inside a transaction")
        try:
            self.bulk_create(objs, xbatch_size=batch_size)
        except IntegrityError as ex_1:
            logger.info(ex_1)
            for obj in objs:
                try:
                    obj.save()
                except IntegrityError as ex_2:
                    logger.info(ex_2)


class BaseModel(models.Model):
    objects = BaseQueryset.as_manager()

    class Meta:
        abstract = True
