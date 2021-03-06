import logging

from django.db import IntegrityError
from django.db import models
from django.db import transaction

from utils.utils import chunks_generator

logger = logging.getLogger(__name__)


class BaseQueryset(models.QuerySet):
    def safe_bulk_create(self, objs, batch_size=10000):
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
        for chunk in chunks_generator(objs, batch_size):
            bulk_chunk = list(chunk)
            try:
                self.bulk_create(bulk_chunk)
            except IntegrityError as ex_1:
                logger.info(ex_1)
                for obj in bulk_chunk:
                    try:
                        obj.save()
                    except IntegrityError as ex_2:
                        logger.info(ex_2)


class BaseModel(models.Model):
    objects = BaseQueryset.as_manager()

    class Meta:
        abstract = True
