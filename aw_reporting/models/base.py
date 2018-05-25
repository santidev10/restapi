import logging

from django.db import models, transaction, IntegrityError

logger = logging.getLogger(__name__)


class BaseQueryset(models.QuerySet):
    def safe_bulk_create(self, objs, batch_size=None):
        """
        In case of Duplicates Integrity Error,
        tries to insert all objects one by one
        :param objs:
        :param batch_size:
        :return:
        """
        error_msg = "duplicate key value violates unique constraint"
        try:
            with transaction.atomic():
                self.bulk_create(objs, batch_size=batch_size)
        except IntegrityError as e:
            if e.args and error_msg in e.args[0]:
                logger.info(e)
                for obj in objs:
                    try:
                        with transaction.atomic():
                            obj.save()
                    except IntegrityError as e:
                        if e.args and error_msg in e.args[0]:
                            logger.info(e)
                        else:
                            raise
            else:
                raise


class BaseModel(models.Model):
    objects = BaseQueryset.as_manager()

    class Meta:
        abstract = True
