import logging

from django.db import transaction
from django.db.models import DateField
from django.db.models.functions.datetime import TruncBase
from django.db.utils import IntegrityError

from utils.utils import chunks_generator

default_logger = logging.getLogger(__name__)


class TruncQuarter(TruncBase):
    kind = 'quarter'


# todo: use the same class from django (since Django 2.1)
class TruncWeek(TruncBase):
    kind = 'week'

    def as_sql(self, compiler, connection):
        if isinstance(self.output_field, DateField):
            inner_sql, inner_params = compiler.compile(self.lhs)
            sql = connection.ops.date_trunc_sql(self.kind, inner_sql)
            return sql, inner_params
        else:
            return super(TruncWeek, self).as_sql(compiler, connection)


def safe_bulk_create(model, objs, batch_size=10000, logger=None):
    """
    In case of Integrity Error, tries to insert all objects one by one
    Expects model manager to have bulk_create method
    :param model:
    :param objs: list
    :param batch_size: int
    :param logger:
    :return:
    """
    if transaction.get_connection().in_atomic_block:
        raise IntegrityError(
            "`safe_bulk_create` is invoked inside a transaction")
    for chunk in chunks_generator(objs, batch_size):
        bulk_chunk = list(chunk)
        try:
            model.objects.bulk_create(bulk_chunk)
        except IntegrityError as ex_1:
            if logger:
                logger.info(ex_1)
            for obj in bulk_chunk:
                try:
                    obj.save()
                except IntegrityError as ex_2:
                    if logger:
                        logger.info(ex_2)
