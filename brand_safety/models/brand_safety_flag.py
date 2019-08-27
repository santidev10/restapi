from django.db.models import CharField
from django.db.models import DateTimeField
from django.db.models import IntegerField
from django.db.models import BigIntegerField
from django.db.models import Model

from audit_tool.models import get_hash_name


class BrandSafetyFlag(Model):
    VIDEO_ITEM = 0
    CHANNEL_ITEM = 1

    item_type = IntegerField(db_index=True)
    item_id = CharField(db_index=True, max_length=64)
    item_id_hash = BigIntegerField(db_index=True)
    completed_at = DateTimeField(null=True, default=None, db_index=True)
    created_at = DateTimeField(auto_now_add=True, db_index=True)

    def save(self, *args, **kwargs):
        if kwargs.get("item_id_hash") is None:
            kwargs["item_id_hash"] = get_hash_name(kwargs["item_id"])
        super().save(*args, **kwargs)

    @staticmethod
    def enqueue(*_, **kwargs):
        """
        Create new BrandSafetyFlag entry
        :param _:
        :param kwargs: Model field values
        :return: BrandSafetyFlag
        """
        enqueue_item = BrandSafetyFlag.objects.create(**kwargs)
        return enqueue_item

    @staticmethod
    def dequeue(item_type, dequeue_limit=10):
        """
        Interface to return up to 10 items from queue
        :return: BrandSafetyFlag
        """
        dequeue_items = BrandSafetyFlag.objects.filter(item_type=item_type, completed_at=None).order_by("created_at")[:dequeue_limit]
        if not dequeue_items:
            raise BrandSafetyFlagQueueEmptyException
        return dequeue_items


class BrandSafetyFlagQueueEmptyException(Exception):
    pass

