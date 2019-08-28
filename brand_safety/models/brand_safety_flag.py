from django.db.models import CharField
from django.db.models import DateTimeField
from django.db.models import IntegerField
from django.db.models import Model
from django.db.utils import IntegrityError


class BrandSafetyFlag(Model):
    VIDEO_ITEM = 0
    CHANNEL_ITEM = 1

    item_type = IntegerField(db_index=True)
    item_id = CharField(unique=True, max_length=64)
    created_at = DateTimeField(auto_now_add=True, db_index=True)

    @staticmethod
    def enqueue(*_, **kwargs):
        """
        Create new BrandSafetyFlag entry
        :param _:
        :param kwargs: Model field values
        :return: BrandSafetyFlag
        """
        try:
            enqueue_item = BrandSafetyFlag.objects.create(**kwargs)
        except IntegrityError:
            # Item is already in queue
            enqueue_item = BrandSafetyFlag.objects.get(**kwargs)
        return enqueue_item

    @staticmethod
    def dequeue(item_type, dequeue_limit=10):
        """
        Interface to return up to 10 items from queue
        :return: BrandSafetyFlag
        """
        dequeue_items = BrandSafetyFlag.objects.filter(item_type=item_type).order_by("created_at")[:dequeue_limit]
        if not dequeue_items:
            raise BrandSafetyFlagQueueEmptyException
        return dequeue_items


class BrandSafetyFlagQueueEmptyException(Exception):
    pass

