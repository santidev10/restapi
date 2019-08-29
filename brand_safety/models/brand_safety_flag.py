from django.db.models import CharField
from django.db.models import DateTimeField
from django.db.models import IntegerField
from django.db.models import Model
from django.db.utils import IntegrityError

from brand_safety.auditors.utils import AuditUtils
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video


class BrandSafetyFlag(Model):
    VIDEO_ITEM = 0
    CHANNEL_ITEM = 1

    item_type = IntegerField(db_index=True)
    item_id = CharField(unique=True, max_length=64)
    created_at = DateTimeField(auto_now_add=True, db_index=True)

    @staticmethod
    def enqueue(*_, **kwargs):
        """
        Create new BrandSafetyFlag entry and reset its brand safety score
        :param _:
        :param kwargs: Model field values
        :return: BrandSafetyFlag
        """
        try:
            enqueue_item = BrandSafetyFlag.objects.create(**kwargs)
        except IntegrityError:
            # Item is already in queue
            enqueue_item = BrandSafetyFlag.objects.get(**kwargs)
        if enqueue_item.item_type == 0:
            manager = VideoManager(upsert_sections=(Sections.BRAND_SAFETY,))
            model = Video
        else:
            manager = ChannelManager(upsert_sections=(Sections.BRAND_SAFETY,))
            model = Channel
        AuditUtils.reset_brand_safety_score(enqueue_item.item_id, model=model, manager=manager)
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

