"""
BaseSegment models module
"""
import logging

from celery import task
from django.contrib.postgres.fields import JSONField
from django.db import IntegrityError
from django.db.models import CharField
from django.db.models import ForeignKey
from django.db.models import Manager
from django.db.models import Model
from django.db.models import SET_NULL


from utils.models import Timestampable

# pylint: disable=import-error
# pylint: enable=import-error

logger = logging.getLogger(__name__)


class SegmentManager(Manager):
    """
    Extend default segment manager
    """
    def update_statistics(self):
        """
        Make re-count of all segments statistic and mini-dash fields
        """
        segments = self.all()
        for segment in segments:
            logger.info('Updating statistics for {}-segment [{} ids]: {}'.format(segment.segment_type, len(segment.related_ids_list), segment.title))
            segment.update_statistics(segment)


class BaseSegment(Timestampable):
    """
    Base segment model
    """
    title = CharField(max_length=255, null=True, blank=True)
    mini_dash_data = JSONField(default=dict())
    owner = ForeignKey('userprofile.userprofile', null=True, blank=True, on_delete=SET_NULL)

    class Meta:
        abstract = True

    def get_related_ids(self):
        return self.related.values_list("related_id", flat=True)

    def add_related_ids(self, ids):
        assert isinstance(ids, list) or isinstance(ids, set), "ids must be a list or set"
        related_model = self.related.model
        objs = [related_model(segment_id=self.pk, related_id=related_id) for related_id in ids]
        error_msg = "duplicate key value violates unique constraint"
        try:
            related_model.objects.bulk_create(objs)
        except IntegrityError as e:
            if e.args and error_msg in e.args[0]:
                for obj in objs:
                    try:
                        obj.save()
                    except IntegrityError as e:
                        if e.args and error_msg in e.args[0]:
                            continue
                        else:
                            raise
            else:
                raise

    def replace_related_ids(self, ids):
        self.related.model.objects.filter(segment=self).delete()
        self.add_related_ids(ids)

    @property
    def related_ids_list(self):
        return self.related.all().values_list('related_id', flat=True)

    def delete_related_ids(self, ids):
        assert isinstance(ids, list) or isinstance(ids, set), "ids must be a list or set"
        related_manager = self.related.model.objects
        related_manager.filter(segment_id=self.pk, related_id__in=ids)\
                       .delete()

    def cleanup_related_records(self, alive_ids):
        ids = set(self.get_related_ids()) - alive_ids
        if ids:
            self.related.model.objects.filter(related_id__in=ids).delete()

    def obtain_singledb_data(self):
        ids = list(self.get_related_ids())
        return self.singledb_method(ids=ids, top=3, minidash=1)

    @task
    def update_statistics(self):
        data = self.obtain_singledb_data()
        # just return on any fail
        if data is None:
            return

        # populate statistics fields
        self.populate_statistics_fields(data)

        self.save()
        return "Done"

    def duplicate(self, owner):
        exclude_fields = ['updated_at', 'id', 'created_at', 'owner_id', 'related']
        segment_data = {f:getattr(self, f) for f in self._meta.get_all_field_names() if f not in exclude_fields}
        segment_data['title'] = '{} (copy)'.format(self.title)
        segment_data['owner'] = owner
        segment_data['category'] = 'private'
        duplicated_segment = self.__class__.objects.create(**segment_data)
        related_manager = self.__class__.related.rel.related_model.objects
        related_list = list(self.related.all())
        for related in related_list:
            related.pk = None
            related.segment = duplicated_segment
        related_manager.bulk_create(related_list)
        return duplicated_segment

    def sync_recommend_channels(self, channel_ids):
        if hasattr(self, 'top_recommend_channels'):
            for ch_id in channel_ids:
                if ch_id in self.top_recommend_channels:
                    self.top_recommend_channels.remove(ch_id)
            self.save()


class BaseSegmentRelated(Model):
    # the 'segment' field must be defined in a successor model like next:
    # segment = ForeignKey(Segment, related_name='related')
    related_id = CharField(max_length=30)

    class Meta:
        abstract = True
        unique_together = (('segment', 'related_id'),)
