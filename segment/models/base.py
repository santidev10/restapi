"""
BaseSegment models module
"""
from celery import task
from datetime import timedelta
import logging
from django.contrib.postgres.fields import JSONField
from django.db import IntegrityError
from django.db.models import CharField
from django.db.models import ForeignKey
from django.db.models import Manager
from django.db.models import Model
from django.utils import timezone

from segment.mini_dash import SegmentMiniDashGenerator
from singledb.connector import SingleDatabaseApiConnectorException

from utils.models import Timestampable

logger = logging.getLogger(__name__)


class SegmentManager(Manager):
    """
    Extend default segment manager
    """
    def update_statistics(self, forced=False):
        """
        Make re-count of all segments statistic and mini-dash fields
        """
        segments = self.all()
        if not forced:
            time_delta = timezone.now() - timedelta(hours=23)
            segments = segments.filter(updated_at__lte=time_delta)
        for segment in segments:
            logger.info('Updating statistics for {}-segment [{} ids]: {}'.format(segment.segment_type, len(segment.related_ids_list), segment.title))
            segment.update_statistics(segment)


class BaseSegment(Timestampable):
    """
    Base segment model
    """
    title = CharField(max_length=255, null=True, blank=True)
    statistics = JSONField(default=dict())
    mini_dash_data = JSONField(default=dict())
    owner = ForeignKey('userprofile.userprofile', null=True, blank=True)

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
        ids = self.get_related_ids()
        if not ids:
            return []

        # TODO flat may freeze SDB if queryset is too big
        query_params = {"ids": ",".join(ids),
                        "fields": ",".join(self.singledb_fields),
                        "flat": 1}
        try:
            return self.singledb_method(query_params)
        except SingleDatabaseApiConnectorException:
            # TODO add fail logging and, probably, retries
            return

    @task
    def update_statistics(self):
        data = self.obtain_singledb_data()
        # just return on any fail
        if data is None:
            return

        # Check all related records still alive in SDB
        alive_ids = {obj.get("id") for obj in data}
        self.cleanup_related_records(alive_ids)

        # calculate statistics
        self.statistics = self.calculate_statistics(data) if data else {}

        # calculate mini-dash
        self.mini_dash_data = SegmentMiniDashGenerator(data, self).data if data else {}

        self.save()
        return "Done"


class BaseSegmentRelated(Model):
    # the 'segment' field must be defined in a successor model like next:
    # segment = ForeignKey(Segment, related_name='related')
    related_id = CharField(max_length=30)

    class Meta:
        abstract = True
        unique_together = (('segment', 'related_id'),)
