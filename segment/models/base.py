"""
BaseSegment models module
"""
import logging
from itertools import chain

from django.conf import settings
from django.contrib.postgres.fields import JSONField, ArrayField
from django.db import IntegrityError
from django.db.models import CharField
from django.db.models import IntegerField
from django.db.models import ForeignKey
from django.db.models import Manager
from django.db.models import Model
from django.db.models import SET_NULL

from singledb.connector import SingleDatabaseApiConnector as Connector
from utils.models import Timestampable
from utils.utils import chunks_generator

logger = logging.getLogger(__name__)

MAX_ITEMS_GET_FROM_SINGLEDB = 10000
MAX_ITEMS_DELETE_FROM_DB = 10


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
            logger.info(
                'Updating statistics for {}-segment [{} ids]: {}'.format(
                    segment.segment_type, len(segment.related_ids_list),
                    segment.title))
            segment.update_statistics()

    def cleanup_related_records(self):
        segments = self.all()

        if segments:
            segment = segments[0]

            related_model = segment.related.model

            for cleanup_ids in segment.get_cleanup_singledb_data():
                start = 0
                end = MAX_ITEMS_DELETE_FROM_DB
                step = MAX_ITEMS_DELETE_FROM_DB

                cleanup_ids = list(cleanup_ids)

                _cleanup_ids = cleanup_ids[start:end]
                while _cleanup_ids:
                    related_model.objects.filter(related_id__in=_cleanup_ids).delete()

                    start += step
                    end += step

                    _cleanup_ids = cleanup_ids[start:end]

        self.update_statistics()


class BaseSegment(Timestampable):
    """
    Base segment model
    """
    title = CharField(max_length=255, null=True, blank=True)
    mini_dash_data = JSONField(default=dict())
    adw_data = JSONField(default=dict())
    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                       on_delete=SET_NULL)
    shared_with = ArrayField(CharField(max_length=200), blank=True,
                             default=list)
    pending_updates = IntegerField(default=0, null=False)
    related = None
    related_aw_statistics_model = None
    segment_type = None
    id_fields_name = None
    sources = None

    class Meta:
        abstract = True
        ordering = ["pk"]

    def get_related_ids(self):
        return self.related.values_list("related_id", flat=True)

    def get_related_model_ids(self):
        return self.related.model.objects.order_by("pk").values_list("related_id", flat=True).distinct()

    def add_related_ids(self, ids):
        assert isinstance(ids, list) or isinstance(ids,
                                                   set), "ids must be a list or set"
        related_model = self.related.model
        objs = [related_model(segment_id=self.pk, related_id=related_id) for
                related_id in ids]
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

    @property
    def shared_with_string(self, separation_symbol="|"):
        return separation_symbol.join(self.shared_with)

    def delete_related_ids(self, ids):
        assert isinstance(ids, list) or isinstance(ids,
                                                   set), "ids must be a list or set"
        related_manager = self.related.model.objects
        related_manager.filter(segment_id=self.pk, related_id__in=ids) \
            .delete()

    def cleanup_related_records(self, ids):
        if ids:
            self.related.model.objects.filter(related_id__in=ids).delete()

    def update_statistics(self):
        """
        Process segment statistics fields
        """
        ids = self.get_related_ids()
        ids_count = ids.count()
        if ids_count > settings.MAX_SEGMENT_TO_AGGREGATE:
            data = self.get_data_by_ids(ids, end=settings.MAX_SEGMENT_TO_AGGREGATE)
            self._set_total_for_huge_segment(ids_count, data)
            self.adw_data = dict()
            self.save()
            return
        data = self.get_data_by_ids(ids)
        # just return on any fail
        if data is None:
            return
        # populate statistics fields
        self.populate_statistics_fields(data)
        self.get_adw_statistics()
        self.save()
        return "Done"

    def get_ids_hash(self, ids, start=None, end=None):
        ids = list(ids)[start:end]
        return Connector().store_ids(ids)

    def get_data_by_ids(self, ids, start=None, end=None):
        ids_hash = self.get_ids_hash(ids, start, end)
        return self.obtain_singledb_data(ids_hash)

    def get_cleanup_singledb_data(self):
        pk_gt_value = 0
        end = MAX_ITEMS_GET_FROM_SINGLEDB

        def _query(_pk_gt_value):
            return self.get_related_model_ids().filter(pk__gt=_pk_gt_value)

        _ids = _query(pk_gt_value)[:end]

        print("ids {}".format(_ids.count()))

        while _ids:
            last_ids = list(_ids)[-1]
            pk_gt_value = self.related.model.objects.filter(related_id=last_ids).first().pk

            print("last ids {}".format(pk_gt_value))

            _ids_hash = self.get_ids_hash(_ids)
            cleanup_ids = set(_ids) - set(self._get_alive_singledb_data(_ids_hash))

            if cleanup_ids:
                yield cleanup_ids

            _ids = _query(pk_gt_value)[:end]

    def _get_alive_singledb_data(self, ids_hash):
        params = {
            "ids_hash": ids_hash,
            "fields": self.id_fields_name,
            "sources": self.sources,
            "size": 10000
        }
        data = self.singledb_method(query_params=params)
        return [item.get(self.id_fields_name) for item in data.get('items')]

    def _set_total_for_huge_segment(self, items_count, data):
        raise NotImplementedError

    def obtain_singledb_data(self, ids_hash):
        raise NotImplementedError

    def populate_statistics_fields(self, data):
        raise NotImplementedError

    def load_list_batch_generator(self, filters):
        raise NotImplementedError

    def add_by_filters(self, filters):
        logger.debug("%s add_by_filters started", self)
        items_imported = 0
        all_items = self.load_list_batch_generator(filters)
        for batch in chunks_generator(all_items, 5000):
            ids = [item["pk"] for item in batch]
            self.add_related_ids(ids)
            items_imported += len(ids)
            logger.debug("%s add_by_filters progress: imported %d", self, items_imported)
        logger.debug("%s add_by_filters finished", self)
        self.update_statistics()

    def get_adw_statistics(self):
        """
        Prepare segment adwords statistics
        """
        from segment.models.utils import count_segment_adwords_statistics
        # prepare adwords statistics
        adwords_statistics = count_segment_adwords_statistics(self)

        # finalize data
        self.adw_data.update(adwords_statistics)

    def duplicate(self, owner):
        exclude_fields = ['updated_at', 'id', 'created_at', 'owner_id',
                          'related', 'shared_with']
        # todo: refactor
        fields = list(set(chain.from_iterable(
            (field.name, field.attname) if hasattr(field, 'attname') else (field.name,)
            for field in self._meta.get_fields()
            # For complete backwards compatibility, you may want to exclude
            # GenericForeignKey from the results.
            if not (field.many_to_one and field.related_model is None)
        )))
        segment_data = {
            f: getattr(self, f)
            for f in fields
            if f not in exclude_fields
        }
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

    def __repr__(self):
        return "<{}>{ id: {}, name: {}}".format(type(self).__name__, self.id, self.title)


class BaseSegmentRelated(Model):
    # the 'segment' field must be defined in a successor model like next:
    # segment = ForeignKey(Segment, related_name='related')
    related_id = CharField(max_length=100)

    class Meta:
        abstract = True
        unique_together = (('segment', 'related_id'),)
