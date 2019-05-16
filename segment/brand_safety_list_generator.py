import requests

from audit_tool.models import APIScriptTracker
import brand_safety.constants as constants
from brand_safety.audit_providers.base import AuditProvider
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentTitles
from singledb.connector import SingleDatabaseApiConnector
from utils.elasticsearch import ElasticSearchConnector


class BrandSafetyListGenerator(object):
    channel_score_fail_limit = 89
    video_score_fail_limit = 89

    def __init__(self, *_, **kwargs):
        self.channel_script_tracker = kwargs["channel_api_tracker"]
        self.video_script_tracker = kwargs["video_api_tracker"]
        self.channel_cursor_id = self.channel_script_tracker.cursor_id
        self.video_cursor_id = self.video_script_tracker.cursor_id
        self.sdb_connector = SingleDatabaseApiConnector()
        self._set_master_segments()
        self.es_connector = ElasticSearchConnector()
        self.audit_proider = AuditProvider()

    def run(self):
        for channel_batch in self._channel_es_generator(last_id=self.channel_script_tracker):
            pass

        for video_batch in self._video_es_generator(last_id=self.video_script_tracker)
            pass

    def process(self, items):


    def _set_master_segments(self):
        """
        Set required persistent segments to save to
        :return:
        """
        self.blacklist_channels, _ = PersistentSegmentChannel.objects.get_or_create(
            title=PersistentSegmentTitles.CHANNELS_BRAND_SAFETY_MASTER_BLACKLIST_SEGMENT_TITLE, category="blacklist")
        self.whitelist_channels, _ = PersistentSegmentChannel.objects.get_or_create(
            title=PersistentSegmentTitles.CHANNELS_BRAND_SAFETY_MASTER_WHITELIST_SEGMENT_TITLE, category="whitelist")
        self.blacklist_videos, _ = PersistentSegmentVideo.objects.get_or_create(
            title=PersistentSegmentTitles.VIDEOS_BRAND_SAFETY_MASTER_BLACKLIST_SEGMENT_TITLE, category="blacklist")
        self.whitelist_videos, _ = PersistentSegmentVideo.objects.get_or_create(
            title=PersistentSegmentTitles.VIDEOS_BRAND_SAFETY_MASTER_WHITELIST_SEGMENT_TITLE, category="whitelist")

    def _sort_brand_safety(self, audits):
        """
        Sort audits by fail or pass based on their overall brand safety score
        :param audits: list
        :return: tuple -> lists of sorted audits
        """
        brand_safety_pass = {}
        brand_safety_fail = {}
        for audit in audits:
            if audit.target_segment == PersistentSegmentCategory.WHITELIST:
                brand_safety_pass[audit.pk] = audit
            elif audit.target_segment == PersistentSegmentCategory.BLACKLIST:
                brand_safety_fail[audit.pk] = audit
            else:
                pass
        return brand_safety_pass, brand_safety_fail

    def _save_master_results(self, *_, **kwargs):
        """
        Save Video and Channel audits based on their brand safety results to their respective persistent segments
        :param kwargs: Persistent segments to save to
        :return: None
        """
        audits = kwargs["audits"]
        # Persistent segments that store brand safety objects
        whitelist_segment = kwargs["whitelist_segment"]
        blacklist_segment = kwargs["blacklist_segment"]
        # Related segment model used to instantiate database objects
        related_segment_model = kwargs["related_segment_model"]
        # Sort audits by brand safety results
        brand_safety_pass, brand_safety_fail = self._sort_brand_safety(audits)
        brand_safety_pass_pks = list(brand_safety_pass.keys())
        brand_safety_fail_pks = list(brand_safety_fail.keys())
        # Remove brand safety failed audits from whitelist as they are no longer belong in the whitelist
        whitelist_segment.related.filter(related_id__in=brand_safety_fail_pks).delete()
        blacklist_segment.related.filter(related_id__in=brand_safety_pass_pks).delete()
        # Get related ids that still exist to avoid creating duplicates with results
        exists = related_segment_model.objects \
            .filter(
            Q(segment=whitelist_segment) | Q(segment=blacklist_segment),
            related_id__in=brand_safety_pass_pks + brand_safety_fail_pks
        ).values_list("related_id", flat=True)
        # Set difference to get audits that need to be created
        to_create = set(brand_safety_pass_pks + brand_safety_fail_pks) - set(exists)
        # Instantiate related models with new appropriate segment and segment types
        to_create = [
            brand_safety_pass[pk].instantiate_related_model(related_segment_model, whitelist_segment,
                                                            segment_type=constants.WHITELIST)
            if brand_safety_pass.get(pk) is not None
            else
            brand_safety_fail[pk].instantiate_related_model(related_segment_model, blacklist_segment,
                                                            segment_type=constants.BLACKLIST)
            for pk in to_create
        ]
        related_segment_model.objects.bulk_create(to_create)
        return brand_safety_pass, brand_safety_fail

    @staticmethod
    def _get_segment_title(segment_type, category, segment_category):
        """
        Return formatted Persistent segment title
        :param segment_type: channel or video
        :param category: Item category e.g. Politics
        :param segment_category: whitelist or blacklist
        :return:
        """
        categorized_segment_title = "{}s {} {}".format(
            segment_type.capitalize(),
            category,
            segment_category.capitalize(),
        )
        return categorized_segment_title

    def instantiate_related_items(self, items, segment, related_segment_model):
        to_create = []
        for item in items:
            related_item = related_segment_model(
                related_id=''
            )

    def _channel_es_generator(self, last_id=None):
        body = {
            "query": {
                "range": {
                    "overall_score": {
                        "gte": self.channel_score_fail_limit
                    },
                    "channel_id": {
                        "gte": last_id
                    },
                }
            }
        }
        while True:
            channels = self.es_connector.search(index=constants.BRAND_SAFETY_CHANNEL_ES_INDEX, **body)
            yield channels
            break

    def _video_es_generator(self, last_id=None):
        pass


