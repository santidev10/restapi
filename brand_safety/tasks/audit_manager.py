from audit_tool.models import APIScriptTracker

from es_components.models import Channel
from es_components.models import Video
from es_components.query_builder import QueryBuilder
from es_components.constants import MAIN_ID_FIELD

class AuditManager(object):
    def __init__(self, audit_type):
        config = {
            0: {
                "query_builder": self._create_channel_update_query,
                "model": Channel,
                "sort": "-stats.subscribers"
            },
            1: {
                "query_builder": self._create_channel_discovery_query,
                "model": Channel,
                "sort": "-stats.subscribers"
            },
            2: {
                "query_builder": self._create_video_discovery_query,
                "model": Video,
                "sort": "-stats.views"
            }
        }
        self.update(config[audit_type])


    def next_batch(self, cursor_id, size=100):
        """
        Get channels to score with no brand safety data
        :param cursor_id:
        :return:
        """
        cursor_id = cursor_id or ""

        query = self.query_creator(cursor_id)
        response = self.model.search().query(query).sort([self.sort]).source(MAIN_ID_FIELD).execute()
        # response = self.channel_manager.search(query, limit=self.channel_master_batch_size,
        #                                        sort=("-stats.subscribers",)).source(self.CHANNEL_FIELDS).execute()
        items = [item.main.id for item in response.hits if item.main.id != cursor_id]

        return items
        # yield data
        # cursor_id = results[-1].main.id
        # self.script_tracker = self.audit_utils.set_cursor(self.script_tracker, cursor_id, integer=False)
        # self.cursor_id = self.script_tracker.cursor_id

    def _create_channel_update_query(self, cursor_id):
        query = QueryBuilder().build().must().exists().field(MAIN_ID_FIELD).get() \
            & QueryBuilder().build().must().range().field(MAIN_ID_FIELD).gte(cursor_id).get() \
            & QueryBuilder().build().must().range().field("stats.total_videos_count").gt(0).get() \
            & QueryBuilder().build().must().range().field("stats.subscribers").gte(self.MINIMUM_SUBSCRIBER_COUNT).get() \
            & QueryBuilder().build().must().range().field("brand_safety.updated_at").lte(self.UPDATE_TIME_THRESHOLD).get()
        return query

    def _create_channel_discovery_query(self):
        query = QueryBuilder().build().must().range().field("stats.total_videos_count").gt(0).get() \
            & QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get() \
            & QueryBuilder().build().must().range().field("stats.subscribers").gte(self.MINIMUM_SUBSCRIBER_COUNT).get()
        return query

    def _create_video_discovery_query(self):
        query = QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get() \
            & QueryBuilder().build().must().exists().field(Sections.GENERAL_DATA).get() \
            & QueryBuilder().build().must().range().field("stats.views").gte(self.MINIMUM_VIEW_COUNT).get()

    @staticmethod
    def get_cursor(name):
        cursor = APIScriptTracker.objects.get_or_create(
            name=CHANNEL_DISCOVERY_TRACKER,
            defaults={
                "cursor_id": None
            }
        )
        return cursor