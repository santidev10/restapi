import logging

from .base_auditor import BaseAuditor
from .video_auditor import VideoAuditor
from brand_safety.auditors.serializers import BrandSafetyChannel
from brand_safety.audit_models.brand_safety_channel_audit import BrandSafetyChannelAudit
from es_components.constants import VIDEO_CHANNEL_ID_FIELD
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
from es_components.models import Channel

logger = logging.getLogger(__name__)


class ChannelAuditor(BaseAuditor):
    es_model = Channel

    CHANNEL_BATCH_SIZE = 2
    MAX_THREAD_POOL = 3

    def __init__(self, *args, ignore_vetted_brand_safety=False, **kwargs):
        """
        Class to handle channel brand safety scoring logic
        :param ignore_vetted_brand_safety: bool -> Determines if the script should use vetted brand safety categories
            to set category scores and overall score to 0 if not safe or to 100 if safe
            A channel is determined safe or not safe by the presence of task_us_data.brand_safety categories
        :param audit_utils: AuditUtils -> Optional passing of an AuditUtils object, as it is expensive to instantiate
            since it compiles keyword processors of every brand safety BadWord row
        """
        super().__init__(*args, **kwargs)
        self._config = dict(
            ignore_vetted_brand_safety=ignore_vetted_brand_safety,
        )
        self.video_auditor = VideoAuditor(ignore_vetted_brand_safety=ignore_vetted_brand_safety,
                                          audit_utils=self.audit_utils)

    def get_data(self, channel_id: str):
        """
        Retrieve Channels and add data to instances using BrandSafetyChannelSerializer
        :param channel_id: str
        :return: list
        """
        channel = self.channel_manager.get([channel_id], skip_none=True)[0]
        with_data = BrandSafetyChannel(channel).to_representation()
        return with_data

    def process(self, channel_id, index=True) -> Channel:
        """
        Process audit with handler depending on document data
        This method sequentially applies a handler method to the channel and if data is returned from the handler,
            the data is upserted and returned
        :param channel_id: str
        :param index: bool
        :return:
        """
        handlers = [self._blocklist_handler, self._vetted_handler, self.audit]
        try:
            channel = self.get_data(channel_id)
        except IndexError:
            return

        for handler in handlers:
            result = handler(channel)
            if result:
                if index is True:
                    self.channel_manager.upsert([result])
                return result

    def _query_channel_videos(self, channel_id: str) -> list:
        """
        Target for channel video query thread pool
        :param channel_ids: list
        :return:
        """
        query = QueryBuilder().build().must().term().field(VIDEO_CHANNEL_ID_FIELD).value(channel_id).get() \
                & QueryBuilder().build().must().exists().field(Sections.GENERAL_DATA).get()
        results = self.video_manager.search(query, limit=10000).execute().hits
        return results

    def audit(self, channel: Channel, index_videos=True):
        """
        Audit single channel
        :param channel: Can either be channel id string or Channel instance
        :param index_videos: bool -> Determines whether to index video audit and channel results
        :return: dict -> brand safety section data to update with
        """
        if isinstance(channel, str):
            channel = self.get_data(channel)
        self._set_channel_data(channel)
        channel.video_audits = self.video_auditor.process_for_channel(channel, channel.videos, index=index_videos)
        channel_audit = BrandSafetyChannelAudit(channel, self.audit_utils,
                                                ignore_vetted_brand_safety=self._config.get(
                                                    "ignore_vetted_brand_safety"))
        channel_audit.run()
        audit_data = channel_audit.instantiate_es()
        return audit_data

    def _set_channel_data(self, channel):
        """
        Set video data on Channel document
        :param channel: BrandSafetyChannel result
        :return:
        """
        videos = self._query_channel_videos(channel.main.id)
        channel.videos = videos
