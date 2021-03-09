import logging

from .base_auditor import BaseAuditor
from .constants import CHANNEL_SOURCE
from .constants import VIDEO_SOURCE
from .video_auditor import VideoAuditor
from brand_safety.auditors.serializers import BrandSafetyChannel
from brand_safety.audit_models.brand_safety_channel_audit import BrandSafetyChannelAudit
from es_components.constants import VIDEO_CHANNEL_ID_FIELD
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
from es_components.models import Channel
from utils.search_after import search_after

logger = logging.getLogger(__name__)


class ChannelAuditor(BaseAuditor):
    es_model = Channel

    def __init__(self, *args, **kwargs):
        """
        Class to handle channel brand safety scoring logic
        :param audit_utils: AuditUtils -> Optional passing of an AuditUtils object, as it is expensive to instantiate
            since it compiles keyword processors of every brand safety BadWord row
        """
        super().__init__(*args, **kwargs)
        self.video_auditor = VideoAuditor(audit_utils=self.audit_utils)

    def get_data(self, channel_id: str) -> BrandSafetyChannel:
        """
        Retrieve Channels and add data to instances using BrandSafetyChannelSerializer
        :param channel_id: str
        :return: list
        """
        channel = self.channel_manager.get([channel_id], skip_none=True, source=CHANNEL_SOURCE)[0]
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
        handlers = [self._blocklist_handler, self.audit]
        try:
            channel = self.get_data(channel_id)
        except IndexError:
            return
        for handler in handlers:
            result = handler(channel, index=index)
            if result:
                if index is True:
                    self.index_audit_results(self.channel_manager, [result])
                return result

    def _query_channel_videos(self, channel_id: str) -> list:
        """
        Target for channel video query thread pool
        :param channel_id: str
        :return:
        """
        query = QueryBuilder().build().must().term().field(VIDEO_CHANNEL_ID_FIELD).value(channel_id).get() \
                & QueryBuilder().build().must().exists().field(Sections.GENERAL_DATA).get()
        results = []
        for batch in search_after(query, self.video_manager):
            results.extend(batch)
        return results

    def audit(self, channel: Channel, index=True) -> Channel:
        """
        Audit single channel
        :param channel: Can either be channel id string or Channel instance
        :param index: bool -> Determines whether to index video audit and channel results
        :return: Channel instantiated with brand safety data, ready for upsert
        """
        if isinstance(channel, str):
            channel = self.get_data(channel)
        self._set_channel_data(channel)
        channel.video_audits = self.video_auditor.process_for_channel(channel, channel.videos, index=index)
        channel_audit = BrandSafetyChannelAudit(channel, self.audit_utils)
        channel_audit.run()
        return channel_audit.add_brand_safety_data()

    def _set_channel_data(self, channel: BrandSafetyChannel):
        """
        Set video data on Channel document
        :param channel: BrandSafetyChannel result
        :return:
        """
        videos = self._query_channel_videos(channel.main.id)
        channel.videos = videos
