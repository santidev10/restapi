from .utils import AuditUtils
from .constants import CHANNEL_SECTIONS
from .constants import VIDEO_SECTIONS
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager


class BaseAuditor:
    es_model = None

    def __init__(self, audit_utils=None):
        self.audit_utils = audit_utils or AuditUtils()
        self.channel_manager = ChannelManager(
            sections=CHANNEL_SECTIONS,
            upsert_sections=(Sections.BRAND_SAFETY,)
        )
        self.video_manager = VideoManager(
            sections=VIDEO_SECTIONS,
            upsert_sections=(Sections.BRAND_SAFETY, Sections.CHANNEL)
        )

    def _blocklist_handler(self, doc):
        if doc.custom_properties.blocklist is True:
            handled = self._blank_doc(doc.main.id)
            handled.populate_brand_safety(overall_score=0)
            return handled

    def _vetted_handler(self, doc):
        if doc.task_us_data.last_vetted_at is not None:
            handled = self._blank_doc(doc.main.id)
            handled.populate_brand_safety(**self.audit_utils.get_brand_safety_data(doc))
            return handled

    def _blank_doc(self, item_id):
        return self.es_model(item_id)