from concurrent.futures import ThreadPoolExecutor

from .utils import AuditUtils
from .constants import CHANNEL_SECTIONS
from .constants import VIDEO_SECTIONS
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from utils.utils import chunks_generator


class BaseAuditor:
    es_model = None
    _config = {} # should be set in __init__ on child classes

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

    def index_audit_results(self, es_manager, audits: list) -> None:
        """
        Update audits with audited brand safety scores
        Check if each document should be upserted depending on config, as vetted videos should not always be updated
        :param es_manager: VideoManager | ChannelManager
        :param audits: list -> BrandSafetyVideo | BrandSafetyChannel audits
        :return: list
        """
        to_upsert = [
            audit.instantiate_es() for audit in audits
        ]
        upsert_params = dict(
            refresh=False, raise_on_error=False,
            raise_on_exception=False, yield_ok=False
        )
        with ThreadPoolExecutor(max_workers=10) as executor:
            for chunk in chunks_generator(to_upsert, 500):
                executor.submit(es_manager.upsert, chunk, **upsert_params)

    def _blocklist_handler(self, doc, **__):
        if doc.custom_properties.blocklist is True:
            handled = self._blank_doc(doc.main.id)
            handled.populate_brand_safety(overall_score=0)
            return handled

    def _vetted_handler(self, doc, **__):
        if self._config.get("ignore_vetted_brand_safety") is False and doc.task_us_data.last_vetted_at is not None:
            handled = self._blank_doc(doc.main.id)
            handled.populate_brand_safety(**self.audit_utils.get_brand_safety_data(doc))
            return handled

    def _blank_doc(self, item_id):
        return self.es_model(item_id)