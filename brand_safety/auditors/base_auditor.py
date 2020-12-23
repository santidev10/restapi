from concurrent.futures import ThreadPoolExecutor

from .constants import CHANNEL_SECTIONS
from .constants import VIDEO_SECTIONS
from .utils import AuditUtils
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from utils.utils import chunks_generator


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

    def index_audit_results(self, es_manager, audit_results: list) -> None:
        """
        Update audits with audited brand safety scores
        :param es_manager: VideoManager | ChannelManager
        :param audit_results: list -> Channel | Video
        :param size: int -> Chunk size for each thread
        :return: list
        """
        upsert_params = dict(
            refresh=False, raise_on_error=False,
            raise_on_exception=False, yield_ok=False
        )
        if len(audit_results) < 100:
            es_manager.upsert(audit_results, **upsert_params)
        else:
            args = chunks_generator(audit_results, 100)
            with ThreadPoolExecutor(max_workers=20) as executor:
                list(executor.submit(es_manager.upsert, list(arg), **upsert_params) for arg in args)

    def _blocklist_handler(self, doc, **__):
        """
        Handler for blocklisted documents
        If blocklisted, bypass audit logic and instantiate with brand safety overall score since scoring is not required
            for blocklisted items
        :param doc: Channel | Video
        :param __: Unused kwargs that may be passed into another handler
        :return:
        """
        if doc.custom_properties.blocklist is True:
            handled = self._blank_doc(doc.main.id)
            handled.populate_brand_safety(overall_score=0)
            return handled

    def _blank_doc(self, item_id):
        return self.es_model(item_id)
