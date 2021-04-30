import concurrent.futures

from .utils import AuditUtils
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from utils.utils import chunks_generator
from utils.exception import upsert_retry


class BaseAuditor:
    es_model = None

    def __init__(self, audit_utils=None):
        self.audit_utils = audit_utils or AuditUtils()
        # Set sections as None as source fields will be set during manager.get method calls
        self.channel_manager = ChannelManager(
            sections=None,
            upsert_sections=(Sections.BRAND_SAFETY,)
        )
        self.video_manager = VideoManager(
            sections=None,
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
            upsert_retry(es_manager, audit_results, **upsert_params)
        else:
            args = chunks_generator(audit_results, 500)
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(upsert_retry, es_manager, list(arg), **upsert_params) for arg in args]
                for future in concurrent.futures.as_completed(futures):
                    future.result()

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
            doc.brand_safety.overall_score = 0
            return doc
