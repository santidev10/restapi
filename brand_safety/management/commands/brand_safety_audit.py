import logging

from django.core.management.base import BaseCommand
from pid import PidFile
from pid import PidFileError

from audit_tool.models import APIScriptTracker
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from brand_safety.constants import CHANNEL
from brand_safety.constants import VIDEO
from brand_safety.models import BrandSafetyFlag
from brand_safety.models import BrandSafetyFlagQueueEmptyException
from utils.es_components_cache import flush_cache


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'
    QUEUE = "queue"
    MANUAL = "manual"
    DISCOVERY = "discovery"
    UPDATE = "update"
    VIDEOS = "videos"
    TYPES = (QUEUE, MANUAL, DISCOVERY, UPDATE, VIDEOS)
    MANUAL_TYPES = (CHANNEL, VIDEO)

    def add_arguments(self, parser):
        parser.add_argument(
            "--type",
            help="Type of audit to be executed",
        )
        parser.add_argument(
            "--ids",
            help="Manual brand safety scoring, should provide ids to update"
        )

    def handle(self, *args, **kwargs):
        audit_type = kwargs["type"]
        if audit_type not in self.TYPES:
            raise ValueError(f"Audit type: {audit_type} invalid.")

        handlers = {
            self.QUEUE: self._handle_queue,
            self.MANUAL: self._handle_manual,
            self.DISCOVERY: self._handle_discovery,
            self.UPDATE: self._handle_update,
            self.VIDEOS: self._handle_videos
        }

        handler = handlers[audit_type]
        pid_file = f"{audit_type}_brand_safety.pid"
        try:
            with PidFile(pid_file, piddir=".") as pid:
                self.run(handler, *args, **kwargs)
        except PidFileError:
            pass

    def run(self, handler, *args, **options):
        try:
            handler(*args, **options)
        except Exception as e:
            logger.exception(e)

    def _handle_queue(self, *args, **kwargs):
        """
        Dequeue channel items from BrandSafetyFlag and audit
        :param args:
        :param kwargs:
        :return:
        """
        auditor = BrandSafetyAudit(discovery=False)
        try:
            channel_dequeue_qs = BrandSafetyFlag.dequeue(1, dequeue_limit=10)
            channel_dequeue_ids = [item.item_id for item in channel_dequeue_qs]
            auditor.manual_channel_audit(channel_dequeue_ids)
            flush_cache()
            BrandSafetyFlag.objects.filter(item_id__in=channel_dequeue_ids, item_type=1).delete()
        except BrandSafetyFlagQueueEmptyException:
            pass

    def _handle_manual(self, *args, **options):
        """
        Manually audit video or channels with provided ids
        :param args:
        :param options:
        :return:
        """
        manual_type = options["manual"]
        manual_ids = options["ids"].strip().split(",")
        auditor = BrandSafetyAudit(discovery=False)
        config = {
            CHANNEL: auditor.manual_channel_audit,
            VIDEO: auditor.manual_video_audit
        }
        try:
            manual_auditor = config[manual_type]
            manual_auditor(manual_ids)
        except KeyError:
            raise ValueError(f"Invalid manual type: {manual_type}")

    def _handle_discovery(self, *args, **options):
        """
        Find channels that have not been audited
        :param args:
        :param options:
        :return:
        """
        api_tracker = APIScriptTracker.objects.get_or_create(name="BrandSafetyDiscovery")[0]
        auditor = BrandSafetyAudit(api_tracker=api_tracker, discovery=True)
        auditor.run()

    def _handle_update(self, *args, **options):
        """
        Update channels and videos
        :param args:
        :param options:
        :return:
        """
        api_tracker = APIScriptTracker.objects.get_or_create(name="BrandSafetyUpdate")[0]
        auditor = BrandSafetyAudit(api_tracker=api_tracker, discovery=False)
        auditor.run()

    def _handle_videos(self, *args, **options):
        """
        Score only videos
        :param args:
        :param options:
        :return:
        """
        auditor = BrandSafetyAudit(discovery=False)
        auditor.audit_all_videos()
