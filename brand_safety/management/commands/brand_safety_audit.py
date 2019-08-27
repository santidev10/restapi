import logging

from django.core.management.base import BaseCommand
from django.utils import timezone
from pid import PidFile
from pid import PidFileError

from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from brand_safety.models import BrandSafetyFlag
from audit_tool.models import APIScriptTracker

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'
    QUEUE = "queue"
    MANUAL = "manual"
    DISCOVERY = "discovery"
    UPDATE = "update"
    VIDEOS = "videos"
    TYPES = (QUEUE, MANUAL, DISCOVERY, UPDATE, VIDEOS)

    def add_arguments(self, parser):
        parser.add_argument(
            "--type",
            help="Type of audit to be executed",
        )
        parser.add_argument(
            "--manual",
            help="Manual brand safety scoring, video or channel"
        )
        parser.add_argument(
            "--ids",
            help="Manual brand safety scoring, should provide ids to update"
        )
        parser.add_argument(
            "--videos",
            help="Score only videos"
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
        Dequeue channel and video items from BrandSafetyFlag and audit
        :param args:
        :param kwargs:
        :return:
        """
        auditor = BrandSafetyAudit(discovery=False)
        video_dequeue_qs = BrandSafetyFlag.dequeue(0)
        video_dequeue_ids = [item.item_id for item in video_dequeue_qs]
        auditor.manual_video_audit(video_dequeue_ids)
        video_dequeue_qs.update(completed_at=timezone.now())

        channel_dequeue_qs = BrandSafetyFlag.dequeue(1)
        channel_dequeue_ids = [item.item_id for item in channel_dequeue_qs]
        auditor.manual_channel_audit(channel_dequeue_ids)
        channel_dequeue_qs.update(completed_at=timezone.now())

    def _handle_manual(self, *args, **options):
        """
        Manually audit video or channels with provided ids
        :param args:
        :param options:
        :return:
        """
        manual_type = options["manual"]
        manual_ids = options["ids"].strip().split(",")
        auditor = BrandSafetyAudit(**options)
        if manual_type == "video":
            auditor.manual_video_audit(manual_ids)
        elif manual_type == "channel":
            auditor.manual_channel_audit(manual_ids)
        else:
            raise ValueError("Unsupported manual type: {}".format(manual_type))

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
