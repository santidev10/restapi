import logging

from django.conf import settings
from django.utils import timezone
from elasticsearch_dsl.search import Q

from administration.notifications import generate_html_email
from brand_safety.auditors.utils import AuditUtils
from es_components.constants import SortDirections
from segment.models import CustomSegmentFileUpload
from segment.models.custom_segment_file_upload import CustomSegmentFileUploadQueueEmptyException
from utils.aws.export_context_manager import ExportContextManager
from utils.aws.s3_exporter import S3Exporter
from utils.aws.ses_emailer import SESEmailer
from segment.models.utils.calculate_segment_details import calculate_statistics
from segment.utils import retry_on_conflict


logger = logging.getLogger(__name__)


class CustomSegmentExportGenerator(S3Exporter):
    bucket_name = settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME
    VIEWS_SORT = {"stats.views": {"order": SortDirections.DESCENDING}}
    SUBSCRIBERS_SORT = {"stats.subscribers": {"order": SortDirections.DESCENDING}}
    CHANNEL_LIMIT = 20000
    VIDEO_LIMIT = 20000
    MAX_API_CALL_RETRY = 20
    RETRY_SLEEP_COEFFICIENT = 1.1
    UPDATE_BATCH_SIZE = 1000

    def __init__(self, updating=False):
        """
        Generate / Update custom segment exports
        :param updating: If updating, then find existing custom segments and regenerate their exports
        """
        self.ses = SESEmailer()
        self.updating = updating
        self.segment_item_ids = []
        self.limit = None

    def generate(self, export=None):
        """
        Either dequeue segment to create and process or updating existing export
        :return:
        """
        # If export is none, then updating
        if export is None:
            try:
                export = CustomSegmentFileUpload.dequeue()
            except CustomSegmentFileUploadQueueEmptyException:
                raise
        segment = export.segment
        owner = segment.owner
        es_manager = segment.get_es_manager()
        serializer = segment.get_serializer()

        if segment.segment_type == 0:
            sort_key = self.VIEWS_SORT
            self.limit = self.VIDEO_LIMIT
        else:
            sort_key = self.SUBSCRIBERS_SORT
            self.limit = self.CHANNEL_LIMIT
        try:
            # Remove all items from this segment to generate relevant items
            retry_on_conflict(segment.remove_all_from_segment, retry_amount=self.MAX_API_CALL_RETRY, sleep_coeff=self.RETRY_SLEEP_COEFFICIENT)
            query = Q(export.query)
            es_generator = self.es_generator(es_manager, query, sort_key, self.limit, serializer)
        except Exception:
            raise
        log_message = "Updating" if self.updating else "Generating"
        logger.error("{} export: {}".format(log_message, segment.title))
        export_manager = ExportContextManager(es_generator, export.columns)
        s3_key = self.get_s3_key(owner.id, segment.title)
        self.export_to_s3(export_manager, s3_key, get_key=False)

        # Add segment UUID to documents
        for batch in AuditUtils.batch(self.segment_item_ids, self.UPDATE_BATCH_SIZE):
            retry_on_conflict(es_manager.add_to_segment_by_ids, batch, segment.uuid, retry_amount=self.MAX_API_CALL_RETRY, sleep_coeff=self.RETRY_SLEEP_COEFFICIENT)
        self._finalize_export(export, segment, owner, s3_key)

    @staticmethod
    def get_s3_key(owner_id, segment_title):
        return "{owner_id}/{segment_title}.csv".format(owner_id=owner_id, segment_title=segment_title)

    def _finalize_export(self, export, segment, owner, s3_key: str):
        """
        Finalize export
            Different operations depending on if the export is newly created or being updated
        :param export: CustomSegmentFileUpload
        :param segment: CustomSegment
        :param owner: UserProfile
        :param s3_key: str
        :return:
        """
        now = timezone.now()
        download_url = self.generate_temporary_url(s3_key, time_limit=3600 * 24 * 7)
        if self.updating:
            export.updated_at = now
        else:
            export.completed_at = timezone.now()
            self._send_notification_email(owner.email, segment.title, download_url)
        export.download_url = download_url
        export.save()

        statistics = calculate_statistics(
            segment.related_aw_statistics_model,
            segment.segment_type,
            segment.get_es_manager(),
            segment.get_segment_items_query()
        )
        segment.statistics = statistics
        segment.save()
        logger.error("Complete: {}".format(segment.title))

    def _send_notification_email(self, email, segment_title, download_url):
        subject = "Custom Target List: {}".format(segment_title)
        text_header = "Your Custom Target List {} is ready".format(segment_title)
        text_content = "<a href={download_url}>Click here to download</a>".format(download_url=download_url)
        html_email = generate_html_email(text_header, text_content)

        self.ses.send_email(email, subject, html_email)

    @staticmethod
    def has_next():
        """
        Check if queue has at least one item
        :return:
        """
        has_next = CustomSegmentFileUpload.objects.filter(completed_at=None).first()
        if has_next:
            return True
        return False

    def es_generator(self, es_manager, query, sort, limit, serializer):
        """
        Yield from es_manager.scan method to surpass 10k query limit
        :param es_manager:
        :param query: dict -> Query JSON
        :param sort: dict -> {"stats.subscribers": {"order": "asc}}
        :param limit: int
        :param serializer: Export serializer -> CustomSegmentVideoExportSerializer
        :return:
        """
        count = 0
        search = es_manager._search()
        search = search.query(query)
        search = search.sort(sort)
        search = search.params(preserve_order=True)
        for doc in search.scan():
            self.segment_item_ids.append(doc.main.id)
            data = serializer(doc).data
            yield data

            count += 1
            if count >= limit:
                break

    def delete_export(self, owner_id, segment_title):
        """
        Delete csv from s3
        :param owner_id:
        :param segment_title:
        :return:
        """
        s3_key = self.get_s3_key(owner_id, segment_title)
        self.delete_obj(s3_key)
