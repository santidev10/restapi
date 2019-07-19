import logging

from django.conf import settings
from django.utils import timezone

from administration.notifications import generate_html_email
from brand_safety.constants import CHANNEL
from segment.models import CustomSegmentFileUpload
from segment.models.custom_segment_file_upload import CustomSegmentFileUploadQueueEmptyException
from utils.elasticsearch import ElasticSearchConnector
from utils.elasticsearch import ElasticSearchConnectorException
from utils.aws.export_context_manager import ExportContextManager
from utils.aws.s3_exporter import S3Exporter
from utils.aws.ses_emailer import SESEmailer

logger = logging.getLogger(__name__)


class CustomSegmentExportGenerator(S3Exporter):
    bucket_name = settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME

    def __init__(self, updating=False):
        self.es_conn = ElasticSearchConnector()
        self.ses = SESEmailer()
        self.updating = updating
        # List of methods that should be invoked with export data in self.es_generator method before actual export
        self.generator_operations = [self._update_fields, self._add_related_ids_to_segment]

    def generate(self, export=None):
        """
        Either dequeue segment to create and process or updating existing export
        :return:
        """
        if export is None:
            try:
                export = CustomSegmentFileUpload.dequeue()
            except CustomSegmentFileUploadQueueEmptyException:
                raise
        segment = export.segment
        owner = segment.owner
        try:
            # Update segment and empty related_ids to recreate relevant related_ids
            segment.related.all().delete()
            es_generator = self.es_generator(export, segment)
        except ElasticSearchConnectorException:
            raise
        log_message = "Updating" if self.updating else "Generating"
        logger.error("{} export: {}".format(log_message, segment.title))
        export_manager = ExportContextManager(es_generator, export.columns)
        s3_key = self.get_s3_key(owner.id, segment.title)
        self.export_to_s3(export_manager, s3_key, get_key=False)
        self._finalize_export(export, segment, owner, s3_key)

    @staticmethod
    def get_s3_key(owner_id, segment_title):
        return "{owner_id}/{segment_title}.csv".format(owner_id=owner_id, segment_title=segment_title)

    def _finalize_export(self, export, segment, owner, s3_key):
        """
        Finalize export
            Different operations depending on if the export is newly created or being upated
        :param export:
        :param segment:
        :param owner:
        :param s3_key:
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
        export.segment.update_statistics()
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

    def es_generator(self, export, segment):
        """
        Hook to execute operations before providing data to actual export operation
        :param export: CustomSegmentFileUpload
        :param segment: CustomSegment
        :return:
        """
        scroll = self.es_conn.scroll(
            export.query,
            export.index,
            size=export.batch_size,
            batches=export.batch_limit,
            sort_field=export.sort
        )
        for batch in scroll:
            for method in self.generator_operations:
                method(export, batch, sequence=True)
            segment.add_related_ids([item["_id"] for item in batch])
            # Yield pertinent es data
            batch = [item["_source"] for item in batch]
            yield batch

    def _update_fields(self, export, chunk, sequence=True):
        """
        Add or update Elasticsearch document fields for csv export
        :param export: export obj
        :param chunk: item(s)
        :param sequence: Bool to indicate whether chunk is a list or tuple
        :return: None
        """
        url_prefix = "https://www.youtube.com/channel/" if export.segment.segment_type == CHANNEL else "https://www.youtube.com/video/"
        if sequence:
            for item in chunk:
                item["_source"].update({
                    "youtube_category": item["_source"]["youtube_category"].capitalize(),
                    "url": url_prefix + item["_id"]
                })

    def _add_related_ids_to_segment(self, export, chunk, sequence=True):
        """
        Add related ids to custom segment
        :param export: export obj
        :param chunk: item(s)
        :param sequence: Bool to indicate whether chunk is a list or tuple
        :return:
        """
        segment = export.segment
        if not sequence:
            chunk = tuple(chunk["_id"])
        else:
            chunk = [item["_id"] for item in chunk]
        segment.add_related_ids(chunk)

    def delete_export(self, owner_id, segment_title):
        s3_key = self.get_s3_key(owner_id, segment_title)
        self.delete_obj(s3_key)
