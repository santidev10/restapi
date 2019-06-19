import logging

from django.conf import settings
from django.utils import timezone

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
        if export is not None:
            es_generator = self.es_generator(export)
        else:
            try:
                export = CustomSegmentFileUpload.dequeue()
            except CustomSegmentFileUploadQueueEmptyException:
                raise
            try:
                es_generator = self.es_generator(export)
            except ElasticSearchConnectorException:
                raise
        export_manager = ExportContextManager(es_generator, export.columns)
        self.export_to_s3(export_manager, export.segment.title)
        self._finalize_export(export)

    @staticmethod
    def get_s3_key(name):
        return "{}.csv".format(name)

    def _finalize_export(self, export):
        now = timezone.now()
        if self.updating:
            export.updated_at = now
        else:
            export.completed_at = timezone.now()
        export.save()
        # segment_title = export.segment.title
        # s3_key_filename = self.get_s3_key(segment_title)
        # download_url = self.generate_temporary_url(s3_key_filename, time_limit=24 * 7)
        # export.download_url = download_url
        # export.save()
        # if not self.updating:
        #     # These methods should be invoked only when the segment is created for the first time
        #     export.segment.update_statistics()
        #     self._send_notification_email(export.owner.email, segment_title, download_url)
        # logger.error("Done processing: {}".format(segment_title))

    def _send_notification_email(self, email, segment_title, download_url):
        self.ses.send_email(email, "Custom Segment Download: {}".format(segment_title), "Download: {}".format(download_url))

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

    def es_generator(self, export):
        """
        Hook to execute operations before providing data to actual export operation
        :param export:
        :return:
        """
        segment = export.segment
        print('exporting export: ', export.id)
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
            print("got", len(batch))
            segment.add_related_ids([item["_id"] for item in batch])
            # Yield pertinent es data
            batch = [item["_source"] for item in batch]
            yield batch
            break

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


