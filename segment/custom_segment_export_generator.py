import logging

from django.conf import settings

from brand_safety.constants import CHANNEL
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.models.custom_segment_file_upload import CustomSegmentFileUploadQueueEmptyException
from utils.elasticsearch import ElasticSearchConnector
from utils.elasticsearch import ElasticSearchConnectorException
from utils.aws.export_context_manager import ExportContextManager
from utils.aws.s3_exporter import S3Exporter
from utils.aws.ses_emailer import SESEmailer

logger = logging.getLogger(__name__)


class CustomSegmentExportGenerator(S3Exporter):
    bucket_name = settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME

    def __init__(self):
        self.es_conn = ElasticSearchConnector()
        self.ses = SESEmailer()

    def generate(self):
        """
        Dequeue segment to create and process
        :return:
        """
        try:
            export = CustomSegmentFileUpload.dequeue()
        except CustomSegmentFileUploadQueueEmptyException:
            logger.error("No items in queue")
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
        # export.completed_at = timezone.now()
        segment_title = export.segment.title
        s3_key_filename = self.get_s3_key(segment_title)
        download_url = self.generate_temporary_url(s3_key_filename, time_limit=24 * 7)
        export.download_url = download_url
        export.save()

        owner_email = export.owner.email
        self.ses.send_email(owner_email, "Custom Segment Download: {}".format(segment_title), "Download: {}".format(export.download_url))
        logger.error("Done processing: {}".format(segment_title))

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
        Hook to add fields to es documents returned from scroll
        :param export:
        :return:
        """
        if export.segment.segment_type == CHANNEL:
            url_prefix = "https://www.youtube.com/channel/"
            id_key = "channel_id"
            index = settings.BRAND_SAFETY_CHANNEL_INDEX
        else:
            url_prefix = "https://www.youtube.com/video/"
            id_key = "video_id"
            index = settings.BRAND_SAFETY_VIDEO_INDEX
        scroll = self.es_conn.scroll(export.query, index, full=False)
        for batch in scroll:
            for item in batch:
                item.update({
                    "youtube_category": item["youtube_category"].capitalize(),
                    "url": url_prefix + item[id_key]
                })
            yield batch


