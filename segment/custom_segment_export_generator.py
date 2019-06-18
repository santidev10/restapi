import logging

from django.conf import settings

from brand_safety.constants import CHANNEL
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.models.custom_segment_file_upload import CustomSegmentFileUploadQueueEmpty
from utils.elasticsearch import ElasticSearchConnector
from utils.elasticsearch import ElasticSearchConnectorException
from utils.aws.export_context_manager import ExportContextManager
from utils.aws.s3_exporter import S3Exporter


logger = logging.getLogger(__name__)


class CustomSegmentExportGenerator(S3Exporter):
    def __init__(self):
        self.es_conn = ElasticSearchConnector()

    def generate(self):
        """
        Dequeue segment to create and process
        :return:
        """
        try:
            export = CustomSegmentFileUpload.dequeue()
        except CustomSegmentFileUploadQueueEmpty:
            logger.error("No items in queue")
            raise
        try:
            es_generator = self.es_generator(export)
        except ElasticSearchConnectorException:
            raise

        export_manager = ExportContextManager(es_generator, export.columns)
        self.export_to_s3(export_manager, export.filename)
        self._finalize_export(export)

    @staticmethod
    def get_s3_key(name):
        return "custom_segments/{}.csv".format(name)

    def _finalize_export(self, export):
        # export.completed_at = timezone.now()
        # export.save()
        logger.error("Done processing: {}".format(export.filename))

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
        if export.content_type == CHANNEL:
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
                item.update({"url": url_prefix + item[id_key]})
            yield batch
