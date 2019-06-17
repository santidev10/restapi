from django.utils import timezone

from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.models.custom_segment_file_upload import CustomSegmentFileUploadQueueEmpty
from utils.elasticsearch import ElasticSearchConnector
from utils.elasticsearch import ElasticSearchConnectorException
from utils.aws.export_context_manager import ExportContextManager
from utils.aws.s3_exporter import S3Exporter


class CustomSegemntExportGenerator(S3Exporter):
    def __init__(self):
        self.es_conn = ElasticSearchConnector()

    def generate(self):
        try:
            export = CustomSegmentFileUpload.dequeue()
        except CustomSegmentFileUploadQueueEmpty:
            print("No items in queue")
            raise

        try:
            es_generator = self.es_conn.scroll(export.query, index=export.index, full=False)
        except ElasticSearchConnectorException:
            raise
        export_manager = ExportContextManager(es_generator, export.columns)
        print('exporting')
        self.export_to_s3(export_manager, export.filename)
        export.completed_at = timezone.now()
        export.save()
        # self._finalize_export(export)

    @staticmethod
    def get_s3_key(name):
        return "custom_segments/{}.csv".format(name)

    def _finalize_export(self, export):
        # do some stuff e.g. email person
        export.completed_at = timezone.now()
        export.save()
        print("Done processing: {}".format(export.filename))
