import logging

from django.conf import settings
from django.utils import timezone
from elasticsearch_dsl.search import Search
from elasticsearch_dsl.search import Q

from administration.notifications import generate_html_email
from brand_safety.constants import CHANNEL
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from es_components.constants import Sections
from es_components.constants import SortDirections
from segment.models import CustomSegmentFileUpload
from segment.models.custom_segment_file_upload import CustomSegmentFileUploadQueueEmptyException
from utils.elasticsearch import ElasticSearchConnector
from utils.elasticsearch import ElasticSearchConnectorException
from utils.aws.export_context_manager import ExportContextManager
from utils.aws.s3_exporter import S3Exporter
from utils.aws.ses_emailer import SESEmailer
from utils.es_components_api_utils import ESQuerysetAdapter


logger = logging.getLogger(__name__)


class CustomSegmentExportGenerator(S3Exporter):
    bucket_name = settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME
    VIEWS_SORT = {"views": {"order": SortDirections.DESCENDING}}
    SUBSCRIBERS_SORT = {"views": {"order": SortDirections.DESCENDING}}
    CHANNEL_LIMIT = 20000
    VIDEO_LIMIT = 20000

    def __init__(self, updating=False):
        self.es_conn = ElasticSearchConnector()
        self.ses = SESEmailer()
        self.updating = updating

    def _map_data(self, export, batch):
        mapper = export.mapper
        mapped = []
        for item in batch:
            try:
                mapped.append(mapper(item["_source"]))
            except KeyError:
                continue
        return mapped

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
            limit = self.VIDEO_LIMIT
        else:
            sort_key = self.SUBSCRIBERS_SORT
            limit = self.CHANNEL_LIMIT
        try:
            # Remove all items from this segment
            # And update segment and empty related_ids to recreate relevant related_ids
            segment.remove_all_from_segment()
            query = es_manager(
                query=Q(segment.get_segment_items_query()),
                sort=sort_key,
                limit=limit
            )
            es_queryset = ESQuerysetAdapter(es_manager)
            es_queryset.filter(query)
            es_generator = self.es_generator(es_queryset, serializer)
        except ElasticSearchConnectorException:
            raise
        log_message = "Updating" if self.updating else "Generating"
        logger.error("{} export: {}".format(log_message, segment.title))
        export_manager = ExportContextManager(es_generator, export.columns)
        s3_key = self.get_s3_key(owner.id, segment.title)
        self.export_to_s3(export_manager, s3_key, get_key=False)

        # Add segment UUID to all documents under query
        # segment.es_manager.add_to_segment(export.query_obj, segment.uuid)
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

    def es_generator(self, queryset, serializer):
        for item in queryset:
            data = serializer(item).data
            yield data


    # def es_generator(self, export, segment):
    #     """
    #     Hook to execute operations before providing data to actual export operation
    #     :param export: CustomSegmentFileUpload
    #     :param segment: CustomSegment
    #     :return:
    #     """
    #     query_body = {
    #         "query": export.query,
    #     }
    #     scroll = self.es_conn.scroll(
    #         query_body,
    #         export.index,
    #         size=export.batch_size,
    #         batches=export.batch_limit,
    #         sort_field=export.sort
    #     )
    #     for batch in scroll:
    #         segment.add_to_segment([item["_id"] for item in batch])
    #         mapped = self._map_data(export, batch)
    #         yield mapped

    def delete_export(self, owner_id, segment_title):
        s3_key = self.get_s3_key(owner_id, segment_title)
        self.delete_obj(s3_key)
