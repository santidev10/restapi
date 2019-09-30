import logging
import time

import boto3
from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import BooleanField
from django.db.models import CharField
from django.db.models import Manager
from django.db.models import TextField
from django.db.models import DateTimeField
from django.db.models import Model
from django.db.models import IntegerField
from django.db.models import UUIDField

from audit_tool.models import AuditCategory
from utils.models import Timestampable
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants.constants import S3_SEGMENT_EXPORT_KEY_PATTERN
from segment.models.persistent.constants.constants import S3_SEGMENT_BRAND_SAFETY_EXPORT_KEY_PATTERN
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
from es_components.constants import SEGMENTS_UUID_FIELD
from segment.models.utils.calculate_segment_statistics import calculate_statistics
from segment.models.utils.export_context_manager import ExportContextManager
from utils.aws.s3_exporter import S3Exporter
from utils.aws.ses_emailer import SESEmailer


class SegmentMixin(object):
    REMOVE_FROM_SEGMENT_RETRY = 15
    RETRY_SLEEP_COEFF = 1
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.SEGMENTS)

    def get_segment_items_query(self):
        query = QueryBuilder().build().must().term().field(SEGMENTS_UUID_FIELD).value(self.uuid).get()
        return query

    def get_queryset(self, sections=None, query=None, sort=None):
        scan = self.generate_search_with_params(sections=sections, query=query, sort=sort).scan()
        return scan

    def _s3(self):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.AMAZON_S3_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AMAZON_S3_SECRET_ACCESS_KEY
        )
        return s3

    def export_to_s3(self, s3_key):
        with ExportContextManager(segment=self) as exported_file_name:
            self._s3().upload_file(
                Bucket=settings.AMAZON_S3_BUCKET_NAME,
                Key=s3_key,
                Filename=exported_file_name,
            )

    def calculate_statistics(self):
        """
        Aggregate statistics
        :param items_count: int
        :return:
        """
        statistics = calculate_statistics(
            self.related_aw_statistics_model, self.segment_type, self.get_es_manager(), self.get_segment_items_query()
        )
        return statistics

    @staticmethod
    def extract_aggregations(aggregation_result_dict):
        """
        Extract value fields of aggregation results
        :param aggregation_result_dict: { "agg_name" : { value: "a_value" } }
        :return:
        """
        results = {}
        for key, value in aggregation_result_dict.items():
            results[key] = value["value"]
        return results

    def remove_all_from_segment(self):
        """
        Remove all references to segment uuid from Elasticsearch
        :return:
        """
        es_manager = self.get_es_manager()
        query = self.get_segment_items_query()
        self.retry_on_conflict(es_manager.remove_from_segment, query, self.uuid, retry_amount=self.REMOVE_FROM_SEGMENT_RETRY, sleep_coeff=self.RETRY_SLEEP_COEFF)

    def generate_search_with_params(self, sections=None, query=None, sort=None):
        """
        Generate scan query with sorting
        :param manager:
        :param query:
        :param sort:
        :return:
        """
        manager = self.get_es_manager(sections=sections)
        if query is None:
            query = self.get_segment_items_query()
        search = manager._search()
        search = search.query(query)
        if sort:
            search = search.sort(sort)
        search = search.params(preserve_order=True)
        return search

    def retry_on_conflict(self, method, *args, retry_amount=10, sleep_coeff=2, **kwargs):
        """
        Retry on Document Conflicts
        """
        tries_count = 0
        try:
            while tries_count <= retry_amount:
                try:
                    result = method(*args, **kwargs)
                except Exception as err:
                    if "ConflictError(409" in str(err):
                        tries_count += 1
                        if tries_count <= retry_amount:
                            sleep_seconds_count = retry_amount ** sleep_coeff
                            time.sleep(sleep_seconds_count)
                    else:
                        raise err
                else:
                    return result
        except Exception:
            raise
