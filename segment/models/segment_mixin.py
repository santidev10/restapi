import time

from es_components.constants import SEGMENTS_UUID_FIELD
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
from segment.models.utils.calculate_segment_statistics import calculate_statistics


class SegmentMixin:
    """
    Mixin methods for segment models
    Expected attributes and methods on models used in mixin:
    Attributes:
        uuid
    Methods:
        get_s3_key, get_es_manager,

    """
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.SEGMENTS)

    def get_segment_items_query(self):
        query = QueryBuilder().build().must().term().field(SEGMENTS_UUID_FIELD).value(self.uuid).get()
        return query

    def get_queryset(self, query=None, sort=None):
        scan = self.generate_search_with_params(query=query, sort=sort).scan()
        return scan

    def calculate_statistics(self, items=None):
        """
        Aggregate statistics
        :param items_count: int
        :return:
        """
        statistics = calculate_statistics(self, items=items)
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

    def remove_all_from_segment(self, query=None):
        """
        Remove all references to segment uuid from Elasticsearch
        :return:
        """
        if query is None:
            query = self.get_segment_items_query()
        self.retry_on_conflict(self.es_manager.remove_from_segment, query, self.uuid)

    def add_to_segment(self, doc_ids=None, query=None):
        if doc_ids:
            self.retry_on_conflict(self.es_manager.add_to_segment_by_ids, doc_ids, self.uuid)
        else:
            self.retry_on_conflict(self.es_manager.add_to_segment, query, self.uuid)

    def generate_search_with_params(self, query=None, sort=None, sections=None):
        """
        Generate scan query with sorting
        :return:
        """
        manager = self.es_manager
        if sections:
            manager.sections = sections
        if query is None:
            query = self.get_segment_items_query()
        # pylint: disable=protected-access
        search = manager._search()
        # pylint: enable=protected-access
        search = search.query(query)
        if sort:
            search = search.sort(sort)
            search = search.params(preserve_order=True)
        return search

    def retry_on_conflict(self, method, *args, retry_amount=5, sleep_coeff=2, **kwargs):
        """
        Retry on Document Conflicts
        """
        tries_count = 0
        while tries_count <= retry_amount:
            try:
                result = method(*args, **kwargs)
            # pylint: disable=broad-except
            except Exception as err:
                # pylint: enable=broad-except
                if "ConflictError(409" in str(err):
                    tries_count += 1
                    if tries_count <= retry_amount:
                        sleep_seconds_count = tries_count ** sleep_coeff
                        time.sleep(sleep_seconds_count)
                else:
                    raise err
            else:
                return result
