from abc import abstractmethod
import hashlib
import json
import logging
import re
from abc import abstractmethod
from typing import Union
from urllib.parse import unquote

from django.contrib.auth import get_user_model
from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from elasticsearch_dsl import Q
from elasticsearch_dsl.response import Response
from redis.exceptions import ConnectionError
from rest_framework.filters import BaseFilterBackend
from rest_framework.serializers import Serializer

import brand_safety.constants as brand_safety_constants
from cache.constants import RESEARCH_CHANNELS_DEFAULT_CACHE_KEY
from cache.constants import RESEARCH_VIDEOS_DEFAULT_CACHE_KEY
from channel.constants import RESEARCH_CHANNELS_DEFAULT_SORT
from video.constants import RESEARCH_VIDEOS_DEFAULT_SORT
from es_components.constants import Sections
from es_components.models import Video
from es_components.iab_categories import IAB_TIER1_CATEGORIES
from es_components.query_builder import QueryBuilder
from es_components.query_repository import get_ias_verified_exists_filter
from es_components.query_repository import get_last_vetted_at_exists_filter
from userprofile.constants import StaticPermissions
from utils.api.filters import FreeFieldOrderingFilter
from utils.api_paginator import CustomPageNumberPaginator
from utils.es_components_cache import CACHE_KEY_PREFIX
from utils.es_components_cache import cached_method
from utils.es_components_cache import redis
from utils.percentiles import get_percentiles
from utils.utils import prune_iab_categories
from utils.utils import slice_generator
import video.constants as video_constants

import pickle

DEFAULT_PAGE_SIZE = 50
UI_STATS_HISTORY_FIELD_LIMIT = 30

logger = logging.getLogger(__name__)


class BrandSafetyParamAdapter:
    scores = {
        brand_safety_constants.HIGH_RISK: "0,69",
        brand_safety_constants.RISKY: "70,79",
        brand_safety_constants.LOW_RISK: "80,89",
        brand_safety_constants.SAFE: "90,100"

    }
    parameter = "brand_safety"
    parameter_full_name = "brand_safety.overall_score"

    def adapt(self, query_params):
        parameter = query_params.get(self.parameter)
        if parameter:
            brand_safety_overall_score = []
            labels = query_params[self.parameter].title().split(",")
            for label in labels:
                score = self.scores.get(label)
                if score:
                    brand_safety_overall_score.append(score)
            query_params[self.parameter_full_name] = brand_safety_overall_score
        return query_params


class SentimentParamAdapter:
    sentiment_ranges = {
        video_constants.WELL_LIKED: "90,100",
        video_constants.AVERAGE: "70,100",
        video_constants.ALL: "0,100"
    }
    parameter_name = "stats.sentiment"

    def adapt(self, query_params):
        parameter = query_params.get(self.parameter_name)
        if parameter:
            label = parameter.strip()
            sentiment_range = self.sentiment_ranges.get(label)
            if sentiment_range:
                query_params[self.parameter_name] = sentiment_range
        return query_params


class FlagsParamAdapter:
    parameter_name = "flags"
    parameter_full_name = "stats.flags"

    def adapt(self, query_params):
        parameter = query_params.get(self.parameter_name)
        if parameter:
            flags = parameter.lower().replace(" ", "_")
            query_params[self.parameter_full_name] = flags
            query_params.pop(self.parameter_name)
        return query_params


def get_limits(query_params, default_page_size=None, max_page_number=None):
    size = int(query_params.get("size", default_page_size or DEFAULT_PAGE_SIZE))
    page = int(query_params.get("page", 1))
    page = min(page, max_page_number) if max_page_number else page
    offset = 0 if page <= 1 else (page - 1) * size

    return size, offset, page


def get_sort_rule(query_params):
    sort_params = query_params.get("sort", None)

    if sort_params:
        key, direction = sort_params.split(":")
        return [{key: {"order": direction}}]
    return None


def get_fields(query_params, allowed_sections_to_load):
    fields = query_params.get("fields", [])

    if fields:
        fields = fields.split(",")

    fields = [
        field
        for field in fields
        if field.split(".")[0] in allowed_sections_to_load
    ]

    return fields


class QueryGenerator:
    es_manager = None
    terms_filter = ()
    range_filter = ()
    match_phrase_filter = ()
    exists_filter = ()
    params_adapters = ()
    must_not_terms_filter = ()

    def __init__(self, query_params, context: dict = None):
        self.query_params = self._adapt_query_params(query_params)
        self.context = context or dict()

    def add_should_filters(self, ranges, filters, field):
        if ranges is None:
            return
        queries = []
        for query_range in ranges:
            if query_range:
                range_min, range_max = query_range.split(",")

                if not (range_min or range_max):
                    continue

                query = QueryBuilder().build().should().range().field(field)
                if range_min:
                    try:
                        range_min = float(range_min)
                    except ValueError:
                        # in case of filtering by date
                        pass
                    query = query.gte(range_min)
                if range_max:
                    try:
                        range_max = float(range_max)
                    except ValueError:
                        # in case of filtering by date
                        pass
                    query = query.lte(range_max)
                queries.append(query)
        combined_query = None
        for query in queries:
            query_obj = query.get()
            if not combined_query:
                combined_query = query_obj
            else:
                combined_query |= query_obj
        filters.append(combined_query)

    # pylint: disable=too-many-nested-blocks
    def __get_filter_range(self):
        filters = []
        for field in self.range_filter:
            if field == "brand_safety.overall_score":
                self.add_should_filters(self.query_params.get(field, None), filters, field)
            else:
                query_range = self.query_params.get(field, None)
                if query_range:
                    range_min, range_max = query_range.split(",")

                    if not (range_min or range_max):
                        continue

                    query = QueryBuilder().build().must().range().field(field)
                    if range_min:
                        try:
                            range_min = float(range_min)
                        except ValueError:
                            # in case of filtering by date
                            pass
                        query = query.gte(range_min)
                    if range_max:
                        try:
                            range_max = float(range_max)
                        except ValueError:
                            # in case of filtering by date
                            pass
                        query = query.lte(range_max)
                    filters.append(query.get())

        return filters
    # pylint: enable=too-many-nested-blocks

    def __get_filters_term(self):
        filters = []

        for field in self.terms_filter:

            value = self.query_params.get(field, None)

            if value:
                value = value.split(",") if isinstance(value, str) else value
                # Add +10 relevancy score boost to result item if primary category matches selected iab category filter
                if field == "general_data.iab_categories":
                    iab_categories = [{"terms": {field: [val for val in value]}}]
                    primary_category = [{
                        "terms": {
                            "general_data.primary_category": [val for val in value if val in IAB_TIER1_CATEGORIES],
                            "boost": 10
                        }
                    }]
                    query = Q({
                                "bool": {
                                    "must": iab_categories,
                                    "should": primary_category
                                }
                            })
                    filters.append(query)
                elif field == "auth_channel":
                    self.adapt_auth_channel_filters(filters)
                else:
                    filters.append(
                        QueryBuilder().build().must().terms().field(field).value(value).get()
                    )
        return filters

    def __get_filters_must_not_terms(self):
        filters = []
        for field in self.must_not_terms_filter:
            value = self.query_params.get(field, None)
            if value:
                value = value.split(",") if isinstance(value, str) else value
                filters.append(
                    QueryBuilder().build().must_not().terms().field(field).value(value).get()
                )
        return filters

    def __get_filters_match_phrase(self):
        """
        Applies a multi-match to ALL match_phrase_filter fields if at least one
        match_phrase_filter field is present with a value in the query string
        """
        filters = []
        fields = []
        search_phrase = None
        for field in self.match_phrase_filter:
            value = self.query_params.get(field, None)
            if value and isinstance(value, str):
                search_phrase = value
            fields.append(field)

        should_array = []
        for field in fields:
            boost_value = 1
            if field == "general_data.title":
                boost_value = 3

            should_array_item_match_phrase_prefix = Q("match_phrase_prefix",
                                                      **{
                                                          field: {"query": search_phrase, "boost": boost_value}
                                                      })
            should_array.append(should_array_item_match_phrase_prefix)

            # add 1 to the boost_value for match field
            boost_value = boost_value + 1
            should_array_item_match_phrase = Q("match_phrase",
                                   **{
                                       field: {"query": search_phrase, "boost": boost_value}
                                   })
            should_array.append(should_array_item_match_phrase)

        query = Q("bool", should=should_array)
        if search_phrase:
            filters.append(query)
        return filters

    @staticmethod
    def adapt_transcript_filters(filters: list, value: Union[str, bool]) -> None:
        """
        adapt the transcripts=true query param into an exists/!exists filter
        :param filters:
        :param value:
        :return:
        """
        # handle the case of an exists filter
        if value is True or (isinstance(value, str) and value.lower() == "true"):
            # query = Q("bool")
            # query |= QueryBuilder().build().must().exists().field(f"{Sections.CUSTOM_CAPTIONS}.items").get()
            # query |= QueryBuilder().build().must().exists().field(f"{Sections.CAPTIONS}").get()
            # new transcripts index query
            query = QueryBuilder().build().must().term().field(f"{Sections.CUSTOM_CAPTIONS}.has_transcripts")\
                .value(True).get()
            filters.append(query)
        # handle not exists filter
        elif value is False or (isinstance(value, str) and value.lower() == "false"):
            # query = Q("bool")
            # query &= QueryBuilder().build().must_not().exists().field(f"{Sections.CUSTOM_CAPTIONS}.items").get()
            # query &= QueryBuilder().build().must_not().exists().field(f"{Sections.CAPTIONS}").get()
            # new transcripts index query
            has_transcripts_query = Q("bool")
            has_transcripts_query |= QueryBuilder().build().must_not().exists()\
                .field(f"{Sections.CUSTOM_CAPTIONS}.has_transcripts").get()
            has_transcripts_query |= QueryBuilder().build().must().term()\
                .field(f"{Sections.CUSTOM_CAPTIONS}.has_transcripts").value(False).get()
            # query &= has_transcripts_query
            filters.append(has_transcripts_query)

    def adapt_ias_filters(self, filters, value):
        """
        ias filter requires a string timestamp from IASHistory.get_last_ingested_timestamp. this is passed in the
        context dictionary on instantiation, by the ESFilterBackend
        :param filters:
        :param value:
        :return:
        """
        if value is True or (isinstance(value, str) and value.lower() == "true"):
            timestamp = self.context.get("ias_last_ingested_timestamp")
            query = get_ias_verified_exists_filter(timestamp)
            filters.append(query)
        return

    def adapt_auth_channel_filters(self, filters):
        """ adds filter for channels with matching AuthChannel object and active token """
        auth_channel_ids = self.context.get("auth_channel_ids")
        query = QueryBuilder().build().must().terms().field("main.id").value(auth_channel_ids).get()
        filters.append(query)
        return

    @staticmethod
    def adapt_last_vetted_at_exists_filter(filters, value):
        """
        modify task_us_data.last_vetted_at:exists to check if the date is greater than LAST_VETTED_AT_MIN_DATE
        :param filters:
        :param value:
        :return: filters
        """
        if value is True or (isinstance(value, str) and value.lower() == "true"):
            query = get_last_vetted_at_exists_filter()
            filters.append(query)
        return

    def __get_filters_exists(self):
        filters = []

        for field in self.exists_filter:
            value = self.query_params.get(field, None)

            if value is None:
                continue

            if field == "transcripts":
                self.adapt_transcript_filters(filters, value)
                continue
            elif field == "ias_data.ias_verified":
                self.adapt_ias_filters(filters, value)
                continue
            elif field == "task_us_data.last_vetted_at":
                self.adapt_last_vetted_at_exists_filter(filters, value)
                continue

            query = QueryBuilder().build()

            if value is True or value.lower() == "true":
                query = query.must()
            elif value is False or value.lower() == "false":
                query = query.must_not()
            else:
                continue
            filters.append(query.exists().field(field).get())

        return filters

    def __get_filters_by_ids(self):
        """
        DEPRECATED
        Use __get_filters_term with "main.id" term
        """
        ids_str = self.query_params.get("ids", None)
        filters = []
        if ids_str:
            ids = ids_str.split(",")
            filters.append(self.es_manager.ids_query(ids))
        return filters

    def _adapt_query_params(self, query_params):
        for adapter in self.params_adapters:
            query_params = adapter().adapt(query_params)
        return query_params

    def get_search_filters(self):
        filters_term = self.__get_filters_term()
        filters_must_not_terms = self.__get_filters_must_not_terms()
        filters_range = self.__get_filter_range()
        filters_match_phrase = self.__get_filters_match_phrase()
        filters_exists = self.__get_filters_exists()
        forced_filter = [self.es_manager.forced_filters(include_deleted=True)] if filters_match_phrase \
            else [self.es_manager.forced_filters()]
        ids_filter = self.__get_filters_by_ids()

        filters = filters_term + filters_range + filters_match_phrase + \
                  filters_exists + forced_filter + ids_filter + filters_must_not_terms

        return filters


# pylint: disable=abstract-method
class ESDictSerializer(Serializer):
    def to_representation(self, instance):
        extra_data = super(ESDictSerializer, self).to_representation(instance)

        chart_data = extra_data.get("chart_data")
        if chart_data and isinstance(chart_data, list):
            chart_data[:] = chart_data[-UI_STATS_HISTORY_FIELD_LIMIT:]
        data = instance.to_dict()
        data = self._add_blocklist(data)
        data = self._check_ias_verified(data)
        # add mapped data directly from a field's value
        for section_name, source_name, mapping, dest_name in [
            # channel
            (Sections.GENERAL_DATA, "country_code", self.context.get("countries_map"), "country"),
            (Sections.GENERAL_DATA, "top_lang_code", self.context.get("languages_map"), "top_language"),
            # video
            (Sections.GENERAL_DATA, "lang_code", self.context.get("languages_map"), "language"),
        ]:
            data = self._add_context_mapped_field(data, section_name, source_name, mapping, dest_name)
        stats = data.get("stats", {})
        for name, value in stats.items():
            if name.endswith("_history") and isinstance(value, list):
                value[:] = value[:UI_STATS_HISTORY_FIELD_LIMIT]
        return {
            **data,
            **extra_data,
        }

    @staticmethod
    def _add_context_mapped_field(data: dict, section_name: str, source_name: str, mapping: dict,
                                  dest_name: str) -> dict:
        """
        add a mapped field in a given section_name, from a field's value already extant in within the section
        :param data:
        :param section_name:
        :param source_name:
        :param mapping:
        :param dest_name:
        :return: dict
        """
        if not mapping:
            return data

        section = data.get(section_name)
        if not section:
            return data

        key = section.get(source_name)
        if not key:
            return data

        dest_data = mapping.get(key)
        if not dest_data:
            return data

        data[section_name][dest_name] = dest_data
        return data

    def _check_ias_verified(self, data: dict):
        """
        Only provide IAS data if channel was included in the last IAS data ingestion
        """
        try:
            if data["ias_data"]["ias_verified"] < self.context["latest_ias_ingestion"]:
                data.pop("ias_data", None)
        except (KeyError, TypeError):
            pass
        return data

    def _add_blocklist(self, data: dict):
        """
        Add blocklist data to video if video does not have blocklist data
            Video is implicitly blocklisted if channel is blocklisted
        :param data:
        :return:
        """
        if isinstance(self.context.get("user"), get_user_model()) \
                and self.context["user"].has_permission(StaticPermissions.RESEARCH__BRAND_SUITABILITY_HIGH_RISK):
            if data.get("custom_properties", {}).get("blocklist") is None:
                try:
                    channel_id = data["channel"]["id"]
                    channel_blocklisted = self.context["channel_blocklist"][channel_id]
                except KeyError:
                    channel_blocklisted = False
                custom_properties = data.get("custom_properties", {})
                custom_properties.update({"blocklist": channel_blocklisted})
                data["custom_properties"] = custom_properties
        else:
            custom_properties = data.get("custom_properties", {})
            try:
                del custom_properties["blocklist"]
            except KeyError:
                pass
        return data
# pylint: enable=abstract-method


class VettedStatusSerializerMixin:

    UNVETTED = "Unvetted"
    VETTED_SAFE = "Vetted Safe"
    VETTED_RISKY = "Vetted Risky"

    def get_vetted_status(self, instance):
        """
        Infers whether or not a Channel/Video has been vetted, and whether or
        not it was vetted safe or risky based on the presence of the
        `task_us_data.brand_safety` and `task_us_data.last_vetted_at` field,
        and whether or not it is empty
        """
        instance_dict = instance.to_dict()
        task_us_data = instance_dict.get(Sections.TASK_US_DATA, None)
        if task_us_data is None \
                or task_us_data.get('last_vetted_at', None) is None:
            return self.UNVETTED
        brand_safety = task_us_data.get('brand_safety', None)
        if brand_safety is None \
                or not len([cat_id for cat_id in brand_safety if cat_id]):
            return self.VETTED_SAFE
        return self.VETTED_RISKY


class BlackListSerializerMixin:

    def __init__(self, instance, *args, **kwargs):
        super().__init__(instance, *args, **kwargs)
        self.blacklist_data = {}
        if instance:
            channels = instance if isinstance(instance, list) else [instance]
            self.blacklist_data = self.fetch_blacklist_items(channels)

    def fetch_blacklist_items(self, channels):
        from audit_tool.models import BlacklistItem

        doc_ids = [doc.meta.id for doc in channels]
        blacklist_items = BlacklistItem.get(doc_ids, BlacklistItem.CHANNEL_ITEM)
        blacklist_items_by_id = {
            item.item_id: {
                "blacklist_data": item.to_dict()
            } for item in blacklist_items
        }
        return blacklist_items_by_id



class ESQuerysetAdapter:
    def __init__(self, manager, *_, cached_aggregations=None, from_cache=None, query_params=None, **__):
        self.manager = manager
        self.sort = None
        self.filter_query = None
        self.slice = None
        self.aggregations = None
        self.percentiles = None
        self.fields_to_load = None
        self.search_limit = None
        self.cached_aggregations = cached_aggregations
        # Additional control if cached methods should use cache
        self.from_cache = from_cache
        self.query_params = query_params

    @cached_method(timeout=7200)
    def count(self):
        count = self.manager.search(filters=self.filter_query).count()
        return count

    def uncached_count(self):
        count = self.manager.search(filters=self.filter_query).count()
        return count

    def order_by(self, *sorting):
        key, direction = sorting[0].split(":")
        self.sort = [
            {key: {"order": direction}},
            {"main.id": {"order": "asc"}}
        ]
        return self

    def filter(self, query):
        self.filter_query = query
        return self

    def fields(self, fields):
        fields = [
            field
            for field in fields
            if field.split(".")[0] in self.manager.sections
        ]

        self.fields_to_load = fields or self.manager.sections
        return self

    def with_limit(self, search_limit):
        self.search_limit = search_limit
        return self

    @cached_method(timeout=900)
    def get_data(self, start=0, end=None):
        data = self.manager.search(
            filters=self.filter_query,
            sort=self.sort,
            offset=start,
            limit=end,
        ) \
            .source(includes=self.fields_to_load).execute().hits
        return data

    def uncached_get_data(self, start=0, end=None):
        data = self.manager.search(
            filters=self.filter_query,
            sort=self.sort,
            offset=start,
            limit=end,
        ) \
            .source(includes=self.fields_to_load).execute().hits
        return data

    def get_aggregations(self):
        if self.cached_aggregations and self.aggregations:
            aggregations = {aggregation: self.cached_aggregations[aggregation]
                            for aggregation in self.cached_aggregations
                            if aggregation in self.aggregations}
            return aggregations
        aggregations = self.manager.get_aggregation(
            search=self.manager.search(filters=self.filter_query),
            properties=self.aggregations,
        )
        return aggregations

    def get_percentiles(self):
        clean_names = [name.split(":")[0] for name in self.percentiles]
        percentiles = get_percentiles(self.manager, clean_names, add_suffix=":percentiles")
        return percentiles

    def with_aggregations(self, aggregations):
        self.aggregations = aggregations
        return self

    def with_percentiles(self, percentiles):
        self.percentiles = percentiles
        return self

    def get_cache_key(self, part, options):
        options = dict(
            filters=[_filter.to_dict() for _filter in self.filter_query],
            sort=self.sort,
            options=options,
            sections=self.manager.sections
        )
        key_json = json.dumps(options, sort_keys=True, cls=DjangoJSONEncoder)
        key_hash = hashlib.md5(key_json.encode()).hexdigest()
        key = f"{CACHE_KEY_PREFIX}.{part}.{self.manager.model.__name__}.{key_hash}"
        return key, key_json

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.get_data(item.start, item.stop)
        raise NotImplementedError

    def __iter__(self):
        if self.sort or self.search_limit:
            yield from self.get_data(end=self.search_limit)
        else:
            yield from self.manager.scan(
                filters=self.filter_query,
            )


class ResearchESQuerysetAdapter(ESQuerysetAdapter):

    def get_data(self, start=0, end=None):
        kwargs_config = {
            0: {
                "target_sort": RESEARCH_VIDEOS_DEFAULT_SORT,
                "cache_key": RESEARCH_VIDEOS_DEFAULT_CACHE_KEY
            },
            1: {
                "target_sort": RESEARCH_CHANNELS_DEFAULT_SORT,
                "cache_key": RESEARCH_CHANNELS_DEFAULT_CACHE_KEY
            }
        }
        try:
            kwargs = kwargs_config[0] if self.manager.model is Video else kwargs_config[1]
            data = self._get_from_cache(start, end, **kwargs)
        except ValueError:
            data = super().uncached_get_data(start, end)
        return data

    def _get_from_cache(self, start: int, end: int = DEFAULT_PAGE_SIZE,
                        cache_key: str = RESEARCH_CHANNELS_DEFAULT_CACHE_KEY,
                        target_sort: list = RESEARCH_CHANNELS_DEFAULT_SORT, ) -> Response:
        """
        Get Research data from cache based on request query parameters
        Cached data is cached with celery beat in cache.tasks.cache_research_defaults

        Cached responses will only be used if the first page is being requested, the request sorting matches target_sort,
            and no filters are applied
            e.g. First page of Research Channels is defined as page 1, no filters, and sorting of
                RESEARCH_CHANNELS_DEFAULT_SORT
                If the request matches those conditions, we will retrieve cached data

        :param start: int -> The start index of the response data. If 0, then the first page is being requested.
            This will be multiples of the request page size "end"
            e.g. page size requested = 50
            page 1 start = 0
            page 2 start = 50
            page 3 start = 100
            ...
        :param end: The size of the requested page, which determines the sliced index end
        :param target_sort: The sort value to check if we should use cached data
        :param cache_key: Key to retrieve cached data from redis
        :return: Elasticsearch DSL Response object
        """
        if not self._should_get_cache(target_sort, start):
            raise ValueError
        data = None
        try:
            cached_data = pickle.loads(redis.get(cache_key))
            updated_response = self._get_response_body(cached_data)
            # Build new response body depending on request query params as cached data caches all ES model sections
            # with different page size
            for i in range(len(cached_data.hits)):
                dict_doc = cached_data.hits.hits._l_[i]
                # Copy sections requested by client
                dict_doc["_source"] = {
                    key: val for key, val in dict_doc["_source"].items() if key in self.fields_to_load
                }
                updated_response["hits"]["hits"].append(dict_doc)
                # Copy until the target page size
                if i >= end - 1:
                    break
            # Build new elasticsearch_dsl Response object. By default Research uses forced filters for all queries
            data = Response(self.manager.search(self.manager.forced_filters()), updated_response)
        except (pickle.PickleError, ConnectionError, IndexError):
            pass
        finally:
            if data is None:
                raise ValueError
        return data

    def _get_response_body(self, cached_response: Response):
        """
        Prepare new dict response body to add "hits" data to
        hits must be processed in case client is requesting different ES model sections or page size
        :param cached_response: elasticsearch_dsl Response
        """
        new_response = {
            key: getattr(cached_response, key)
            for key in dir(cached_response) if key != "hits"
        }
        new_response["hits"] = dict(
            max_score=cached_response.hits.max_score,
            total=cached_response.hits.total,
            hits=[],
        )
        return new_response

    def _should_get_cache(self, target_sort: list[dict], start: int) -> bool:
        """
        Determine if cache data should be retrieved
        Client applies request filters by adding key value pairs in request query parameters
            If a section is detected in query params, then filters are being applied. Return False to retrieve
            uncached data
        :param start: Start index of queryset and indicates request page. Refer to _get_from_cache docstring
        :param target_sort: Request sorting. Refer to _get_from_cache docstring
        :return: bool -> Whether or not data should be retrieved from cache
        """
        if start != 0 or self.sort != target_sort:
            return False
        filters_pattern = "|".join((
            f"({s})" for s in self.manager.allowed_sections
        ))
        get_cache = True
        # Search for filters being applied in in request query params
        with_filters = re.search(filters_pattern, "".join(self.query_params.keys()))
        if with_filters:
            get_cache = False
        return get_cache


class ESFilterBackend(BaseFilterBackend):

    @abstractmethod
    def _get_query_generator_context(self) -> dict:
        """
        get a context dictionary to be passed to the query generator on instantiation
        :return:
        """
        return {}

    def _get_query_params(self, request):
        return request.query_params.dict()

    def _get_query_generator(self, request, queryset, view):
        dynamic_generator_class = type(
            "DynamicGenerator",
            (QueryGenerator,),
            dict(
                es_manager=queryset.manager,
                terms_filter=view.terms_filter,
                range_filter=view.range_filter,
                match_phrase_filter=view.match_phrase_filter,
                exists_filter=view.exists_filter,
                params_adapters=view.params_adapters,
            )
        )
        query_params = self._get_query_params(request)
        return dynamic_generator_class(query_params, context=self._get_query_generator_context())

    # pylint: disable=unused-argument
    def _get_aggregations(self, request, queryset, view):
        query_params = self._get_query_params(request)
        aggregations = unquote(query_params.get("aggregations", "")).split(",")
        if "flags" in aggregations:
            aggregations.append("stats.flags")
        if "transcripts" in aggregations:
            aggregations.append("custom_captions.items:exists")
            aggregations.append("captions:exists")
            aggregations.append("transcripts:exists")
            aggregations.remove("transcripts")
        if view.allowed_aggregations is not None:
            aggregations = [agg
                            for agg in aggregations
                            if agg in view.allowed_aggregations]
        return aggregations
    # pylint: enable=unused-argument

    # pylint: disable=unused-argument
    def _get_percentiles(self, request, queryset, view):
        query_params = self._get_query_params(request)
        percentiles = unquote(query_params.get("aggregations", "")).split(",")
        if view.allowed_percentiles is not None:
            percentiles = [agg
                           for agg in percentiles
                           if agg in view.allowed_percentiles]
        return percentiles
    # pylint: enable=unused-argument

    def _get_fields(self, request):
        query_params = self._get_query_params(request)
        fields = query_params.get("fields", "").split(",")
        return fields

    def filter_queryset(self, request, queryset, view):
        if not isinstance(queryset, ESQuerysetAdapter):
            raise BrokenPipeError
        query_generator = self._get_query_generator(request, queryset, view)
        query = query_generator.get_search_filters()
        fields = self._get_fields(request)
        aggregations = self._get_aggregations(request, queryset, view)
        percentiles = self._get_percentiles(request, queryset, view)
        result = queryset.filter(query) \
            .fields(fields) \
            .with_aggregations(aggregations) \
            .with_percentiles(percentiles)
        return result


class ESPOSTFilterBackend(ESFilterBackend):

    def _get_query_generator_context(self) -> dict:
        return super()._get_query_generator_context()

    def _get_query_params(self, request):
        query_params = super()._get_query_params(request)
        if request.method == "POST":
            query_params.update(request.data)
        return query_params


class APIViewMixin:
    serializer_class = ESDictSerializer
    filter_backends = (FreeFieldOrderingFilter, ESFilterBackend)

    allowed_aggregations = ()
    allowed_percentiles = ()

    terms_filter = ()
    must_not_terms_filter = ()
    range_filter = ()
    match_phrase_filter = ()
    exists_filter = ()
    params_adapters = ()

    def get_cached_aggregations_key(self):
        """
        gets cached aggregations key depending on user type:
        if has research brand suitability high risk, return with 'Unsuitable' brand safety agg,
        if not, return without 'Unsuitable' agg
        """
        if self.request.user.has_permission(StaticPermissions.RESEARCH__BRAND_SUITABILITY_HIGH_RISK):
            return self.admin_cached_aggregations_key
        return self.cached_aggregations_key

    def get_cached_aggregations(self):
        """
        gets and sets cached aggregations depending on key provided
        by self.get_cached_aggregations_key
        """
        if hasattr(self, 'cached_aggregations'):
            return self.cached_aggregations

        key = self.get_cached_aggregations_key()
        try:
            cached_aggregations_object = self.cache_class.objects.get(key=key)
            self.cached_aggregations = cached_aggregations_object.value
        # pylint: disable=broad-except
        except Exception as e:
            # pylint: enable=broad-except
            self.cached_aggregations = None

        return self.cached_aggregations

    def get_manager_class(self):
        """
        gets the correct manager class based on user permissions.
        admin class currently adds brand_safety's 'Unsuitable' score range
        """
        if self.request.user.has_permission(StaticPermissions.RESEARCH__BRAND_SUITABILITY_HIGH_RISK):
            return self.admin_manager_class
        return self.manager_class


class PaginatorWithAggregationMixin:
    def _get_response_data(self: CustomPageNumberPaginator, data):
        for item in data:
            try:
                item["general_data"]["iab_categories"] = prune_iab_categories(item["general_data"]["iab_categories"])
            except BaseException:
                continue
        response_data = super(PaginatorWithAggregationMixin, self)._get_response_data(data)
        object_list = self.page.paginator.object_list
        if isinstance(object_list, ESQuerysetAdapter):
            aggregations = object_list.get_aggregations() or {}
            percentiles = object_list.get_percentiles() or {}
            all_aggregations = dict(**aggregations, **percentiles)
            response_data["aggregations"] = all_aggregations or None
        else:
            logger.warning("Can't get aggregation from %s", str(type(object_list)))
        return response_data


class ExportDataGenerator:
    serializer_class = ESDictSerializer
    terms_filter = ()
    must_not_terms_filter = ()
    range_filter = ()
    match_phrase_filter = ()
    exists_filter = ()
    params_adapters = ()
    queryset = None
    export_limit = settings.RESEARCH_EXPORT_LIMIT

    def __init__(self, query_params):
        self.query_params = query_params

    def _get_query_generator(self):
        dynamic_generator_class = type(
            "DynamicGenerator",
            (QueryGenerator,),
            dict(
                es_manager=self.queryset.manager,
                terms_filter=self.terms_filter,
                must_not_terms_filter=self.must_not_terms_filter,
                range_filter=self.range_filter,
                match_phrase_filter=self.match_phrase_filter,
                exists_filter=self.exists_filter,
                params_adapters=self.params_adapters,
            )
        )
        return dynamic_generator_class(self.query_params)

    def __iter__(self):
        self.queryset.filter(
            self._get_query_generator().get_search_filters()
        )
        for item in slice_generator(self.queryset, limit=self.export_limit):
            yield self.serializer_class(item).data
